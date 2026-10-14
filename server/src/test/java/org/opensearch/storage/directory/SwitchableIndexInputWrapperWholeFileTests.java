/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */
package org.opensearch.storage.directory;

import com.carrotsearch.randomizedtesting.annotations.ThreadLeakFilters;

import org.apache.lucene.store.FSDirectory;
import org.apache.lucene.store.IOContext;
import org.apache.lucene.store.IndexInput;
import org.apache.lucene.store.IndexOutput;
import org.apache.lucene.store.RandomAccessInput;
import org.opensearch.common.lucene.store.InputStreamIndexInput;
import org.opensearch.index.store.remote.file.CleanerDaemonThreadLeakFilter;
import org.opensearch.index.store.remote.filecache.FileCache;
import org.opensearch.index.store.remote.filecache.FileCacheFactory;
import org.opensearch.index.store.remote.utils.TransferManager;
import org.opensearch.storage.indexinput.SwitchableIndexInput;
import org.opensearch.storage.indexinput.SwitchableIndexInputWrapper;
import org.opensearch.storage.prefetch.TieredStoragePrefetchSettings;
import org.opensearch.storage.warmfullfile.FullFileState;
import org.opensearch.storage.warmfullfile.WarmFullFileManager;
import org.junit.Before;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executor;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Supplier;

import static org.opensearch.storage.utils.DirectoryUtils.getFilePath;
import static org.opensearch.storage.utils.DirectoryUtils.getFilePathSwitchable;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 * Step 5 tests: the wrapper's promotion trigger, whole-file view serving, and handle counting.
 */
@ThreadLeakFilters(filters = CleanerDaemonThreadLeakFilter.class)
public class SwitchableIndexInputWrapperWholeFileTests extends TieredStorageBaseTestCase {

    private static final String FILE_NAME = "_0.si";
    private static final Supplier<TieredStoragePrefetchSettings> PREFETCH = () -> {
        TieredStoragePrefetchSettings s = mock(TieredStoragePrefetchSettings.class);
        when(s.getReadAheadBlockCount()).thenReturn(TieredStoragePrefetchSettings.DEFAULT_READ_AHEAD_BLOCK_COUNT);
        when(s.getReadAheadEnableFileFormats()).thenReturn(TieredStoragePrefetchSettings.READ_AHEAD_ENABLE_FILE_FORMATS);
        when(s.isStoredFieldsPrefetchEnabled()).thenReturn(true);
        return s;
    };

    private FSDirectory localDirectory;
    private FileCache fileCache;
    private TransferManager transferManager;

    /** Records promote() submissions without running them, so trigger behaviour is isolated from downloads. */
    private static final class RecordingExecutor implements Executor {
        final AtomicInteger submitted = new AtomicInteger();

        @Override
        public void execute(Runnable command) {
            submitted.incrementAndGet();
        }
    }

    @Before
    public void setup() throws IOException {
        setupRemoteSegmentStoreDirectory();
        populateMetadata();
        remoteSegmentStoreDirectory.init();
        populateData();
        localDirectory = FSDirectory.open(createTempDir());
        fileCache = FileCacheFactory.createConcurrentLRUFileCache(FILE_CACHE_CAPACITY, 1);
        transferManager = new TransferManager(
            (name, position, length) -> new InputStreamIndexInput(
                remoteSegmentStoreDirectory.openBlockInput(name, position, length, IOContext.DEFAULT),
                length
            ),
            fileCache,
            threadPool
        );
    }

    private SwitchableIndexInput remoteModeInput() throws IOException {
        return new SwitchableIndexInput(
            "switchable",
            FILE_NAME,
            getFilePath(localDirectory, FILE_NAME),
            getFilePathSwitchable(localDirectory, FILE_NAME),
            fileCache,
            localDirectory,
            remoteSegmentStoreDirectory,
            transferManager,
            true, // remote (block) mode
            threadPool,
            PREFETCH
        );
    }

    private FullFileState stateFor(SwitchableIndexInput in) {
        return new FullFileState(
            transferManager,
            localDirectory,
            remoteSegmentStoreDirectory,
            fileCache,
            FILE_NAME,
            in.length(),
            getFilePath(localDirectory, FILE_NAME)
        );
    }

    private WarmFullFileManager manager(Executor executor, AtomicLong clock) {
        return new WarmFullFileManager(fileCache, executor, clock::get, 8);
    }

    /** Writes a local whole-file copy whose byte at position p is p mod 251, and opens it as the family master. */
    private IndexInput openMaster(long length) throws IOException {
        String name = "master.bin";
        try (IndexOutput out = localDirectory.createOutput(name, IOContext.DEFAULT)) {
            for (long p = 0; p < length; p++) {
                out.writeByte((byte) (p % 251));
            }
        }
        return localDirectory.openInput(name, IOContext.DEFAULT);
    }

    private static byte expected(long p) {
        return (byte) (p % 251);
    }

    private void makeFull(FullFileState st, IndexInput master) {
        st.setLocalMaster(master);
        assertTrue(st.casState(FullFileState.State.BLOCK, FullFileState.State.PROMOTING));
        assertTrue(st.casState(FullFileState.State.PROMOTING, FullFileState.State.FULL));
    }

    // ---------------- Trigger ----------------

    public void testFirstCloneRequestsPromotionExactlyOnce() throws IOException {
        SwitchableIndexInput in = remoteModeInput();
        FullFileState st = stateFor(in);
        RecordingExecutor exec = new RecordingExecutor();
        SwitchableIndexInputWrapper handle = new SwitchableIndexInputWrapper("h", in, st, true, manager(exec, new AtomicLong()));

        IndexInput c1 = handle.clone();
        assertEquals(FullFileState.State.PROMOTING, st.state());
        assertEquals(1, exec.submitted.get());
        IndexInput c2 = handle.clone();
        assertEquals("no second request while PROMOTING", 1, exec.submitted.get());
        assertTrue("block clones returned while not FULL", c1 instanceof SwitchableIndexInputWrapper);
        assertTrue(c2 instanceof SwitchableIndexInputWrapper);
    }

    public void testFirstSliceRequestsPromotion() throws IOException {
        SwitchableIndexInput in = remoteModeInput();
        FullFileState st = stateFor(in);
        RecordingExecutor exec = new RecordingExecutor();
        SwitchableIndexInputWrapper handle = new SwitchableIndexInputWrapper("h", in, st, true, manager(exec, new AtomicLong()));

        IndexInput s = handle.slice("s", 0, Math.min(16, in.length()));
        assertEquals(1, exec.submitted.get());
        assertTrue(s instanceof SwitchableIndexInputWrapper);
    }

    public void testConcurrentClonesRequestExactlyOnce() throws Exception {
        SwitchableIndexInput in = remoteModeInput();
        FullFileState st = stateFor(in);
        RecordingExecutor exec = new RecordingExecutor();
        SwitchableIndexInputWrapper handle = new SwitchableIndexInputWrapper("h", in, st, true, manager(exec, new AtomicLong()));

        int threads = 8;
        CountDownLatch start = new CountDownLatch(1);
        List<Thread> workers = new ArrayList<>();
        for (int i = 0; i < threads; i++) {
            Thread t = new Thread(() -> {
                try {
                    start.await();
                    handle.clone();
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                }
            });
            workers.add(t);
            t.start();
        }
        start.countDown();
        for (Thread t : workers) {
            t.join();
        }
        assertEquals("exactly one promotion request across threads", 1, exec.submitted.get());
    }

    public void testIneligibleHandleNeverRequests() throws IOException {
        SwitchableIndexInput in = remoteModeInput();
        FullFileState st = stateFor(in);
        RecordingExecutor exec = new RecordingExecutor();
        SwitchableIndexInputWrapper handle = new SwitchableIndexInputWrapper("h", in, st, false, manager(exec, new AtomicLong()));

        handle.clone();
        handle.slice("s", 0, Math.min(16, in.length()));
        assertEquals(0, exec.submitted.get());
        assertEquals(FullFileState.State.BLOCK, st.state());
    }

    public void testNoRequestBeforeRetryAt() throws IOException {
        SwitchableIndexInput in = remoteModeInput();
        FullFileState st = stateFor(in);
        AtomicLong clock = new AtomicLong(1_000);
        st.setRetryAt(5_000);
        RecordingExecutor exec = new RecordingExecutor();
        SwitchableIndexInputWrapper handle = new SwitchableIndexInputWrapper("h", in, st, true, manager(exec, clock));

        handle.clone();
        assertEquals("before retryAt", 0, exec.submitted.get());
        clock.set(5_000);
        handle.clone();
        assertEquals("at retryAt", 1, exec.submitted.get());
    }

    public void testNoRequestWhileFull() throws IOException {
        SwitchableIndexInput in = remoteModeInput();
        FullFileState st = stateFor(in);
        RecordingExecutor exec = new RecordingExecutor();
        SwitchableIndexInputWrapper handle = new SwitchableIndexInputWrapper("h", in, st, true, manager(exec, new AtomicLong()));
        makeFull(st, openMaster(in.length()));

        handle.clone();
        handle.slice("s", 0, Math.min(16, in.length()));
        assertEquals(0, exec.submitted.get());
    }

    public void testSubmitRejectedReturnsToBlock() throws IOException {
        SwitchableIndexInput in = remoteModeInput();
        FullFileState st = stateFor(in);
        Executor rejecting = command -> { throw new java.util.concurrent.RejectedExecutionException("full"); };
        SwitchableIndexInputWrapper handle = new SwitchableIndexInputWrapper("h", in, st, true, manager(rejecting, new AtomicLong()));

        IndexInput c = handle.clone();
        assertEquals(FullFileState.State.BLOCK, st.state());
        assertTrue(c instanceof SwitchableIndexInputWrapper);
    }

    // ---------------- Views ----------------

    public void testCloneWhenFullReturnsViewWithMatchingBytesAndPosition() throws IOException {
        SwitchableIndexInput in = remoteModeInput();
        long len = in.length();
        assumeTrue("need a few bytes to test", len >= 8);
        FullFileState st = stateFor(in);
        SwitchableIndexInputWrapper handle = new SwitchableIndexInputWrapper("h", in, st, true, manager(new RecordingExecutor(), new AtomicLong()));
        makeFull(st, openMaster(len));

        long pos = len / 2;
        handle.seek(pos);
        IndexInput view = handle.clone();
        assertFalse("view, not a block wrapper", view instanceof SwitchableIndexInputWrapper);
        assertEquals("clone keeps the file pointer", pos, view.getFilePointer());
        assertEquals(len, view.length());
        assertEquals(expected(pos), view.readByte());

        view.seek(0);
        byte[] all = new byte[(int) len];
        view.readBytes(all, 0, all.length);
        for (int i = 0; i < len; i++) {
            assertEquals("byte " + i, expected(i), all[i]);
        }
    }

    public void testSliceAndSliceOfSliceWhenFull() throws IOException {
        SwitchableIndexInput in = remoteModeInput();
        long len = in.length();
        assumeTrue("need room for nested slices", len >= 16);
        FullFileState st = stateFor(in);
        SwitchableIndexInputWrapper handle = new SwitchableIndexInputWrapper("h", in, st, true, manager(new RecordingExecutor(), new AtomicLong()));
        makeFull(st, openMaster(len));

        long off = len / 4;
        long sliceLen = len / 2;
        IndexInput slice = handle.slice("s", off, sliceLen);
        assertFalse(slice instanceof SwitchableIndexInputWrapper);
        assertEquals("slice starts at 0", 0, slice.getFilePointer());
        assertEquals(sliceLen, slice.length());
        for (int i = 0; i < sliceLen; i++) {
            assertEquals(expected(off + i), slice.readByte());
        }

        long off2 = sliceLen / 4;
        IndexInput nested = slice.slice("ss", off2, sliceLen / 2);
        assertEquals(expected(off + off2), nested.readByte());

        if (slice instanceof RandomAccessInput rai) {
            assertEquals("positional read", expected(off + 3), rai.readByte(3));
        }
    }

    public void testBlockCloneTakenBeforeFullStaysABlockInput() throws IOException {
        SwitchableIndexInput in = remoteModeInput();
        long len = in.length();
        FullFileState st = stateFor(in);
        SwitchableIndexInputWrapper handle = new SwitchableIndexInputWrapper("h", in, st, false, manager(new RecordingExecutor(), new AtomicLong()));

        IndexInput before = handle.clone();
        makeFull(st, openMaster(len));
        IndexInput after = handle.clone();

        assertTrue("existing inputs are never switched", before instanceof SwitchableIndexInputWrapper);
        assertFalse("new clones get views once FULL", after instanceof SwitchableIndexInputWrapper);
        // The pre-FULL clone still reads (block data in this fixture is all zeros).
        assertEquals(0, before.readByte());
    }

    public void testFeatureOffWrapperBehavesAsBefore() throws IOException {
        SwitchableIndexInput in = remoteModeInput();
        SwitchableIndexInputWrapper handle = new SwitchableIndexInputWrapper("h", in);
        IndexInput c = handle.clone();
        IndexInput s = handle.slice("s", 0, Math.min(16, in.length()));
        assertTrue(c instanceof SwitchableIndexInputWrapper);
        assertTrue(s instanceof SwitchableIndexInputWrapper);
    }

    /**
     * Writes a master of exactly {@code length} bytes with one value of each type at known positions (little-endian, as
     * Lucene's DataOutput writes), then zero padding. Returns the positions in write order.
     */
    private long[] writeTypedMaster(String name, long length) throws IOException {
        long[] pos = new long[8];
        try (IndexOutput out = localDirectory.createOutput(name, IOContext.DEFAULT)) {
            pos[0] = out.getFilePointer();
            out.writeByte((byte) 0x5A);
            pos[1] = out.getFilePointer();
            out.writeShort((short) 0x1234);
            pos[2] = out.getFilePointer();
            out.writeInt(0xCAFEBABE);
            pos[3] = out.getFilePointer();
            out.writeLong(0x0123456789ABCDEFL);
            pos[4] = out.getFilePointer();
            out.writeVInt(300);
            pos[5] = out.getFilePointer();
            out.writeVLong(1L << 40);
            pos[6] = out.getFilePointer();
            for (float f : new float[] { 1.5f, -2.25f, 3.0f, 0.125f }) {
                out.writeInt(Float.floatToIntBits(f));
            }
            pos[7] = out.getFilePointer();
            for (long l : new long[] { 7L, -9L, Long.MAX_VALUE }) {
                out.writeLong(l);
            }
            while (out.getFilePointer() < length) {
                out.writeByte((byte) 0);
            }
        }
        return pos;
    }

    public void testViewsMatchEveryReadMethod() throws IOException {
        SwitchableIndexInput in = remoteModeInput();
        long len = in.length();
        assumeTrue("fixture must hold the typed header", len >= 128);
        FullFileState st = stateFor(in);
        SwitchableIndexInputWrapper handle = new SwitchableIndexInputWrapper("h", in, st, true, manager(new RecordingExecutor(), new AtomicLong()));
        long[] p = writeTypedMaster("typed.bin", len);
        makeFull(st, localDirectory.openInput("typed.bin", IOContext.DEFAULT));

        // Sequential reads on a clone view.
        IndexInput view = handle.clone();
        assertFalse(view instanceof SwitchableIndexInputWrapper);
        view.seek(0);
        assertEquals((byte) 0x5A, view.readByte());
        assertEquals((short) 0x1234, view.readShort());
        assertEquals(0xCAFEBABE, view.readInt());
        assertEquals(0x0123456789ABCDEFL, view.readLong());
        assertEquals(300, view.readVInt());
        assertEquals(1L << 40, view.readVLong());
        float[] floats = new float[4];
        view.readFloats(floats, 0, 4);
        assertArrayEquals(new float[] { 1.5f, -2.25f, 3.0f, 0.125f }, floats, 0f);
        long[] longs = new long[3];
        view.readLongs(longs, 0, 3);
        assertArrayEquals(new long[] { 7L, -9L, Long.MAX_VALUE }, longs);

        // readBytes with an offset into the destination array.
        view.seek(p[2]);
        byte[] dst = new byte[6];
        view.readBytes(dst, 2, 4);
        assertEquals((byte) 0xBE, dst[2]); // little-endian low byte of 0xCAFEBABE
        assertEquals((byte) 0xCA, dst[5]);

        // Positional reads (RandomAccessInput) on the clone view do not move the file pointer.
        RandomAccessInput rai = (RandomAccessInput) view;
        long before = view.getFilePointer();
        assertEquals((byte) 0x5A, rai.readByte(p[0]));
        assertEquals((short) 0x1234, rai.readShort(p[1]));
        assertEquals(0xCAFEBABE, rai.readInt(p[2]));
        assertEquals(0x0123456789ABCDEFL, rai.readLong(p[3]));
        assertEquals(before, view.getFilePointer());

        // A slice view is re-based to 0: the int at p[2] is at 0, the long right after it at 4.
        IndexInput slice = handle.slice("s", p[2], 12);
        assertEquals(0xCAFEBABE, slice.readInt());
        assertEquals(0x0123456789ABCDEFL, ((RandomAccessInput) slice).readLong(4));
    }

    public void testViewsAreRealMmapInputsWithCorrectSegmentBytes() throws Exception {
        SwitchableIndexInput in = remoteModeInput();
        long len = in.length();
        assumeTrue("need a few bytes", len >= 64);
        FullFileState st = stateFor(in);
        SwitchableIndexInputWrapper handle = new SwitchableIndexInputWrapper("h", in, st, true, manager(new RecordingExecutor(), new AtomicLong()));
        // FSDirectory.open on a 64-bit JVM is an MMapDirectory, so the master is a real mmap input.
        makeFull(st, openMaster(len));

        IndexInput view = handle.clone();
        assertTrue(
            "views must be Lucene's mmap input so k-NN's SIMD scorer can engage",
            view instanceof org.apache.lucene.store.MemorySegmentAccessInput
        );
        IndexInput slice = handle.slice("s", 10, 32);
        assertTrue(slice instanceof org.apache.lucene.store.MemorySegmentAccessInput);

        // segmentSliceOrNull returns a segment over the right bytes. Called reflectively so this test does not
        // reference java.lang.foreign types directly (a preview API on JDK 21).
        long segPos = 7;
        long segLen = 20;
        java.lang.reflect.Method sliceOrNull = org.apache.lucene.store.MemorySegmentAccessInput.class.getMethod(
            "segmentSliceOrNull",
            long.class,
            long.class
        );
        Object segment = sliceOrNull.invoke(view, segPos, segLen);
        assertNotNull("single-chunk mapping must expose a segment", segment);
        Class<?> memorySegment = Class.forName("java.lang.foreign.MemorySegment");
        Class<?> ofByte = Class.forName("java.lang.foreign.ValueLayout$OfByte");
        Object javaByte = Class.forName("java.lang.foreign.ValueLayout").getField("JAVA_BYTE").get(null);
        byte[] bytes = (byte[]) memorySegment.getMethod("toArray", ofByte).invoke(segment, javaByte);
        assertEquals(segLen, bytes.length);
        for (int i = 0; i < segLen; i++) {
            assertEquals("segment byte " + i, expected(segPos + i), bytes[i]);
        }

        // On a slice view, positions are relative to the slice start.
        Object sliceSegment = sliceOrNull.invoke(slice, 0L, 4L);
        byte[] sliceBytes = (byte[]) memorySegment.getMethod("toArray", ofByte).invoke(sliceSegment, javaByte);
        for (int i = 0; i < 4; i++) {
            assertEquals(expected(10 + i), sliceBytes[i]);
        }
    }

    public void testSliceBeyondHandleWindowIsRejectedWhenFull() throws IOException {
        SwitchableIndexInput in = remoteModeInput();
        long len = in.length();
        assumeTrue("need room for a sub-window", len >= 64);
        FullFileState st = stateFor(in);
        SwitchableIndexInputWrapper handle = new SwitchableIndexInputWrapper("h", in, st, true, manager(new RecordingExecutor(), new AtomicLong()));
        makeFull(st, openMaster(len));

        // In-bounds slices are views.
        assertTrue(handle.slice("ok", 8, 16) instanceof org.apache.lucene.store.MemorySegmentAccessInput);

        // The handle's window is [0, len). Slicing past its end (or before its start) must fail, as on the block path,
        // even though a larger master could serve those bytes.
        expectThrows(IllegalArgumentException.class, () -> handle.slice("too-far", len - 4, 8));
        expectThrows(IllegalArgumentException.class, () -> handle.slice("negative", -1, 4));
    }

    public void testSliceOfSliceHandleCannotEscapeItsWindowWhenFull() throws IOException {
        SwitchableIndexInput in = remoteModeInput();
        long len = in.length();
        assumeTrue("need room for a sub-window", len >= 64);
        FullFileState st = stateFor(in);
        WarmFullFileManager mgr = manager(new RecordingExecutor(), new AtomicLong());
        SwitchableIndexInputWrapper handle = new SwitchableIndexInputWrapper("h", in, st, false, mgr);

        // Take a block slice BEFORE FULL (a wrapper over a 16-byte sub-window), then go FULL.
        IndexInput window = handle.slice("w", 8, 16);
        assertTrue(window instanceof SwitchableIndexInputWrapper);
        makeFull(st, openMaster(len));

        // Slicing that 16-byte window: in bounds is a view at the right absolute offset; out of bounds is rejected,
        // even though the master holds bytes past the window.
        IndexInput inner = window.slice("inner", 4, 8);
        assertTrue(inner instanceof org.apache.lucene.store.MemorySegmentAccessInput);
        assertEquals(expected(8 + 4), inner.readByte());
        expectThrows(IllegalArgumentException.class, () -> window.slice("escape", 12, 8));
    }

    // ---------------- Whole flow ----------------

    public void testWrapperTriggersRealPromotionAndNextCloneIsView() throws Exception {
        SwitchableIndexInput in = remoteModeInput();
        long len = in.length();
        FullFileState st = stateFor(in);
        // Production constructor with 1 processor: 1 segment (budget = half the cache) and 1 slot. promote() runs inline
        // on the calling thread; the download itself runs on the REMOTE_RECOVERY pool inside fetchBlobAsync.
        WarmFullFileManager realManager = new WarmFullFileManager(fileCache, Runnable::run, threadPool::relativeTimeInMillis, 1);
        SwitchableIndexInputWrapper handle = new SwitchableIndexInputWrapper("h", in, st, true, realManager);

        // First clone: still blocks, and it starts the real promotion.
        IndexInput first = handle.clone();
        assertTrue("first clone is a block input", first instanceof SwitchableIndexInputWrapper);

        assertBusy(() -> assertEquals(FullFileState.State.FULL, st.state()), 30, java.util.concurrent.TimeUnit.SECONDS);
        assertNotNull(st.localMaster());
        assertTrue("whole file downloaded next to the shard files", java.nio.file.Files.exists(getFilePath(localDirectory, FILE_NAME)));
        assertEquals(len, localDirectory.fileLength(FILE_NAME));
        assertNotNull("whole file is in the FileCache", fileCache.get(getFilePath(localDirectory, FILE_NAME)));
        fileCache.decRef(getFilePath(localDirectory, FILE_NAME)); // undo the get() above

        // Next clone: a view of the local mapping, reading the same bytes (all zeros in this fixture).
        IndexInput second = handle.clone();
        assertTrue("next clone is a whole-file view", second instanceof org.apache.lucene.store.MemorySegmentAccessInput);
        assertEquals(len, second.length());
        assertEquals(0, second.readByte());
        // The block clone taken before promotion is never switched. (Reading it here is not asserted: populateData()
        // mocks every remote read with ONE shared IndexInput, which the whole-file download has already consumed, so a
        // later block fetch would read an exhausted stream. Reading pre-FULL block clones next to views is covered by
        // testBlockCloneTakenBeforeFullStaysABlockInput, which does not download.)
        assertTrue(first instanceof SwitchableIndexInputWrapper);
    }

    // ---------------- Handle count ----------------

    public void testOpenedHandleHoldsOneCountReleasedOnceOnClose() throws IOException {
        SwitchableIndexInput in = remoteModeInput();
        FullFileState st = stateFor(in);
        SwitchableIndexInputWrapper handle = new SwitchableIndexInputWrapper("h", in, st, true, manager(new RecordingExecutor(), new AtomicLong()));
        assertEquals(1, st.handleCount());

        handle.clone(); // derived wrappers do not count
        handle.slice("s", 0, Math.min(16, in.length()));
        assertEquals(1, st.handleCount());

        handle.close();
        assertEquals(0, st.handleCount());
        handle.close(); // idempotent
        assertEquals(0, st.handleCount());
    }
}

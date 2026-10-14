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

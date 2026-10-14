/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */
package org.opensearch.storage.warmfullfile;

import org.apache.lucene.store.FSDirectory;
import org.apache.lucene.store.IOContext;
import org.apache.lucene.store.IndexInput;
import org.opensearch.index.store.remote.filecache.CachedIndexInput;
import org.opensearch.index.store.remote.filecache.FileCache;
import org.opensearch.index.store.remote.filecache.FileCacheFactory;
import org.opensearch.index.store.remote.utils.BlobFetchRequest;
import org.opensearch.index.store.remote.utils.TransferManager;
import org.opensearch.test.OpenSearchTestCase;
import org.opensearch.threadpool.TestThreadPool;
import org.opensearch.threadpool.ThreadPool;
import org.junit.After;
import org.junit.Before;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

public class WarmFullFileManagerTests extends OpenSearchTestCase {

    private static final long PART = 8L * 1024 * 1024;

    private ThreadPool threadPool;
    private FSDirectory localDirectory;
    private Path dir;

    @Before
    public void setUpHarness() throws Exception {
        threadPool = new TestThreadPool("WarmFullFileManagerTests");
        dir = createTempDir();
        localDirectory = FSDirectory.open(dir);
    }

    @After
    public void tearDownHarness() throws Exception {
        localDirectory.close();
        threadPool.shutdownNow();
    }

    /**
     * A StreamReader whose bytes are position-dependent, so an offset/length bug produces wrong bytes (unlike the
     * all-zero mocks in populateData / TransferManagerTestCase that the design warns against). The byte at absolute
     * file position p equals p modulo 256. Counts reads so tests can assert each range was read.
     */
    private static final class PositionDependentStreamReader implements TransferManager.StreamReader {
        private final AtomicInteger reads = new AtomicInteger(0);

        @Override
        public InputStream read(String name, long position, long length) {
            reads.incrementAndGet();
            byte[] b = new byte[(int) length];
            for (int i = 0; i < length; i++) {
                b[i] = (byte) ((position + i) & 0xff);
            }
            return new ByteArrayInputStream(b);
        }

        int readCount() {
            return reads.get();
        }
    }

    private FullFileState newState(TransferManager tm, FileCache fileCache, String name, long length) {
        // promote() never touches remoteDirectory, so null is safe here.
        return new FullFileState(tm, localDirectory, null, fileCache, name, length, localDirectory.getDirectory().resolve(name));
    }

    private byte[] expectedBytes(long length) {
        byte[] b = new byte[(int) length];
        for (int i = 0; i < length; i++) {
            b[i] = (byte) (i & 0xff);
        }
        return b;
    }

    private void awaitState(FullFileState st, FullFileState.State target) throws Exception {
        assertBusy(() -> assertEquals(target, st.state()), 10, java.util.concurrent.TimeUnit.SECONDS);
    }

    // ---------- buildParts (pure) ----------

    public void testBuildPartsExactMultiple() {
        List<BlobFetchRequest.BlobPart> parts = WarmFullFileManager.buildParts("_5.faiss", 2 * PART, PART);
        assertEquals(2, parts.size());
        assertEquals(0L, parts.get(0).getPosition());
        assertEquals(PART, parts.get(0).getLength());
        assertEquals(PART, parts.get(1).getPosition());
        assertEquals(PART, parts.get(1).getLength());
    }

    public void testBuildPartsShortLastPart() {
        long length = 2 * PART + 123;
        List<BlobFetchRequest.BlobPart> parts = WarmFullFileManager.buildParts("_5.faiss", length, PART);
        assertEquals(3, parts.size());
        assertEquals(123L, parts.get(2).getLength());
        assertEquals(2 * PART, parts.get(2).getPosition());
        assertEquals(length, parts.stream().mapToLong(BlobFetchRequest.BlobPart::getLength).sum());
    }

    public void testBuildPartsNoZeroLengthTail() {
        List<BlobFetchRequest.BlobPart> parts = WarmFullFileManager.buildParts("_5.faiss", PART, PART);
        assertEquals(1, parts.size());
        assertEquals(PART, parts.get(0).getLength());
    }

    public void testSegmentIndexInRangeAndStable() {
        FileCache fileCache = FileCacheFactory.createConcurrentLRUFileCache(1_000_000, 1);
        WarmFullFileManager mgr = new WarmFullFileManager(fileCache, Runnable::run, new AtomicLong()::get, 1, 0.5, 8, PART);
        Path p = Path.of("/cache/_5.faiss");
        assertEquals(mgr.segmentIndexFor(p), mgr.segmentIndexFor(p));
        int idx = mgr.segmentIndexFor(p);
        assertTrue(idx >= 0 && idx < 8);
    }

    // ---------- promote(): full download path with the real TransferManager + position-dependent reader ----------

    public void testMultiPartDownloadWritesSourceBytesAndReachesFull() throws Exception {
        // Injectable part size (1024) + a 4096-byte file => a real 4-part download, verified byte-for-byte with
        // position-dependent bytes (catches offset bugs), reaching FULL.
        long length = 4096;
        String name = "_5.faiss";
        PositionDependentStreamReader reader = new PositionDependentStreamReader();
        FileCache fileCache = FileCacheFactory.createConcurrentLRUFileCache(10_000_000, 1);
        TransferManager tm = new TransferManager(reader, fileCache, threadPool);
        // run promote on the calling thread; fetchBlobAsync still uses REMOTE_RECOVERY internally
        WarmFullFileManager mgr = new WarmFullFileManager(fileCache, Runnable::run, threadPool::relativeTimeInMillis, 4, 0.5, 1, 1024);

        FullFileState st = newState(tm, fileCache, name, length);
        assertTrue(st.casState(FullFileState.State.BLOCK, FullFileState.State.PROMOTING));
        mgr.promote(st);

        awaitState(st, FullFileState.State.FULL);
        assertNotNull(st.localMaster());

        // The downloaded local file exists with the expected length and bytes.
        assertTrue(Files.exists(dir.resolve(name)));
        assertEquals(length, localDirectory.fileLength(name));
        byte[] expected = expectedBytes(length);
        try (IndexInput in = localDirectory.openInput(name, IOContext.DEFAULT)) {
            byte[] actual = new byte[(int) length];
            in.readBytes(actual, 0, (int) length);
            assertArrayEquals("downloaded bytes must be position-dependent, not zeros", expected, actual);
        }
        // 4096 bytes with a 1024 part size -> 4 range reads, each read once.
        assertEquals("multi-part: 4 parts each read once", 4, reader.readCount());
    }

    public void testSlotDeclineReturnsToBlockWithRetryAt() throws Exception {
        long length = 4096;
        String name = "_7.faiss";
        PositionDependentStreamReader reader = new PositionDependentStreamReader();
        FileCache fileCache = FileCacheFactory.createConcurrentLRUFileCache(10_000_000, 1);
        TransferManager tm = new TransferManager(reader, fileCache, threadPool);
        AtomicLong clock = new AtomicLong(1000);
        // zero free slots: construct with 0 -> clamped to 1, then pre-acquire by starting one promote that holds it?
        // Simpler: budgetFraction 0 so admission always declines on budget (covers the decline path deterministically).
        WarmFullFileManager mgr = new WarmFullFileManager(fileCache, Runnable::run, clock::get, 4, 0.0, 1, 1024);

        FullFileState st = newState(tm, fileCache, name, length);
        assertTrue(st.casState(FullFileState.State.BLOCK, FullFileState.State.PROMOTING));
        mgr.promote(st);

        // budget fraction 0 => cap 0 => reservation fails => decline back to BLOCK with retryAt set
        assertEquals(FullFileState.State.BLOCK, st.state());
        assertEquals(1000 + WarmFullFileManager.RETRY_DELAY_MILLIS, st.retryAt());
        assertFalse("no file downloaded on decline", Files.exists(dir.resolve(name)));
        assertEquals("no remote read on decline", 0, reader.readCount());
    }

    public void testLengthMismatchFailsBackToBlock() throws Exception {
        // Reader returns FEWER bytes than requested -> local file length != expected -> promotion fails back to BLOCK.
        long expectedLength = 8192;
        String name = "_9.faiss";
        TransferManager.StreamReader shortReader = (n, position, len) -> new ByteArrayInputStream(new byte[(int) (len - 1)]);
        FileCache fileCache = FileCacheFactory.createConcurrentLRUFileCache(10_000_000, 1);
        TransferManager tm = new TransferManager(shortReader, fileCache, threadPool);
        AtomicLong clock = new AtomicLong(500);
        WarmFullFileManager mgr = new WarmFullFileManager(fileCache, Runnable::run, clock::get, 4, 0.5, 1, 1024);

        FullFileState st = newState(tm, fileCache, name, expectedLength);
        assertTrue(st.casState(FullFileState.State.BLOCK, FullFileState.State.PROMOTING));
        mgr.promote(st);

        awaitState(st, FullFileState.State.BLOCK);
        assertEquals(500 + WarmFullFileManager.RETRY_DELAY_MILLIS, st.retryAt());
        assertNull("no mapping published on failure", st.localMaster());

        // The truncated file and its entry must be gone, so a retry downloads fresh instead of reusing them.
        Path path = dir.resolve(name);
        assertNull("truncated entry removed", fileCache.get(path));
        assertFalse("truncated file deleted", Files.exists(path));

        // Retry with a correct reader on the same cache: must now reach FULL with the right length.
        PositionDependentStreamReader goodReader = new PositionDependentStreamReader();
        TransferManager goodTm = new TransferManager(goodReader, fileCache, threadPool);
        FullFileState retry = newState(goodTm, fileCache, name, expectedLength);
        assertTrue(retry.casState(FullFileState.State.BLOCK, FullFileState.State.PROMOTING));
        mgr.promote(retry);
        awaitState(retry, FullFileState.State.FULL);
        assertEquals(expectedLength, localDirectory.fileLength(name));
        assertTrue("retry actually downloaded", goodReader.readCount() > 0);
    }
}

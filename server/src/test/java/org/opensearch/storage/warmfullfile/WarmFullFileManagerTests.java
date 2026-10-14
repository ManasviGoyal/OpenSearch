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
     * all-zero mocks in populateData / TransferManagerTestCase, which cannot catch one). The byte at absolute
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

    public void testBudgetDeclineReturnsToBlockWithRetryAt() throws Exception {
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

    public void testFamilyClosedDuringDownloadTakesNoReferenceAndReleasesBytes() throws Exception {
        long length = 4096;
        String name = "_11.faiss";
        java.util.concurrent.CountDownLatch release = new java.util.concurrent.CountDownLatch(1);
        PositionDependentStreamReader inner = new PositionDependentStreamReader();
        TransferManager.StreamReader blocking = (n, position, len) -> {
            try {
                release.await();
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
            return inner.read(n, position, len);
        };
        FileCache fileCache = FileCacheFactory.createConcurrentLRUFileCache(10_000_000, 1);
        TransferManager tm = new TransferManager(blocking, fileCache, threadPool);
        WarmFullFileManager mgr = new WarmFullFileManager(fileCache, Runnable::run, threadPool::relativeTimeInMillis, 4, 0.5, 1, 1024);

        FullFileState st = newState(tm, fileCache, name, length);
        assertTrue(st.casState(FullFileState.State.BLOCK, FullFileState.State.PROMOTING));
        mgr.promote(st); // returns once the download is queued; the reader blocks on the latch
        assertEquals("bytes reserved while downloading", length, mgr.perSegmentWholeFileBytes(0));

        st.closeFamily(); // e.g. the shard closed mid-download
        release.countDown();

        assertBusy(() -> assertEquals("bytes released", 0L, mgr.perSegmentWholeFileBytes(0)), 30, java.util.concurrent.TimeUnit.SECONDS);
        assertEquals(FullFileState.State.CLOSED, st.state());
        assertNull("nothing published", st.localMaster());
        assertFalse(st.cleanupDone());
        assertEquals("no family reference held on the whole-file entry", Integer.valueOf(0), fileCache.getRef(dir.resolve(name)));
    }

    public void testConcurrentFamilyCloseAndPromotionDoNotDeadlock() throws Exception {
        // Race: completions publishing FULL while the families' entries are removed. Keys spread over 4
        // segments, removal runs closeFamily() inside the FileCache listener under a segment lock, and the cleanup
        // re-enters the cache. If that re-entry ever happened on the listener thread, two families whose keys hash to
        // each other's segments could deadlock AB-BA; the timeout below catches that.
        int families = 24;
        long length = 4096;
        FileCache fileCache = FileCacheFactory.createConcurrentLRUFileCache(10_000_000, 4);
        TransferManager tm = new TransferManager(new PositionDependentStreamReader(), fileCache, threadPool);
        WarmFullFileManager mgr = new WarmFullFileManager(
            fileCache,
            threadPool.generic(),
            threadPool::relativeTimeInMillis,
            families,
            0.5,
            4,
            1024
        );
        List<FullFileState> states = new java.util.ArrayList<>();
        List<Path> switchKeys = new java.util.ArrayList<>();
        for (int i = 0; i < families; i++) {
            String name = "_r" + i + ".faiss";
            FullFileState st = newState(tm, fileCache, name, length);
            states.add(st);
            Path switchKey = dir.resolve(name + "_switchable");
            switchKeys.add(switchKey);
            // Stand-in for CachedSwitchableIndexInput: its close() is the family-close routine, called by the listener.
            fileCache.put(switchKey, new CachedIndexInput() {
                private volatile boolean closed;

                @Override
                public IndexInput getIndexInput() {
                    return null;
                }

                @Override
                public long length() {
                    return 0;
                }

                @Override
                public boolean isClosed() {
                    return closed;
                }

                @Override
                public void close() {
                    closed = true;
                    st.closeFamily();
                }
            });
            fileCache.decRef(switchKey);
        }

        java.util.concurrent.CountDownLatch go = new java.util.concurrent.CountDownLatch(1);
        Thread promoter = new Thread(() -> {
            try {
                go.await();
            } catch (InterruptedException e) {
                return;
            }
            for (FullFileState st : states) {
                if (st.casState(FullFileState.State.BLOCK, FullFileState.State.PROMOTING)) {
                    mgr.executor().execute(() -> mgr.promote(st));
                }
            }
        });
        Thread remover = new Thread(() -> {
            try {
                go.await();
            } catch (InterruptedException e) {
                return;
            }
            for (Path k : switchKeys) {
                fileCache.remove(k);
            }
        });
        promoter.start();
        remover.start();
        go.countDown();
        promoter.join(30_000);
        remover.join(30_000);
        assertFalse("promoter stuck (deadlock?)", promoter.isAlive());
        assertFalse("remover stuck (deadlock?)", remover.isAlive());

        // Every family ends CLOSED; any that reached FULL first must finish its cleanup; nothing stays mapped.
        assertBusy(() -> {
            for (FullFileState st : states) {
                assertEquals(FullFileState.State.CLOSED, st.state());
                assertNull("no mapping left behind for " + st.fullFilePath(), st.localMaster());
            }
            for (int s = 0; s < 4; s++) {
                assertEquals("budget fully released in segment " + s, 0L, mgr.perSegmentWholeFileBytes(s));
            }
        }, 30, java.util.concurrent.TimeUnit.SECONDS);
    }

    public void testNioMasterIsDeclinedPermanentlyAndCleanedUp() throws Exception {
        // A local directory that serves the file with NIO (as hybridfs does for extensions on its NIO list).
        long length = 4096;
        String name = "_nio.faiss";
        Path path = dir.resolve(name);
        FileCache fileCache = FileCacheFactory.createConcurrentLRUFileCache(10_000_000, 1);
        PositionDependentStreamReader reader = new PositionDependentStreamReader();
        TransferManager tm = new TransferManager(reader, fileCache, threadPool);
        // One slot: if the decline kept it, the mmap promotion below would be declined.
        WarmFullFileManager mgr = new WarmFullFileManager(fileCache, Runnable::run, threadPool::relativeTimeInMillis, 1, 0.5, 1, 1024);

        try (org.apache.lucene.store.NIOFSDirectory nio = new org.apache.lucene.store.NIOFSDirectory(dir)) {
            FullFileState st = new FullFileState(tm, nio, null, fileCache, name, length, path);
            assertTrue(st.casState(FullFileState.State.BLOCK, FullFileState.State.PROMOTING));
            mgr.promote(st);

            assertBusy(() -> assertEquals(Long.MAX_VALUE, st.retryAt()), 10, java.util.concurrent.TimeUnit.SECONDS);
            assertEquals(FullFileState.State.BLOCK, st.state());
            assertNull("nothing published", st.localMaster());
            assertEquals("downloaded once (4 parts)", 4, reader.readCount());
            assertTrue("the trigger never fires again", mgr.relativeTimeInMillis() < st.retryAt());
            assertNull("useless whole file removed from the cache", fileCache.get(path));
            assertFalse("and deleted from disk", Files.exists(path));
            assertEquals("budget released", 0L, mgr.perSegmentWholeFileBytes(0));
        }

        // The slot was released: an mmap-served file now promotes.
        FullFileState ok = newState(tm, fileCache, "_ok.faiss", length);
        assertTrue(ok.casState(FullFileState.State.BLOCK, FullFileState.State.PROMOTING));
        mgr.promote(ok);
        awaitState(ok, FullFileState.State.FULL);
    }

    public void testNioDeclineWarnsOncePerExtensionThenDebug() throws Exception {
        // One operator mistake (an NIO-served extension) must not produce one WARN per segment per shard: the first
        // file of each extension is a WARN, later files of that extension are DEBUG. Other extensions get their own WARN.
        long length = 4096;
        FileCache fileCache = FileCacheFactory.createConcurrentLRUFileCache(10_000_000, 1);
        TransferManager tm = new TransferManager(new PositionDependentStreamReader(), fileCache, threadPool);
        WarmFullFileManager mgr = new WarmFullFileManager(fileCache, Runnable::run, threadPool::relativeTimeInMillis, 4, 0.5, 1, 1024);
        String loggerName = WarmFullFileManager.class.getName();
        try (
            org.apache.lucene.store.NIOFSDirectory nio = new org.apache.lucene.store.NIOFSDirectory(dir);
            org.opensearch.test.MockLogAppender appender = org.opensearch.test.MockLogAppender.createForLoggers(
                org.apache.logging.log4j.LogManager.getLogger(WarmFullFileManager.class)
            )
        ) {
            appender.addExpectation(
                new org.opensearch.test.MockLogAppender.SeenEventExpectation(
                    "one WARN for faiss",
                    loggerName,
                    org.apache.logging.log4j.Level.WARN,
                    "*[.faiss]*first file [" + dir.resolve("_a.faiss") + "]*"
                )
            );
            appender.addExpectation(
                new org.opensearch.test.MockLogAppender.UnseenEventExpectation(
                    "no second WARN for faiss",
                    loggerName,
                    org.apache.logging.log4j.Level.WARN,
                    "*first file [" + dir.resolve("_b.faiss") + "]*"
                )
            );
            appender.addExpectation(
                new org.opensearch.test.MockLogAppender.SeenEventExpectation(
                    "one WARN for veq",
                    loggerName,
                    org.apache.logging.log4j.Level.WARN,
                    "*[.veq]*first file [" + dir.resolve("_c.veq") + "]*"
                )
            );
            for (String name : new String[] { "_a.faiss", "_b.faiss", "_c.veq" }) {
                FullFileState st = new FullFileState(tm, nio, null, fileCache, name, length, dir.resolve(name));
                assertTrue(st.casState(FullFileState.State.BLOCK, FullFileState.State.PROMOTING));
                mgr.promote(st);
                assertBusy(() -> assertEquals(Long.MAX_VALUE, st.retryAt()), 10, java.util.concurrent.TimeUnit.SECONDS);
                assertEquals(FullFileState.State.BLOCK, st.state());
            }
            appender.assertAllExpectationsMatched();
        }
    }

    public void testPromoteOnClosedFamilyTakesNoSlotAndReadsNothing() throws Exception {
        // The trigger CAS'd BLOCK -> PROMOTING and submitted promote(); the shard then closed before the executor ran it.
        // promote() must return at once: no slot, no budget reservation, no remote read, no file.
        FileCache fileCache = FileCacheFactory.createConcurrentLRUFileCache(10_000_000, 1);
        PositionDependentStreamReader reader = new PositionDependentStreamReader();
        TransferManager tm = new TransferManager(reader, fileCache, threadPool);
        // One slot: if promote() took it before noticing the closed family, the promotion below would be declined.
        WarmFullFileManager mgr = new WarmFullFileManager(fileCache, Runnable::run, threadPool::relativeTimeInMillis, 1, 0.5, 1, 1024);

        FullFileState closed = newState(tm, fileCache, "_closed.faiss", 4096);
        assertTrue(closed.casState(FullFileState.State.BLOCK, FullFileState.State.PROMOTING));
        closed.closeFamily();
        mgr.promote(closed);

        assertEquals(FullFileState.State.CLOSED, closed.state());
        assertEquals("no remote read", 0, reader.readCount());
        assertEquals("no bytes reserved", 0L, mgr.perSegmentWholeFileBytes(0));
        assertNull("nothing published", closed.localMaster());
        assertFalse("no file written", Files.exists(dir.resolve("_closed.faiss")));
        assertNull("no cache entry", fileCache.get(dir.resolve("_closed.faiss")));

        // The single slot was never taken: a live family promotes.
        FullFileState ok = newState(tm, fileCache, "_live.faiss", 4096);
        assertTrue(ok.casState(FullFileState.State.BLOCK, FullFileState.State.PROMOTING));
        mgr.promote(ok);
        awaitState(ok, FullFileState.State.FULL);
    }

    public void testExtensionOf() {
        assertEquals("faiss", WarmFullFileManager.extensionOf("_0.faiss"));
        assertEquals("veq", WarmFullFileManager.extensionOf("_0_Lucene104_0.veq"));
        assertEquals("segments_5", WarmFullFileManager.extensionOf("segments_5"));
    }

    public void testBudgetDeclinesDoNotLeakDownloadSlots() throws Exception {
        // One download slot, and a budget of half a 10,000-byte segment = 5,000 bytes. Each 8,192-byte file is declined
        // on budget AFTER the slot is taken; if a decline kept the slot, the following 4,096-byte file would be declined
        // with "no free download slot" instead of promoting.
        FileCache fileCache = FileCacheFactory.createConcurrentLRUFileCache(10_000, 1);
        PositionDependentStreamReader reader = new PositionDependentStreamReader();
        TransferManager tm = new TransferManager(reader, fileCache, threadPool);
        WarmFullFileManager mgr = new WarmFullFileManager(fileCache, Runnable::run, threadPool::relativeTimeInMillis, 1, 0.5, 1, 1024);

        for (int i = 0; i < 3; i++) {
            FullFileState big = newState(tm, fileCache, "_big" + i + ".faiss", 8192);
            assertTrue(big.casState(FullFileState.State.BLOCK, FullFileState.State.PROMOTING));
            mgr.promote(big);
            assertEquals("declined on budget", FullFileState.State.BLOCK, big.state());
        }
        assertEquals("no download for budget declines", 0, reader.readCount());

        FullFileState small = newState(tm, fileCache, "_small.faiss", 4096);
        assertTrue(small.casState(FullFileState.State.BLOCK, FullFileState.State.PROMOTING));
        mgr.promote(small);
        awaitState(small, FullFileState.State.FULL);
        assertEquals(4096L, mgr.perSegmentWholeFileBytes(0));
    }

    public void testFileAtOrAboveMmapChunkSizeIsDeclinedWithoutDownload() throws Exception {
        // A local MMapDirectory with a 2 KiB chunk stands in for a file >= Lucene's 16 GiB default chunk.
        try (org.apache.lucene.store.MMapDirectory smallChunks = new org.apache.lucene.store.MMapDirectory(dir, 2048)) {
            assertEquals(2048, WarmFullFileManager.maxChunkSize(smallChunks));
            String name = "_13.faiss";
            long length = 4096;
            PositionDependentStreamReader reader = new PositionDependentStreamReader();
            FileCache fileCache = FileCacheFactory.createConcurrentLRUFileCache(10_000_000, 1);
            TransferManager tm = new TransferManager(reader, fileCache, threadPool);
            AtomicLong clock = new AtomicLong(100);
            WarmFullFileManager mgr = new WarmFullFileManager(fileCache, Runnable::run, clock::get, 4, 0.5, 1, 1024);
            FullFileState st = new FullFileState(tm, smallChunks, null, fileCache, name, length, dir.resolve(name));
            assertTrue(st.casState(FullFileState.State.BLOCK, FullFileState.State.PROMOTING));

            mgr.promote(st);

            assertEquals(FullFileState.State.BLOCK, st.state());
            assertEquals(100 + WarmFullFileManager.RETRY_DELAY_MILLIS, st.retryAt());
            assertEquals("no download", 0, reader.readCount());
            assertEquals("no bytes reserved", 0L, mgr.perSegmentWholeFileBytes(0));
        }
        assertEquals(
            "the default limit is Lucene's default chunk size",
            org.apache.lucene.store.MMapDirectory.DEFAULT_MAX_CHUNK_SIZE,
            WarmFullFileManager.MAX_WHOLE_FILE_BYTES
        );
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

/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */
package org.opensearch.storage.warmfullfile;

import com.carrotsearch.randomizedtesting.annotations.ThreadLeakFilters;

import org.apache.lucene.store.FSDirectory;
import org.apache.lucene.store.IOContext;
import org.apache.lucene.store.IndexInput;
import org.opensearch.index.store.remote.file.CleanerDaemonThreadLeakFilter;
import org.opensearch.index.store.remote.filecache.CachedFullFileIndexInput;
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
import java.io.FilterInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

// CachedFullFileIndexInput (used by the pre-existing-entry tests) registers with a Cleaner whose daemon thread is never
// stopped; the filter excludes that one thread from leak detection, as the other FileCache tests do.
@ThreadLeakFilters(filters = CleanerDaemonThreadLeakFilter.class)
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
    static final class PositionDependentStreamReader implements TransferManager.StreamReader {
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

    /** Delegates to {@code inner} once {@code release} is counted down; every read blocks until then. */
    private static TransferManager.StreamReader blockingReader(CountDownLatch release, TransferManager.StreamReader inner) {
        return (name, position, length) -> {
            try {
                release.await();
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
            return inner.read(name, position, length);
        };
    }

    /** Serves position-dependent bytes but throws from the {@code failingCall}-th read (1-based) instead of returning a stream. */
    private static final class FailingPartStreamReader implements TransferManager.StreamReader {
        private final PositionDependentStreamReader inner = new PositionDependentStreamReader();
        private final AtomicInteger calls = new AtomicInteger(0);
        private final int failingCall;

        FailingPartStreamReader(int failingCall) {
            this.failingCall = failingCall;
        }

        @Override
        public InputStream read(String name, long position, long length) throws IOException {
            if (calls.incrementAndGet() == failingCall) {
                throw new IOException("simulated failure on part " + failingCall);
            }
            return inner.read(name, position, length);
        }

        /** @return the number of streams actually handed out (the failing call hands out none) */
        int streamsReturned() {
            return inner.readCount();
        }
    }

    /** Wraps every stream the delegate returns so that the number of opened and closed streams can be compared. */
    private static final class CloseCountingStreamReader implements TransferManager.StreamReader {
        private final TransferManager.StreamReader delegate;
        private final AtomicInteger opened = new AtomicInteger(0);
        private final AtomicInteger closed = new AtomicInteger(0);

        CloseCountingStreamReader(TransferManager.StreamReader delegate) {
            this.delegate = delegate;
        }

        @Override
        public InputStream read(String name, long position, long length) throws IOException {
            InputStream in = delegate.read(name, position, length);
            opened.incrementAndGet();
            return new FilterInputStream(in) {
                private boolean closedOnce;

                @Override
                public void close() throws IOException {
                    if (closedOnce == false) {
                        closedOnce = true;
                        closed.incrementAndGet();
                    }
                    super.close();
                }
            };
        }

        int opened() {
            return opened.get();
        }

        int closed() {
            return closed.get();
        }
    }

    /** Reads the whole content of the family's published mapping through a clone (the master's position is untouched). */
    private static byte[] readAll(IndexInput master, long length) throws IOException {
        IndexInput clone = master.clone();
        clone.seek(0);
        byte[] actual = new byte[(int) length];
        clone.readBytes(actual, 0, (int) length);
        return actual;
    }

    private byte[] wrongBytes(long length) {
        byte[] b = new byte[(int) length];
        java.util.Arrays.fill(b, (byte) 0x7f);
        return b;
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

    public void testTwoCompletionsForOneFamilyKeepTheFirstMappingAndUnwindTheSecond() throws Exception {
        // Two promote() calls for the same family join one in-flight download, so two completion handlers run for one
        // completion. Exactly one mapping must be installed and kept; the other handler closes its own mapping and
        // releases its own slot and budget without touching the installed one, the state ends FULL and retryAt is not
        // set on a FULL family.
        long length = 4096;
        FileCache fileCache = FileCacheFactory.createConcurrentLRUFileCache(10_000_000, 1);
        PositionDependentStreamReader reader = new PositionDependentStreamReader();
        CountDownLatch release = new CountDownLatch(1);
        TransferManager tm = new TransferManager(blockingReader(release, reader), fileCache, threadPool);
        AtomicLong clock = new AtomicLong(1_000);
        WarmFullFileManager mgr = new WarmFullFileManager(fileCache, Runnable::run, clock::get, 2, 0.5, 1, 1024);
        Path path = dir.resolve("_twice.faiss");
        FullFileState st = newState(tm, fileCache, "_twice.faiss", length);
        assertTrue(st.casState(FullFileState.State.BLOCK, FullFileState.State.PROMOTING));

        mgr.promote(st); // starts the download and holds slot 1
        mgr.promote(st); // joins the in-flight download and holds slot 2
        assertEquals("both calls reserved budget while in flight", 2 * length, mgr.perSegmentWholeFileBytes(0));
        release.countDown();

        awaitState(st, FullFileState.State.FULL);
        assertBusy(() -> assertEquals("the losing handler released its budget", length, mgr.perSegmentWholeFileBytes(0)));
        assertNotNull("the winning mapping is kept", st.localMaster());
        assertArrayEquals(expectedBytes(length), readAll(st.localMaster(), length));
        assertEquals("downloaded once", 4, reader.readCount());
        assertEquals("exactly the family's reference", Integer.valueOf(1), fileCache.getRef(path));
        assertEquals("no retry set on a FULL family", 0L, st.retryAt());

        // Both slots are free again: two further families are admitted at the same time.
        CountDownLatch release2 = new CountDownLatch(1);
        TransferManager tm2 = new TransferManager(blockingReader(release2, new PositionDependentStreamReader()), fileCache, threadPool);
        FullFileState a = newState(tm2, fileCache, "_a.faiss", length);
        FullFileState b = newState(tm2, fileCache, "_b.faiss", length);
        assertTrue(a.casState(FullFileState.State.BLOCK, FullFileState.State.PROMOTING));
        assertTrue(b.casState(FullFileState.State.BLOCK, FullFileState.State.PROMOTING));
        mgr.promote(a);
        mgr.promote(b);
        assertEquals("neither was declined for lack of a slot", FullFileState.State.PROMOTING, a.state());
        assertEquals(FullFileState.State.PROMOTING, b.state());
        release2.countDown();
        awaitState(a, FullFileState.State.FULL);
        awaitState(b, FullFileState.State.FULL);
    }

    public void testBeforeDownloadHookRunsOncePerDownloadAfterAdmissionAndBeforeAnyRead() throws Exception {
        // A blocking hook holds the download at a known point: the slot and the budget are already held, nothing has
        // been read from the remote store yet, and another family is declined for lack of a slot until the hook
        // returns. The hook runs exactly once per download.
        long length = 4096;
        FileCache fileCache = FileCacheFactory.createConcurrentLRUFileCache(10_000_000, 1);
        PositionDependentStreamReader reader = new PositionDependentStreamReader();
        TransferManager tm = new TransferManager(reader, fileCache, threadPool);
        AtomicLong clock = new AtomicLong(5_000);
        WarmFullFileManager mgr = new WarmFullFileManager(fileCache, Runnable::run, clock::get, 1, 0.5, 1, 1024);
        AtomicInteger hookRuns = new AtomicInteger();
        CountDownLatch hookEntered = new CountDownLatch(1);
        CountDownLatch release = new CountDownLatch(1);
        mgr.setBeforeDownloadHook(() -> {
            hookRuns.incrementAndGet();
            hookEntered.countDown();
            try {
                release.await();
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
        });

        FullFileState held = newState(tm, fileCache, "_held.faiss", length);
        assertTrue(held.casState(FullFileState.State.BLOCK, FullFileState.State.PROMOTING));
        threadPool.generic().execute(() -> mgr.promote(held));
        assertTrue("hook entered", hookEntered.await(30, java.util.concurrent.TimeUnit.SECONDS));

        assertEquals("nothing read while held", 0, reader.readCount());
        assertEquals("budget held", length, mgr.perSegmentWholeFileBytes(0));
        assertEquals(FullFileState.State.PROMOTING, held.state());
        FullFileState other = newState(tm, fileCache, "_other.faiss", length);
        assertTrue(other.casState(FullFileState.State.BLOCK, FullFileState.State.PROMOTING));
        mgr.promote(other);
        assertEquals("slot held, so the other family is declined", FullFileState.State.BLOCK, other.state());
        assertEquals(5_000 + WarmFullFileManager.RETRY_DELAY_MILLIS, other.retryAt());
        assertEquals("the declined family did not run the hook", 1, hookRuns.get());

        release.countDown();
        awaitState(held, FullFileState.State.FULL);
        assertEquals("one download", 4, reader.readCount());
        assertEquals("hook ran once for that download", 1, hookRuns.get());

        mgr.setBeforeDownloadHook(null);
        FullFileState plain = newState(tm, fileCache, "_plain.faiss", length);
        assertTrue(plain.casState(FullFileState.State.BLOCK, FullFileState.State.PROMOTING));
        mgr.promote(plain);
        awaitState(plain, FullFileState.State.FULL);
        assertEquals("no-op hook restored", 1, hookRuns.get());
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

    // ---------- admission: download slots ----------

    public void testSlotDeclineReturnsToBlockWithoutReservingOrReading() throws Exception {
        // One slot, a generous budget. The first family's download holds the slot (its reader blocks), so the second
        // family is declined on the slot alone: BLOCK, retryAt = now + 60 s, no bytes reserved for it, no remote read.
        long length = 4096;
        CountDownLatch release = new CountDownLatch(1);
        PositionDependentStreamReader firstReader = new PositionDependentStreamReader();
        PositionDependentStreamReader secondReader = new PositionDependentStreamReader();
        FileCache fileCache = FileCacheFactory.createConcurrentLRUFileCache(10_000_000, 1);
        TransferManager blockedTm = new TransferManager(blockingReader(release, firstReader), fileCache, threadPool);
        TransferManager freeTm = new TransferManager(secondReader, fileCache, threadPool);
        AtomicLong clock = new AtomicLong(5_000);
        WarmFullFileManager mgr = new WarmFullFileManager(fileCache, Runnable::run, clock::get, 1, 0.5, 1, 1024);

        FullFileState first = newState(blockedTm, fileCache, "_slot1.faiss", length);
        assertTrue(first.casState(FullFileState.State.BLOCK, FullFileState.State.PROMOTING));
        mgr.promote(first); // returns with the download queued and the single slot held
        assertEquals(FullFileState.State.PROMOTING, first.state());
        assertEquals(length, mgr.perSegmentWholeFileBytes(0));

        FullFileState second = newState(freeTm, fileCache, "_slot2.faiss", length);
        assertTrue(second.casState(FullFileState.State.BLOCK, FullFileState.State.PROMOTING));
        mgr.promote(second);

        assertEquals("declined on the slot", FullFileState.State.BLOCK, second.state());
        assertEquals(5_000 + WarmFullFileManager.RETRY_DELAY_MILLIS, second.retryAt());
        assertEquals("only the first family's bytes are reserved", length, mgr.perSegmentWholeFileBytes(0));
        assertEquals("no remote read for the declined family", 0, secondReader.readCount());
        assertFalse("no file for the declined family", Files.exists(dir.resolve("_slot2.faiss")));
        assertNull("no cache entry for the declined family", fileCache.get(dir.resolve("_slot2.faiss")));
        assertEquals("the first download has not started reading", 0, firstReader.readCount());

        release.countDown();
        awaitState(first, FullFileState.State.FULL);
        assertEquals(4, firstReader.readCount());

        // The slot is free again once the first family is FULL: the declined family now promotes.
        assertTrue(second.casState(FullFileState.State.BLOCK, FullFileState.State.PROMOTING));
        mgr.promote(second);
        awaitState(second, FullFileState.State.FULL);
        assertEquals(4, secondReader.readCount());
    }

    // ---------- failure in the middle of a multi-part download ----------

    public void testMiddlePartFailureLeavesNoFileOrEntryAndReleasesSlotAndBytes() throws Exception {
        // The reader throws on the 3rd of 4 parts. The partial file and its cache entry must be gone, the family back
        // in BLOCK with retryAt set, and the slot and bytes released (a 1-slot manager then promotes another file).
        long length = 4096;
        String name = "_mid.faiss";
        Path path = dir.resolve(name);
        FailingPartStreamReader failing = new FailingPartStreamReader(3);
        FileCache fileCache = FileCacheFactory.createConcurrentLRUFileCache(10_000_000, 1);
        TransferManager tm = new TransferManager(failing, fileCache, threadPool);
        AtomicLong clock = new AtomicLong(700);
        WarmFullFileManager mgr = new WarmFullFileManager(fileCache, Runnable::run, clock::get, 1, 0.5, 1, 1024);

        FullFileState st = newState(tm, fileCache, name, length);
        assertTrue(st.casState(FullFileState.State.BLOCK, FullFileState.State.PROMOTING));
        mgr.promote(st);

        awaitState(st, FullFileState.State.BLOCK);
        assertEquals(700 + WarmFullFileManager.RETRY_DELAY_MILLIS, st.retryAt());
        assertNull("nothing published", st.localMaster());
        assertEquals("two parts were read before the failure", 2, failing.streamsReturned());
        assertFalse("partial file deleted", Files.exists(path));
        assertNull("no cache entry left at the whole-file key", fileCache.get(path));
        assertEquals("bytes released", 0L, mgr.perSegmentWholeFileBytes(0));
        assertEquals("no usage left in the cache", 0L, fileCache.usage());

        // The slot was released: a following promotion of another file succeeds with the single slot.
        PositionDependentStreamReader good = new PositionDependentStreamReader();
        TransferManager goodTm = new TransferManager(good, fileCache, threadPool);
        FullFileState ok = newState(goodTm, fileCache, "_after_mid.faiss", length);
        assertTrue(ok.casState(FullFileState.State.BLOCK, FullFileState.State.PROMOTING));
        mgr.promote(ok);
        awaitState(ok, FullFileState.State.FULL);
        assertEquals(4, good.readCount());
        assertEquals(length, mgr.perSegmentWholeFileBytes(0));
    }

    // ---------- every reader stream is closed ----------

    public void testEveryReaderStreamIsClosedOnSuccess() throws Exception {
        long length = 4096;
        CloseCountingStreamReader counting = new CloseCountingStreamReader(new PositionDependentStreamReader());
        FileCache fileCache = FileCacheFactory.createConcurrentLRUFileCache(10_000_000, 1);
        TransferManager tm = new TransferManager(counting, fileCache, threadPool);
        WarmFullFileManager mgr = new WarmFullFileManager(fileCache, Runnable::run, threadPool::relativeTimeInMillis, 4, 0.5, 1, 1024);

        FullFileState st = newState(tm, fileCache, "_closed_ok.faiss", length);
        assertTrue(st.casState(FullFileState.State.BLOCK, FullFileState.State.PROMOTING));
        mgr.promote(st);
        awaitState(st, FullFileState.State.FULL);

        assertEquals("one stream per part", 4, counting.opened());
        assertEquals("every stream closed", counting.opened(), counting.closed());
    }

    public void testEveryReaderStreamIsClosedOnMiddlePartFailure() throws Exception {
        long length = 4096;
        CloseCountingStreamReader counting = new CloseCountingStreamReader(new FailingPartStreamReader(3));
        FileCache fileCache = FileCacheFactory.createConcurrentLRUFileCache(10_000_000, 1);
        TransferManager tm = new TransferManager(counting, fileCache, threadPool);
        WarmFullFileManager mgr = new WarmFullFileManager(fileCache, Runnable::run, threadPool::relativeTimeInMillis, 4, 0.5, 1, 1024);

        FullFileState st = newState(tm, fileCache, "_closed_fail.faiss", length);
        assertTrue(st.casState(FullFileState.State.BLOCK, FullFileState.State.PROMOTING));
        mgr.promote(st);
        awaitState(st, FullFileState.State.BLOCK);

        assertEquals("the two parts before the failure were opened", 2, counting.opened());
        assertEquals("both were closed", counting.opened(), counting.closed());
    }

    // ---------- pre-existing entries at the whole-file key ----------

    public void testLiveCachedFullFileEntryAtKeyIsRemovedAndRedownloaded() throws Exception {
        // A completed whole-file entry with WRONG bytes sits at the key. It must be removed (not served), and the file
        // re-downloaded from the reader.
        long length = 4096;
        String name = "_stale_full.faiss";
        Path path = dir.resolve(name);
        PositionDependentStreamReader reader = new PositionDependentStreamReader();
        FileCache fileCache = FileCacheFactory.createConcurrentLRUFileCache(10_000_000, 1);
        TransferManager tm = new TransferManager(reader, fileCache, threadPool);
        WarmFullFileManager mgr = new WarmFullFileManager(fileCache, Runnable::run, threadPool::relativeTimeInMillis, 4, 0.5, 1, 1024);

        Files.write(path, wrongBytes(length));
        IndexInput wrong = localDirectory.openInput(name, IOContext.DEFAULT);
        CachedFullFileIndexInput stale = new CachedFullFileIndexInput(fileCache, path, wrong);
        fileCache.put(path, stale);
        fileCache.decRef(path);
        assertEquals(length, fileCache.usage());

        FullFileState st = newState(tm, fileCache, name, length);
        assertTrue(st.casState(FullFileState.State.BLOCK, FullFileState.State.PROMOTING));
        mgr.promote(st);
        awaitState(st, FullFileState.State.FULL);

        assertEquals("re-downloaded in 4 parts", 4, reader.readCount());
        assertTrue("stale entry closed by its removal", stale.isClosed());
        assertArrayEquals("served bytes come from the reader", expectedBytes(length), readAll(st.localMaster(), length));
        try (IndexInput onDisk = localDirectory.openInput(name, IOContext.DEFAULT)) {
            assertArrayEquals("file on disk was rewritten", expectedBytes(length), readAll(onDisk, length));
        }
        CachedIndexInput current = fileCache.get(path);
        assertNotNull(current);
        assertNotSame("the stale entry is no longer in the cache", stale, current);
        fileCache.decRef(path);
        assertEquals("exactly the family's reference", Integer.valueOf(1), fileCache.getRef(path));
    }

    public void testRestoredPlaceholderAtKeyIsRemovedAndRedownloaded() throws Exception {
        // A restore placeholder (no open handle, file with WRONG bytes on disk) sits at the key: removed, re-downloaded.
        long length = 4096;
        String name = "_restored.faiss";
        Path path = dir.resolve(name);
        PositionDependentStreamReader reader = new PositionDependentStreamReader();
        FileCache fileCache = FileCacheFactory.createConcurrentLRUFileCache(10_000_000, 1);
        TransferManager tm = new TransferManager(reader, fileCache, threadPool);
        WarmFullFileManager mgr = new WarmFullFileManager(fileCache, Runnable::run, threadPool::relativeTimeInMillis, 4, 0.5, 1, 1024);

        Files.write(path, wrongBytes(length));
        FileCache.RestoredCachedIndexInput restored = new FileCache.RestoredCachedIndexInput(length);
        fileCache.put(path, restored);
        fileCache.decRef(path);

        FullFileState st = newState(tm, fileCache, name, length);
        assertTrue(st.casState(FullFileState.State.BLOCK, FullFileState.State.PROMOTING));
        mgr.promote(st);
        awaitState(st, FullFileState.State.FULL);

        assertEquals("re-downloaded in 4 parts", 4, reader.readCount());
        assertArrayEquals("served bytes come from the reader", expectedBytes(length), readAll(st.localMaster(), length));
        CachedIndexInput current = fileCache.get(path);
        assertNotNull(current);
        assertNotSame("the placeholder is no longer in the cache", restored, current);
        fileCache.decRef(path);
        assertEquals("exactly the family's reference", Integer.valueOf(1), fileCache.getRef(path));
    }

    public void testInFlightPlaceholderIsReusedWithoutSecondDownload() throws Exception {
        // A download of the same whole file is already in flight (its reader blocks). promote() must join it rather
        // than start another: one download's worth of reads, FULL, and exactly the family's one reference afterwards.
        long length = 4096;
        String name = "_inflight.faiss";
        Path path = dir.resolve(name);
        CountDownLatch release = new CountDownLatch(1);
        PositionDependentStreamReader inner = new PositionDependentStreamReader();
        FileCache fileCache = FileCacheFactory.createConcurrentLRUFileCache(10_000_000, 1);
        TransferManager tm = new TransferManager(blockingReader(release, inner), fileCache, threadPool);
        WarmFullFileManager mgr = new WarmFullFileManager(fileCache, Runnable::run, threadPool::relativeTimeInMillis, 4, 0.5, 1, 1024);

        BlobFetchRequest sameRequest = BlobFetchRequest.builder()
            .directory(localDirectory)
            .fileName(name)
            .blobParts(WarmFullFileManager.buildParts(name, length, 1024))
            .build();
        CompletableFuture<IndexInput> inFlight = tm.fetchBlobAsync(sameRequest);
        assertFalse(inFlight.isDone());
        CachedIndexInput placeholder = fileCache.get(path);
        assertNotNull("the in-flight placeholder is in the cache", placeholder);
        fileCache.decRef(path);

        FullFileState st = newState(tm, fileCache, name, length);
        assertTrue(st.casState(FullFileState.State.BLOCK, FullFileState.State.PROMOTING));
        mgr.promote(st);
        assertEquals("waiting on the shared download", FullFileState.State.PROMOTING, st.state());
        assertEquals("no read until the latch opens", 0, inner.readCount());

        release.countDown();
        awaitState(st, FullFileState.State.FULL);
        assertTrue(inFlight.isDone());
        assertEquals("one download's worth of part reads", 4, inner.readCount());
        assertArrayEquals(expectedBytes(length), readAll(st.localMaster(), length));
        CachedIndexInput current = fileCache.get(path);
        assertSame("the placeholder entry itself was reused", placeholder, current);
        fileCache.decRef(path);
        assertEquals("exactly the family's reference, nothing leaked", Integer.valueOf(1), fileCache.getRef(path));
        assertEquals(length, mgr.perSegmentWholeFileBytes(0));
    }

    public void testStrayFileWithoutEntryIsDeletedAndRedownloaded() throws Exception {
        // A file with WRONG bytes exists at the path but nothing in the cache refers to it: deleted and re-downloaded.
        long length = 4096;
        String name = "_stray.faiss";
        Path path = dir.resolve(name);
        PositionDependentStreamReader reader = new PositionDependentStreamReader();
        FileCache fileCache = FileCacheFactory.createConcurrentLRUFileCache(10_000_000, 1);
        TransferManager tm = new TransferManager(reader, fileCache, threadPool);
        WarmFullFileManager mgr = new WarmFullFileManager(fileCache, Runnable::run, threadPool::relativeTimeInMillis, 4, 0.5, 1, 1024);

        Files.write(path, wrongBytes(length));
        assertNull(fileCache.get(path));

        FullFileState st = newState(tm, fileCache, name, length);
        assertTrue(st.casState(FullFileState.State.BLOCK, FullFileState.State.PROMOTING));
        mgr.promote(st);
        awaitState(st, FullFileState.State.FULL);

        assertEquals("re-downloaded, one read per part", 4, reader.readCount());
        assertArrayEquals("served bytes come from the reader", expectedBytes(length), readAll(st.localMaster(), length));
        try (IndexInput onDisk = localDirectory.openInput(name, IOContext.DEFAULT)) {
            assertArrayEquals("stray file replaced on disk", expectedBytes(length), readAll(onDisk, length));
        }
        assertNotNull(fileCache.get(path));
        fileCache.decRef(path);
        assertEquals(Integer.valueOf(1), fileCache.getRef(path));
    }

    // ---------- concurrent promote() of one family ----------

    public void testConcurrentPromoteOfOneFamilyDownloadsOnce() throws Exception {
        // One state CAS'd to PROMOTING once, then promote(st) from 4 threads at once. The file is downloaded once, the
        // family ends FULL holding exactly one cache reference and one file's worth of budget, and all 4 slots are
        // free afterwards.
        long length = 4096;
        String name = "_race.faiss";
        Path path = dir.resolve(name);
        PositionDependentStreamReader reader = new PositionDependentStreamReader();
        FileCache fileCache = FileCacheFactory.createConcurrentLRUFileCache(10_000_000, 1);
        TransferManager tm = new TransferManager(reader, fileCache, threadPool);
        WarmFullFileManager mgr = new WarmFullFileManager(fileCache, Runnable::run, threadPool::relativeTimeInMillis, 4, 0.5, 1, 1024);

        FullFileState st = newState(tm, fileCache, name, length);
        assertTrue(st.casState(FullFileState.State.BLOCK, FullFileState.State.PROMOTING));

        int callers = 4;
        CountDownLatch start = new CountDownLatch(1);
        List<Thread> threads = new ArrayList<>();
        for (int i = 0; i < callers; i++) {
            Thread t = new Thread(() -> {
                try {
                    start.await();
                } catch (InterruptedException e) {
                    return;
                }
                mgr.promote(st);
            }, "promoter-" + i);
            threads.add(t);
            t.start();
        }
        start.countDown();
        for (Thread t : threads) {
            t.join(30_000);
            assertFalse("promoter stuck: " + t.getName(), t.isAlive());
        }

        assertBusy(() -> {
            assertEquals(FullFileState.State.FULL, st.state());
            assertEquals("one download's worth of part reads", 4, reader.readCount());
            assertEquals("one file's worth of budget", length, mgr.perSegmentWholeFileBytes(0));
            assertEquals("exactly the family's reference", Integer.valueOf(1), fileCache.getRef(path));
        }, 30, TimeUnit.SECONDS);
        assertEquals(length, localDirectory.fileLength(name));

        // All 4 slots are free: 4 more families are admitted at once (none declined) while their reader blocks.
        CountDownLatch release = new CountDownLatch(1);
        PositionDependentStreamReader laterReader = new PositionDependentStreamReader();
        TransferManager laterTm = new TransferManager(blockingReader(release, laterReader), fileCache, threadPool);
        List<FullFileState> later = new ArrayList<>();
        for (int i = 0; i < callers; i++) {
            FullFileState s = newState(laterTm, fileCache, "_after_race" + i + ".faiss", length);
            assertTrue(s.casState(FullFileState.State.BLOCK, FullFileState.State.PROMOTING));
            mgr.promote(s);
            assertEquals("admitted (slot " + i + " was free)", FullFileState.State.PROMOTING, s.state());
            later.add(s);
        }
        assertEquals("budget: the raced family plus 4 in flight", length * (callers + 1), mgr.perSegmentWholeFileBytes(0));
        release.countDown();
        for (FullFileState s : later) {
            awaitState(s, FullFileState.State.FULL);
        }
        assertEquals(4 * callers, laterReader.readCount());
    }
    // ---------- stats() and the promotion INFO line ----------

    public void testStatsCountPromotionAndMeasureDownloadTimeAndLogAtInfo() throws Exception {
        long length = 4096;
        String name = "_stats.faiss";
        CountDownLatch release = new CountDownLatch(1);
        PositionDependentStreamReader reader = new PositionDependentStreamReader();
        FileCache fileCache = FileCacheFactory.createConcurrentLRUFileCache(10_000_000, 1);
        TransferManager tm = new TransferManager(blockingReader(release, reader), fileCache, threadPool);
        AtomicLong clock = new AtomicLong(5_000);
        WarmFullFileManager mgr = new WarmFullFileManager(fileCache, Runnable::run, clock::get, 2, 0.5, 1, 1024);

        WarmFullFileStats before = mgr.stats();
        assertEquals(0L, before.promoted());
        assertEquals(0L, before.totalDeclined());
        assertEquals(0L, before.reservedBytes());
        assertEquals(0, before.activeDownloads());
        assertEquals("every reason is present", WarmFullFileStats.DeclineReason.values().length, before.declined().size());

        String loggerName = WarmFullFileManager.class.getName();
        try (
            org.opensearch.test.MockLogAppender appender = org.opensearch.test.MockLogAppender.createForLoggers(
                org.apache.logging.log4j.LogManager.getLogger(WarmFullFileManager.class)
            )
        ) {
            appender.addExpectation(
                new org.opensearch.test.MockLogAppender.SeenEventExpectation(
                    "INFO line with bytes, time and parts",
                    loggerName,
                    org.apache.logging.log4j.Level.INFO,
                    "promoted [*" + name + "] to a whole file: 4096 bytes in 250 ms (4 parts of 1024 bytes, cache segment 0)"
                )
            );
            FullFileState st = newState(tm, fileCache, name, length);
            assertTrue(st.casState(FullFileState.State.BLOCK, FullFileState.State.PROMOTING));
            mgr.promote(st); // the reader blocks on the latch, so the download is in flight

            WarmFullFileStats inFlight = mgr.stats();
            assertEquals("slot held while downloading", 1, inFlight.activeDownloads());
            assertEquals("bytes reserved while downloading", length, inFlight.reservedBytes());
            assertEquals(0L, inFlight.promoted());

            clock.addAndGet(250); // all download time passes before the first byte is served
            release.countDown();
            awaitState(st, FullFileState.State.FULL);

            // The counters and the INFO line follow the state change to FULL, so poll for them.
            assertBusy(() -> {
                WarmFullFileStats after = mgr.stats();
                assertEquals(1L, after.requested());
                assertEquals(1L, after.started());
                assertEquals(1L, after.promoted());
                assertEquals(0L, after.failed());
                assertEquals(length, after.downloadedBytes());
                assertEquals(length, after.promotedBytes());
                assertArrayEquals("one segment, the file's bytes", new long[] { length }, after.reservedBytesPerSegment());
                assertEquals("download time is measured with the manager clock", 250L, after.downloadMillis());
                assertEquals("FULL keeps the budget reserved", length, after.reservedBytes());
                assertEquals("slot released at FULL", 0, after.activeDownloads());
                assertEquals(0L, after.totalDeclined());
                appender.assertAllExpectationsMatched();
            }, 10, TimeUnit.SECONDS);
        }
    }

    public void testStatsCountDeclinesByReason() throws Exception {
        long length = 4096;
        FileCache fileCache = FileCacheFactory.createConcurrentLRUFileCache(10_000_000, 1);
        PositionDependentStreamReader reader = new PositionDependentStreamReader();
        TransferManager tm = new TransferManager(reader, fileCache, threadPool);

        // Budget: fraction 0 declines every file before any read.
        WarmFullFileManager budgetMgr = new WarmFullFileManager(fileCache, Runnable::run, new AtomicLong()::get, 4, 0.0, 1, 1024);
        FullFileState overBudget = newState(tm, fileCache, "_budget.faiss", length);
        assertTrue(overBudget.casState(FullFileState.State.BLOCK, FullFileState.State.PROMOTING));
        budgetMgr.promote(overBudget);
        assertEquals(FullFileState.State.BLOCK, overBudget.state());
        assertEquals(1L, budgetMgr.stats().declined().get(WarmFullFileStats.DeclineReason.BUDGET_EXCEEDED).longValue());
        assertEquals(1L, budgetMgr.stats().totalDeclined());
        assertEquals("requested but never started", 1L, budgetMgr.stats().requested());
        assertEquals(0L, budgetMgr.stats().started());
        assertEquals("an admission decline is not a failure", 0L, budgetMgr.stats().failed());

        WarmFullFileManager mgr = new WarmFullFileManager(fileCache, Runnable::run, new AtomicLong()::get, 4, 0.5, 1, 1024);

        // Too large: at the mmap chunk size, declined before taking a slot or reserving bytes.
        FullFileState huge = newState(tm, fileCache, "_huge.faiss", WarmFullFileManager.MAX_WHOLE_FILE_BYTES);
        assertTrue(huge.casState(FullFileState.State.BLOCK, FullFileState.State.PROMOTING));
        mgr.promote(huge);
        assertEquals(FullFileState.State.BLOCK, huge.state());

        // Not mmap: an NIO-served directory, declined permanently after the download.
        try (org.apache.lucene.store.NIOFSDirectory nio = new org.apache.lucene.store.NIOFSDirectory(dir)) {
            FullFileState onNio = new FullFileState(tm, nio, null, fileCache, "_nio.faiss", length, dir.resolve("_nio.faiss"));
            assertTrue(onNio.casState(FullFileState.State.BLOCK, FullFileState.State.PROMOTING));
            mgr.promote(onNio);
            assertBusy(() -> assertEquals(Long.MAX_VALUE, onNio.retryAt()), 10, TimeUnit.SECONDS);
        }

        // Download failed: the second part read throws.
        TransferManager failingTm = new TransferManager(new FailingPartStreamReader(2), fileCache, threadPool);
        FullFileState failing = newState(failingTm, fileCache, "_failing.faiss", length);
        assertTrue(failing.casState(FullFileState.State.BLOCK, FullFileState.State.PROMOTING));
        mgr.promote(failing);
        awaitState(failing, FullFileState.State.BLOCK);

        WarmFullFileStats stats = mgr.stats();
        assertEquals(1L, stats.declined().get(WarmFullFileStats.DeclineReason.TOO_LARGE).longValue());
        assertEquals(1L, stats.declined().get(WarmFullFileStats.DeclineReason.NOT_MMAP).longValue());
        assertEquals(1L, stats.declined().get(WarmFullFileStats.DeclineReason.DOWNLOAD_FAILED).longValue());
        assertEquals(0L, stats.declined().get(WarmFullFileStats.DeclineReason.NO_SLOT).longValue());
        assertEquals(3L, stats.totalDeclined());
        assertEquals("three promote() calls", 3L, stats.requested());
        assertEquals("the too-large file never started a download", 2L, stats.started());
        assertEquals("NIO and the failed part both started and did not reach FULL", 2L, stats.failed());
        assertEquals("only the NIO download completed", length, stats.downloadedBytes());
        assertEquals("nothing promoted", 0L, stats.promoted());
        assertEquals("every decline released its bytes", 0L, stats.reservedBytes());
        assertEquals("every decline released its slot", 0, stats.activeDownloads());
        assertNotNull(stats.toString());
    }

    public void testStatsCountNoSlotDecline() throws Exception {
        long length = 4096;
        FileCache fileCache = FileCacheFactory.createConcurrentLRUFileCache(10_000_000, 1);
        CountDownLatch release = new CountDownLatch(1);
        PositionDependentStreamReader reader = new PositionDependentStreamReader();
        TransferManager tm = new TransferManager(blockingReader(release, reader), fileCache, threadPool);
        WarmFullFileManager mgr = new WarmFullFileManager(fileCache, Runnable::run, new AtomicLong()::get, 1, 0.5, 1, 1024);

        FullFileState holder = newState(tm, fileCache, "_holder.faiss", length);
        assertTrue(holder.casState(FullFileState.State.BLOCK, FullFileState.State.PROMOTING));
        mgr.promote(holder); // holds the only slot while the reader blocks
        assertEquals(1, mgr.stats().activeDownloads());

        FullFileState second = newState(tm, fileCache, "_second.faiss", length);
        assertTrue(second.casState(FullFileState.State.BLOCK, FullFileState.State.PROMOTING));
        mgr.promote(second);
        assertEquals(FullFileState.State.BLOCK, second.state());
        assertEquals(1L, mgr.stats().declined().get(WarmFullFileStats.DeclineReason.NO_SLOT).longValue());
        assertEquals(2L, mgr.stats().requested());
        assertEquals("only the holder started", 1L, mgr.stats().started());

        release.countDown();
        awaitState(holder, FullFileState.State.FULL);
        assertBusy(() -> {
            assertEquals(1L, mgr.stats().promoted());
            assertEquals(0, mgr.stats().activeDownloads());
        }, 10, TimeUnit.SECONDS);
    }
}

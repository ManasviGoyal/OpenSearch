/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */
package org.opensearch.storage.warmfullfile;

import com.carrotsearch.randomizedtesting.annotations.ThreadLeakFilters;

import org.apache.logging.log4j.Level;
import org.apache.logging.log4j.LogManager;
import org.apache.lucene.codecs.CodecUtil;
import org.apache.lucene.store.FSDirectory;
import org.apache.lucene.store.IOContext;
import org.apache.lucene.store.IndexInput;
import org.apache.lucene.store.IndexOutput;
import org.apache.lucene.store.MMapDirectory;
import org.apache.lucene.store.NIOFSDirectory;
import org.opensearch.common.lucene.store.ByteArrayIndexInput;
import org.opensearch.index.store.remote.file.CleanerDaemonThreadLeakFilter;
import org.opensearch.index.store.remote.filecache.CachedFullFileIndexInput;
import org.opensearch.index.store.remote.filecache.CachedIndexInput;
import org.opensearch.index.store.remote.filecache.FileCache;
import org.opensearch.index.store.remote.filecache.FileCacheFactory;
import org.opensearch.index.store.remote.utils.BlobFetchRequest;
import org.opensearch.index.store.remote.utils.TransferManager;
import org.opensearch.storage.warmfullfile.WarmFullFileStats.DeclineReason;
import org.opensearch.test.MockLogAppender;
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
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.LongSupplier;

// CachedFullFileIndexInput (used by the pre-existing-entry tests) registers with a Cleaner whose daemon thread is never
// stopped; the filter excludes that one thread from leak detection, as the other FileCache tests do.
@ThreadLeakFilters(filters = CleanerDaemonThreadLeakFilter.class)
public class WarmFullFileManagerTests extends OpenSearchTestCase {

    /** The production-sized part used by the buildParts tests. */
    private static final long DEFAULT_PART = WarmFullFileManager.DEFAULT_PART_SIZE;
    /** Part size injected into every manager under test: a {@link #FILE_LENGTH} file downloads in 4 parts. */
    private static final long PART_SIZE = 1024;
    private static final long FILE_LENGTH = 4096;

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

    /**
     * A StreamReader that serves slices of one fixed byte array, so a download writes exactly that content (used with
     * {@link #footeredContent(long)} to download a file that carries a valid Lucene footer). Counts reads.
     */
    static final class FixedContentStreamReader implements TransferManager.StreamReader {
        private final byte[] content;
        private final AtomicInteger reads = new AtomicInteger(0);

        FixedContentStreamReader(byte[] content) {
            this.content = content;
        }

        @Override
        public InputStream read(String name, long position, long length) {
            reads.incrementAndGet();
            return new ByteArrayInputStream(content, (int) position, (int) length);
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
        Arrays.fill(b, (byte) 0x7f);
        return b;
    }

    /** A single-segment cache large enough that no test here is limited by capacity. */
    private static FileCache newFileCache() {
        return FileCacheFactory.createConcurrentLRUFileCache(10_000_000, 1);
    }

    /** A one-segment manager with {@link #PART_SIZE} parts that runs promote() on the calling thread and uses the pool clock. */
    private WarmFullFileManager newManager(FileCache fileCache, int slots, double budgetFraction) {
        return newManager(fileCache, threadPool::relativeTimeInMillis, slots, budgetFraction);
    }

    /** As {@link #newManager(FileCache, int, double)} with an injected clock, for tests that assert on retryAt. */
    private static WarmFullFileManager newManager(FileCache fileCache, LongSupplier clock, int slots, double budgetFraction) {
        return new WarmFullFileManager(fileCache, Runnable::run, clock, slots, budgetFraction, PART_SIZE);
    }

    private FullFileState newState(TransferManager tm, FileCache fileCache, String name, long length) {
        return new FullFileState(tm, localDirectory, fileCache, name, length, localDirectory.getDirectory().resolve(name));
    }

    /** What the trigger does: CAS the family BLOCK -> PROMOTING and hand it to the manager. */
    private static void startPromotion(WarmFullFileManager mgr, FullFileState st) {
        assertTrue(st.casState(FullFileState.State.BLOCK, FullFileState.State.PROMOTING));
        mgr.promote(st);
    }

    private byte[] expectedBytes(long length) {
        byte[] b = new byte[(int) length];
        for (int i = 0; i < length; i++) {
            b[i] = (byte) (i & 0xff);
        }
        return b;
    }

    private void awaitState(FullFileState st, FullFileState.State target) throws Exception {
        assertBusy(() -> assertEquals(target, st.state()), 10, TimeUnit.SECONDS);
    }

    /**
     * A whole file as the remote store holds it: {@code bodyLength} position-dependent bytes (as {@link #expectedBytes})
     * followed by a valid Lucene footer, so it passes the footer and entire-file checksum checks. Built through a
     * temporary file in {@code localDirectory} so the footer's checksum covers the exact bytes returned.
     */
    private byte[] footeredContent(long bodyLength) throws IOException {
        String tempName = "_footered_" + randomAlphaOfLength(8) + ".tmp";
        try (IndexOutput out = localDirectory.createOutput(tempName, IOContext.DEFAULT)) {
            out.writeBytes(expectedBytes(bodyLength), (int) bodyLength);
            CodecUtil.writeFooter(out);
        }
        byte[] content = Files.readAllBytes(dir.resolve(tempName));
        localDirectory.deleteFile(tempName);
        assertEquals(bodyLength + CodecUtil.footerLength(), content.length);
        return content;
    }

    /** The footer checksum of {@code footered} content, in the decimal form the remote store's upload metadata uses. */
    private static String footerChecksum(byte[] footered) throws IOException {
        try (IndexInput in = new ByteArrayIndexInput("footered", footered)) {
            return Long.toString(CodecUtil.retrieveChecksum(in));
        }
    }

    /** A family in remote mode whose remote checksum is {@code checksum}, as the directory sets it after an upload. */
    private FullFileState newStateWithChecksum(TransferManager tm, FileCache fileCache, String name, long length, String checksum) {
        FullFileState st = newState(tm, fileCache, name, length);
        st.markRemoteMode(checksum);
        return st;
    }

    /** As {@link #newManager(FileCache, int, double)} with 4 slots, half the cache as budget, and the given entire-file setting. */
    private WarmFullFileManager newManager(FileCache fileCache, boolean verifyEntireFile) {
        return new WarmFullFileManager(
            fileCache,
            Runnable::run,
            threadPool::relativeTimeInMillis,
            4,
            0.5,
            PART_SIZE,
            WarmFullFileManager.NO_OP_SCHEDULER,
            verifyEntireFile
        );
    }

    /** The number of parts a download of {@code length} bytes takes with {@link #PART_SIZE} parts. */
    private static int partsFor(String name, long length) {
        return WarmFullFileManager.buildParts(name, length, PART_SIZE).size();
    }

    /** Puts a restore placeholder for a {@code length}-byte file at {@code path} and drops the put's reference. */
    private static FileCache.RestoredCachedIndexInput putRestoredPlaceholder(FileCache fileCache, Path path, long length) {
        FileCache.RestoredCachedIndexInput restored = new FileCache.RestoredCachedIndexInput(length);
        fileCache.put(path, restored);
        fileCache.decRef(path);
        return restored;
    }

    /** Asserts the family serves {@code content} from its own mapping and holds exactly one reference on the whole-file entry. */
    private static void assertServedWithOneReference(FullFileState st, FileCache fileCache, Path path, byte[] content) throws IOException {
        assertArrayEquals("served bytes", content, readAll(st.localMaster(), content.length));
        assertNotNull(fileCache.get(path));
        fileCache.decRef(path);
        assertEquals("exactly the family's reference", Integer.valueOf(1), fileCache.getRef(path));
    }

    // ---------- buildParts (pure) ----------

    public void testBuildPartsExactMultiple() {
        List<BlobFetchRequest.BlobPart> parts = WarmFullFileManager.buildParts("_5.faiss", 2 * DEFAULT_PART, DEFAULT_PART);
        assertEquals(2, parts.size());
        assertEquals(0L, parts.get(0).getPosition());
        assertEquals(DEFAULT_PART, parts.get(0).getLength());
        assertEquals(DEFAULT_PART, parts.get(1).getPosition());
        assertEquals(DEFAULT_PART, parts.get(1).getLength());
    }

    public void testBuildPartsShortLastPart() {
        long length = 2 * DEFAULT_PART + 123;
        List<BlobFetchRequest.BlobPart> parts = WarmFullFileManager.buildParts("_5.faiss", length, DEFAULT_PART);
        assertEquals(3, parts.size());
        assertEquals(123L, parts.get(2).getLength());
        assertEquals(2 * DEFAULT_PART, parts.get(2).getPosition());
        assertEquals(length, parts.stream().mapToLong(BlobFetchRequest.BlobPart::getLength).sum());
    }

    public void testBuildPartsNoZeroLengthTail() {
        List<BlobFetchRequest.BlobPart> parts = WarmFullFileManager.buildParts("_5.faiss", DEFAULT_PART, DEFAULT_PART);
        assertEquals(1, parts.size());
        assertEquals(DEFAULT_PART, parts.get(0).getLength());
    }

    // ---------- promote(): full download path with the real TransferManager + position-dependent reader ----------

    public void testMultiPartDownloadWritesSourceBytesAndReachesFull() throws Exception {
        // A FILE_LENGTH file with PART_SIZE parts is a real 4-part download, verified byte-for-byte with
        // position-dependent bytes (catches offset bugs), reaching FULL.
        String name = "_5.faiss";
        PositionDependentStreamReader reader = new PositionDependentStreamReader();
        FileCache fileCache = newFileCache();
        TransferManager tm = new TransferManager(reader, fileCache, threadPool);
        // promote() runs on the calling thread; fetchBlobAsync still uses REMOTE_RECOVERY internally
        WarmFullFileManager mgr = newManager(fileCache, 4, 0.5);

        FullFileState st = newState(tm, fileCache, name, FILE_LENGTH);
        startPromotion(mgr, st);

        awaitState(st, FullFileState.State.FULL);
        assertNotNull(st.localMaster());

        // The downloaded local file exists with the expected length and bytes.
        assertTrue(Files.exists(dir.resolve(name)));
        assertEquals(FILE_LENGTH, localDirectory.fileLength(name));
        byte[] expected = expectedBytes(FILE_LENGTH);
        try (IndexInput in = localDirectory.openInput(name, IOContext.DEFAULT)) {
            byte[] actual = new byte[(int) FILE_LENGTH];
            in.readBytes(actual, 0, (int) FILE_LENGTH);
            assertArrayEquals("downloaded bytes must be position-dependent, not zeros", expected, actual);
        }
        // 4096 bytes with a 1024 part size -> 4 range reads, each read once.
        assertEquals("multi-part: 4 parts each read once", 4, reader.readCount());
    }

    public void testBudgetDeclineReturnsToBlockWithRetryAt() throws Exception {
        String name = "_7.faiss";
        PositionDependentStreamReader reader = new PositionDependentStreamReader();
        FileCache fileCache = newFileCache();
        TransferManager tm = new TransferManager(reader, fileCache, threadPool);
        AtomicLong clock = new AtomicLong(1000);
        // Budget fraction 0 makes every admission fail on budget.
        WarmFullFileManager mgr = newManager(fileCache, clock::get, 4, 0.0);

        FullFileState st = newState(tm, fileCache, name, FILE_LENGTH);
        startPromotion(mgr, st);

        // budget fraction 0 => cap 0 => reservation fails => decline back to BLOCK with retryAt set
        assertEquals(FullFileState.State.BLOCK, st.state());
        assertEquals(1000 + WarmFullFileManager.RETRY_DELAY_MILLIS, st.retryAt());
        assertFalse("no file downloaded on decline", Files.exists(dir.resolve(name)));
        assertEquals("no remote read on decline", 0, reader.readCount());
    }

    public void testFamilyClosedDuringDownloadTakesNoReferenceAndReleasesBytes() throws Exception {
        // The family closes mid-download and the reader is released right after: whether the cancel or the release
        // wins, the download ends without publishing, the budget is released and no family reference is held.
        String name = "_11.faiss";
        CountDownLatch release = new CountDownLatch(1);
        FileCache fileCache = newFileCache();
        TransferManager tm = new TransferManager(blockingReader(release, new PositionDependentStreamReader()), fileCache, threadPool);
        WarmFullFileManager mgr = newManager(fileCache, 4, 0.5);

        FullFileState st = newState(tm, fileCache, name, FILE_LENGTH);
        startPromotion(mgr, st); // returns once the download is queued; the reader blocks on the latch
        assertEquals("bytes reserved while downloading", FILE_LENGTH, mgr.stats().reservedBytesPerSegment()[0]);

        st.closeFamily(); // e.g. the shard closed mid-download
        release.countDown();

        assertBusy(() -> assertEquals("bytes released", 0L, mgr.stats().reservedBytesPerSegment()[0]), 30, TimeUnit.SECONDS);
        assertEquals(FullFileState.State.CLOSED, st.state());
        assertNull("nothing published", st.localMaster());
        assertFalse(st.cleanupDone());
        assertNull("the cancelled download's entry is gone", fileCache.getRef(dir.resolve(name)));
        assertEquals("counted as a close, not a download failure", 1L, mgr.stats().declined().get(DeclineReason.FAMILY_CLOSED).longValue());
    }

    public void testClosingTheFamilyCancelsABlockedDownloadAndReleasesAdmission() throws Exception {
        // The reader never returns: closing the family cancels the download anyway, and the slot, the budget, the
        // entry and the partial file are all gone without the reader ever being released.
        String name = "_cancel_blocked.faiss";
        Path path = dir.resolve(name);
        CountDownLatch release = new CountDownLatch(1);
        PositionDependentStreamReader inner = new PositionDependentStreamReader();
        FileCache fileCache = newFileCache();
        TransferManager tm = new TransferManager(blockingReader(release, inner), fileCache, threadPool);
        WarmFullFileManager mgr = newManager(fileCache, 1, 0.5);

        FullFileState st = newState(tm, fileCache, name, FILE_LENGTH);
        startPromotion(mgr, st);
        assertEquals("the one slot is held", 1, mgr.stats().activeDownloads());
        assertEquals(FILE_LENGTH, mgr.stats().reservedBytesPerSegment()[0]);

        st.closeFamily();

        assertBusy(() -> {
            assertEquals("slot released", 0, mgr.stats().activeDownloads());
            assertEquals("budget released", 0L, mgr.stats().reservedBytesPerSegment()[0]);
            assertNull("no entry left", fileCache.get(path));
            assertFalse("no partial file left", Files.exists(path));
        }, 30, TimeUnit.SECONDS);
        assertEquals("the reader was never released", 1L, release.getCount());
        assertEquals(FullFileState.State.CLOSED, st.state());
        assertNull(st.localMaster());
        WarmFullFileStats stats = mgr.stats();
        assertEquals(1L, stats.declined().get(DeclineReason.FAMILY_CLOSED).longValue());
        assertEquals(0L, stats.declined().get(DeclineReason.DOWNLOAD_FAILED).longValue());
        assertEquals(0L, stats.promoted());
        release.countDown();
    }

    public void testFamilyClosedBetweenPartsStopsTheDownloadBeforeTheNextPart() throws Exception {
        // The family closes after the second part was read: the loop stops before the third, so of the four parts only
        // two were ever requested, and nothing of the download remains.
        String name = "_cancel_between.faiss";
        Path path = dir.resolve(name);
        PositionDependentStreamReader inner = new PositionDependentStreamReader();
        AtomicReference<FullFileState> family = new AtomicReference<>();
        TransferManager.StreamReader closingOnSecondPart = (n, position, length) -> {
            InputStream stream = inner.read(n, position, length);
            if (inner.readCount() == 2) {
                family.get().closeFamily();
            }
            return stream;
        };
        FileCache fileCache = newFileCache();
        TransferManager tm = new TransferManager(closingOnSecondPart, fileCache, threadPool);
        WarmFullFileManager mgr = newManager(fileCache, 4, 0.5);

        FullFileState st = newState(tm, fileCache, name, FILE_LENGTH);
        family.set(st);
        startPromotion(mgr, st);

        assertBusy(() -> {
            assertEquals(0, mgr.stats().activeDownloads());
            assertEquals(0L, mgr.stats().reservedBytesPerSegment()[0]);
            assertNull(fileCache.get(path));
            assertFalse(Files.exists(path));
        }, 30, TimeUnit.SECONDS);
        assertEquals("the third part was never requested", 2, inner.readCount());
        assertEquals(FullFileState.State.CLOSED, st.state());
        assertEquals(1L, mgr.stats().declined().get(DeclineReason.FAMILY_CLOSED).longValue());
        assertEquals(0L, mgr.stats().declined().get(DeclineReason.DOWNLOAD_FAILED).longValue());
    }

    public void testConcurrentFamilyCloseAndPromotionDoNotDeadlock() throws Exception {
        // Race: completions publishing FULL while the families' entries are removed. Keys spread over 4
        // segments, removal runs closeFamily() inside the FileCache listener under a segment lock, and the cleanup
        // re-enters the cache. If that re-entry ever happened on the listener thread, two families whose keys hash to
        // each other's segments could deadlock AB-BA; the timeout below catches that.
        int families = 24;
        FileCache fileCache = FileCacheFactory.createConcurrentLRUFileCache(10_000_000, 4);
        TransferManager tm = new TransferManager(new PositionDependentStreamReader(), fileCache, threadPool);
        WarmFullFileManager mgr = new WarmFullFileManager(
            fileCache,
            threadPool.generic(),
            threadPool::relativeTimeInMillis,
            families,
            0.5,
            PART_SIZE
        );
        List<FullFileState> states = new ArrayList<>();
        List<Path> switchKeys = new ArrayList<>();
        for (int i = 0; i < families; i++) {
            String name = "_r" + i + ".faiss";
            FullFileState st = newState(tm, fileCache, name, FILE_LENGTH);
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

        CountDownLatch go = new CountDownLatch(1);
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
                assertEquals("budget fully released in segment " + s, 0L, mgr.stats().reservedBytesPerSegment()[s]);
            }
        }, 30, TimeUnit.SECONDS);
    }

    public void testNioMasterIsDeclinedPermanentlyAndCleanedUp() throws Exception {
        // A local directory that serves the file with NIO (as hybridfs does for extensions on its NIO list).
        String name = "_nio.faiss";
        Path path = dir.resolve(name);
        FileCache fileCache = newFileCache();
        PositionDependentStreamReader reader = new PositionDependentStreamReader();
        TransferManager tm = new TransferManager(reader, fileCache, threadPool);
        // One slot: if the decline kept it, the mmap promotion below would be declined.
        WarmFullFileManager mgr = newManager(fileCache, 1, 0.5);

        try (NIOFSDirectory nio = new NIOFSDirectory(dir)) {
            FullFileState st = new FullFileState(tm, nio, fileCache, name, FILE_LENGTH, path);
            startPromotion(mgr, st);

            assertBusy(() -> assertEquals(Long.MAX_VALUE, st.retryAt()), 10, TimeUnit.SECONDS);
            assertEquals(FullFileState.State.BLOCK, st.state());
            assertNull("nothing published", st.localMaster());
            assertEquals("downloaded once (4 parts)", 4, reader.readCount());
            assertTrue("the trigger never fires again", mgr.relativeTimeInMillis() < st.retryAt());
            assertNull("useless whole file removed from the cache", fileCache.get(path));
            assertFalse("and deleted from disk", Files.exists(path));
            assertEquals("budget released", 0L, mgr.stats().reservedBytesPerSegment()[0]);
        }

        // The slot was released: an mmap-served file now promotes.
        FullFileState ok = newState(tm, fileCache, "_ok.faiss", FILE_LENGTH);
        startPromotion(mgr, ok);
        awaitState(ok, FullFileState.State.FULL);
    }

    public void testNioDeclineWarnsOncePerExtensionThenDebug() throws Exception {
        // One operator mistake (an NIO-served extension) must not produce one WARN per segment per shard: the first
        // file of each extension is a WARN, later files of that extension are DEBUG. Other extensions get their own WARN.
        FileCache fileCache = newFileCache();
        TransferManager tm = new TransferManager(new PositionDependentStreamReader(), fileCache, threadPool);
        WarmFullFileManager mgr = newManager(fileCache, 4, 0.5);
        String loggerName = WarmFullFileManager.class.getName();
        try (
            NIOFSDirectory nio = new NIOFSDirectory(dir);
            MockLogAppender appender = MockLogAppender.createForLoggers(LogManager.getLogger(WarmFullFileManager.class))
        ) {
            appender.addExpectation(
                new MockLogAppender.SeenEventExpectation(
                    "one WARN for faiss",
                    loggerName,
                    Level.WARN,
                    "*[.faiss]*first file [" + dir.resolve("_a.faiss") + "]*"
                )
            );
            appender.addExpectation(
                new MockLogAppender.UnseenEventExpectation(
                    "no second WARN for faiss",
                    loggerName,
                    Level.WARN,
                    "*first file [" + dir.resolve("_b.faiss") + "]*"
                )
            );
            appender.addExpectation(
                new MockLogAppender.SeenEventExpectation(
                    "one WARN for veq",
                    loggerName,
                    Level.WARN,
                    "*[.veq]*first file [" + dir.resolve("_c.veq") + "]*"
                )
            );
            for (String name : new String[] { "_a.faiss", "_b.faiss", "_c.veq" }) {
                FullFileState st = new FullFileState(tm, nio, fileCache, name, FILE_LENGTH, dir.resolve(name));
                startPromotion(mgr, st);
                assertBusy(() -> assertEquals(Long.MAX_VALUE, st.retryAt()), 10, TimeUnit.SECONDS);
                assertEquals(FullFileState.State.BLOCK, st.state());
            }
            appender.assertAllExpectationsMatched();
        }
    }

    public void testPromoteOnClosedFamilyTakesNoSlotAndReadsNothing() throws Exception {
        // The trigger CAS'd BLOCK -> PROMOTING and submitted promote(); the shard then closed before the executor ran it.
        // promote() must return at once: no slot, no budget reservation, no remote read, no file.
        FileCache fileCache = newFileCache();
        PositionDependentStreamReader reader = new PositionDependentStreamReader();
        TransferManager tm = new TransferManager(reader, fileCache, threadPool);
        // One slot: if promote() took it before noticing the closed family, the promotion below would be declined.
        WarmFullFileManager mgr = newManager(fileCache, 1, 0.5);

        FullFileState closed = newState(tm, fileCache, "_closed.faiss", FILE_LENGTH);
        assertTrue(closed.casState(FullFileState.State.BLOCK, FullFileState.State.PROMOTING));
        closed.closeFamily();
        mgr.promote(closed);

        assertEquals(FullFileState.State.CLOSED, closed.state());
        assertEquals("no remote read", 0, reader.readCount());
        assertEquals("no bytes reserved", 0L, mgr.stats().reservedBytesPerSegment()[0]);
        assertNull("nothing published", closed.localMaster());
        assertFalse("no file written", Files.exists(dir.resolve("_closed.faiss")));
        assertNull("no cache entry", fileCache.get(dir.resolve("_closed.faiss")));

        // The single slot was never taken: a live family promotes.
        FullFileState ok = newState(tm, fileCache, "_live.faiss", FILE_LENGTH);
        startPromotion(mgr, ok);
        awaitState(ok, FullFileState.State.FULL);
    }

    public void testTwoCompletionsForOneFamilyKeepTheFirstMappingAndUnwindTheSecond() throws Exception {
        // Two promote() calls for the same family join one in-flight download, so two completion handlers run for one
        // completion. Exactly one mapping must be installed and kept; the other handler closes its own mapping and
        // releases its own slot and budget without touching the installed one, the state ends FULL and retryAt is not
        // set on a FULL family.
        FileCache fileCache = newFileCache();
        PositionDependentStreamReader reader = new PositionDependentStreamReader();
        CountDownLatch release = new CountDownLatch(1);
        TransferManager tm = new TransferManager(blockingReader(release, reader), fileCache, threadPool);
        AtomicLong clock = new AtomicLong(1_000);
        WarmFullFileManager mgr = newManager(fileCache, clock::get, 2, 0.5);
        Path path = dir.resolve("_twice.faiss");
        FullFileState st = newState(tm, fileCache, "_twice.faiss", FILE_LENGTH);

        startPromotion(mgr, st); // starts the download and holds slot 1
        mgr.promote(st); // joins the in-flight download and holds slot 2
        assertEquals("both calls reserved budget while in flight", 2 * FILE_LENGTH, mgr.stats().reservedBytesPerSegment()[0]);
        release.countDown();

        awaitState(st, FullFileState.State.FULL);
        assertBusy(() -> assertEquals("the losing handler released its budget", FILE_LENGTH, mgr.stats().reservedBytesPerSegment()[0]));
        assertNotNull("the winning mapping is kept", st.localMaster());
        assertArrayEquals(expectedBytes(FILE_LENGTH), readAll(st.localMaster(), FILE_LENGTH));
        assertEquals("downloaded once", 4, reader.readCount());
        assertEquals("exactly the family's reference", Integer.valueOf(1), fileCache.getRef(path));
        assertEquals("no retry set on a FULL family", 0L, st.retryAt());

        // Both slots are free again: two further families are admitted at the same time.
        CountDownLatch release2 = new CountDownLatch(1);
        TransferManager tm2 = new TransferManager(blockingReader(release2, new PositionDependentStreamReader()), fileCache, threadPool);
        FullFileState a = newState(tm2, fileCache, "_a.faiss", FILE_LENGTH);
        FullFileState b = newState(tm2, fileCache, "_b.faiss", FILE_LENGTH);
        startPromotion(mgr, a);
        startPromotion(mgr, b);
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
        FileCache fileCache = newFileCache();
        PositionDependentStreamReader reader = new PositionDependentStreamReader();
        TransferManager tm = new TransferManager(reader, fileCache, threadPool);
        AtomicLong clock = new AtomicLong(5_000);
        WarmFullFileManager mgr = newManager(fileCache, clock::get, 1, 0.5);
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

        FullFileState held = newState(tm, fileCache, "_held.faiss", FILE_LENGTH);
        assertTrue(held.casState(FullFileState.State.BLOCK, FullFileState.State.PROMOTING));
        threadPool.generic().execute(() -> mgr.promote(held));
        assertTrue("hook entered", hookEntered.await(30, TimeUnit.SECONDS));

        assertEquals("nothing read while held", 0, reader.readCount());
        assertEquals("budget held", FILE_LENGTH, mgr.stats().reservedBytesPerSegment()[0]);
        assertEquals(FullFileState.State.PROMOTING, held.state());
        FullFileState other = newState(tm, fileCache, "_other.faiss", FILE_LENGTH);
        startPromotion(mgr, other);
        assertEquals("slot held, so the other family is declined", FullFileState.State.BLOCK, other.state());
        assertEquals(5_000 + WarmFullFileManager.NO_SLOT_RETRY_DELAY_MILLIS, other.retryAt());
        assertEquals("the declined family did not run the hook", 1, hookRuns.get());

        release.countDown();
        awaitState(held, FullFileState.State.FULL);
        assertEquals("one download", 4, reader.readCount());
        assertEquals("hook ran once for that download", 1, hookRuns.get());

        mgr.setBeforeDownloadHook(null);
        FullFileState plain = newState(tm, fileCache, "_plain.faiss", FILE_LENGTH);
        startPromotion(mgr, plain);
        awaitState(plain, FullFileState.State.FULL);
        assertEquals("no-op hook restored", 1, hookRuns.get());
    }

    public void testBudgetDeclinesDoNotLeakDownloadSlots() throws Exception {
        // One download slot, and a budget of half a 10,000-byte segment = 5,000 bytes. Each 8,192-byte file is declined
        // on budget AFTER the slot is taken; if a decline kept the slot, the following 4,096-byte file would be declined
        // with "no free download slot" instead of promoting.
        FileCache fileCache = FileCacheFactory.createConcurrentLRUFileCache(10_000, 1);
        PositionDependentStreamReader reader = new PositionDependentStreamReader();
        TransferManager tm = new TransferManager(reader, fileCache, threadPool);
        WarmFullFileManager mgr = newManager(fileCache, 1, 0.5);

        for (int i = 0; i < 3; i++) {
            FullFileState big = newState(tm, fileCache, "_big" + i + ".faiss", 8192);
            startPromotion(mgr, big);
            assertEquals("declined on budget", FullFileState.State.BLOCK, big.state());
        }
        assertEquals("no download for budget declines", 0, reader.readCount());

        FullFileState small = newState(tm, fileCache, "_small.faiss", FILE_LENGTH);
        startPromotion(mgr, small);
        awaitState(small, FullFileState.State.FULL);
        assertEquals(FILE_LENGTH, mgr.stats().reservedBytesPerSegment()[0]);
    }

    public void testFileAtOrAboveMmapChunkSizeIsDeclinedWithoutDownload() throws Exception {
        // A local MMapDirectory with a 2 KiB chunk stands in for a file >= Lucene's 16 GiB default chunk.
        try (MMapDirectory smallChunks = new MMapDirectory(dir, 2048)) {
            assertEquals(2048, WarmFullFileManager.maxChunkSize(smallChunks));
            String name = "_13.faiss";
            PositionDependentStreamReader reader = new PositionDependentStreamReader();
            FileCache fileCache = newFileCache();
            TransferManager tm = new TransferManager(reader, fileCache, threadPool);
            AtomicLong clock = new AtomicLong(100);
            WarmFullFileManager mgr = newManager(fileCache, clock::get, 4, 0.5);
            FullFileState st = new FullFileState(tm, smallChunks, fileCache, name, FILE_LENGTH, dir.resolve(name));

            startPromotion(mgr, st);

            assertEquals(FullFileState.State.BLOCK, st.state());
            assertEquals(100 + WarmFullFileManager.RETRY_DELAY_MILLIS, st.retryAt());
            assertEquals("no download", 0, reader.readCount());
            assertEquals("no bytes reserved", 0L, mgr.stats().reservedBytesPerSegment()[0]);
        }
        assertEquals(
            "the default limit is Lucene's default chunk size",
            MMapDirectory.DEFAULT_MAX_CHUNK_SIZE,
            WarmFullFileManager.MAX_WHOLE_FILE_BYTES
        );
    }

    public void testLengthMismatchFailsBackToBlock() throws Exception {
        // Reader returns FEWER bytes than requested -> local file length != expected -> promotion fails back to BLOCK.
        long expectedLength = 8192;
        String name = "_9.faiss";
        TransferManager.StreamReader shortReader = (n, position, len) -> new ByteArrayInputStream(new byte[(int) (len - 1)]);
        FileCache fileCache = newFileCache();
        TransferManager tm = new TransferManager(shortReader, fileCache, threadPool);
        AtomicLong clock = new AtomicLong(500);
        WarmFullFileManager mgr = newManager(fileCache, clock::get, 4, 0.5);

        FullFileState st = newState(tm, fileCache, name, expectedLength);
        startPromotion(mgr, st);

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
        startPromotion(mgr, retry);
        awaitState(retry, FullFileState.State.FULL);
        assertEquals(expectedLength, localDirectory.fileLength(name));
        assertTrue("retry actually downloaded", goodReader.readCount() > 0);
    }

    // ---------- admission: download slots ----------

    public void testNoSlotDeclineRetriesSoonerWithoutReservingAndIsCounted() throws Exception {
        // One slot, a generous budget. The holder's download keeps the slot (its reader blocks), so the second family
        // is declined on the slot alone: BLOCK, retryAt = now + NO_SLOT_RETRY_DELAY_MILLIS (shorter than the failure
        // retry), no bytes reserved for it, no remote read, no file, and the decline is counted under NO_SLOT.
        CountDownLatch release = new CountDownLatch(1);
        PositionDependentStreamReader holderReader = new PositionDependentStreamReader();
        PositionDependentStreamReader declinedReader = new PositionDependentStreamReader();
        FileCache fileCache = newFileCache();
        TransferManager blockedTm = new TransferManager(blockingReader(release, holderReader), fileCache, threadPool);
        TransferManager freeTm = new TransferManager(declinedReader, fileCache, threadPool);
        AtomicLong clock = new AtomicLong(5_000);
        WarmFullFileManager mgr = newManager(fileCache, clock::get, 1, 0.5);

        FullFileState holder = newState(blockedTm, fileCache, "_holder.faiss", FILE_LENGTH);
        startPromotion(mgr, holder); // returns with the download queued and the single slot held
        assertEquals(FullFileState.State.PROMOTING, holder.state());
        assertEquals(FILE_LENGTH, mgr.stats().reservedBytesPerSegment()[0]);
        assertEquals(1, mgr.stats().activeDownloads());

        FullFileState declined = newState(freeTm, fileCache, "_noslot.faiss", FILE_LENGTH);
        startPromotion(mgr, declined);

        assertEquals("declined on the slot", FullFileState.State.BLOCK, declined.state());
        assertEquals(5_000 + WarmFullFileManager.NO_SLOT_RETRY_DELAY_MILLIS, declined.retryAt());
        assertTrue("the slot retry is shorter than the failure retry", declined.retryAt() < 5_000 + WarmFullFileManager.RETRY_DELAY_MILLIS);
        assertEquals("only the holder's bytes are reserved", FILE_LENGTH, mgr.stats().reservedBytesPerSegment()[0]);
        assertEquals("no remote read for the declined family", 0, declinedReader.readCount());
        assertFalse("no file for the declined family", Files.exists(dir.resolve("_noslot.faiss")));
        assertNull("no cache entry for the declined family", fileCache.get(dir.resolve("_noslot.faiss")));
        assertEquals("the holder's download has not started reading", 0, holderReader.readCount());
        WarmFullFileStats whileHeld = mgr.stats();
        assertEquals(1L, whileHeld.declined().get(WarmFullFileStats.DeclineReason.NO_SLOT).longValue());
        assertEquals(2L, whileHeld.requested());
        assertEquals("only the holder started", 1L, whileHeld.started());
        assertEquals("the holder still holds the slot", 1, whileHeld.activeDownloads());

        release.countDown();
        awaitState(holder, FullFileState.State.FULL);
        assertEquals(4, holderReader.readCount());
        assertBusy(() -> {
            assertEquals(1L, mgr.stats().promoted());
            assertEquals(0, mgr.stats().activeDownloads());
        }, 10, TimeUnit.SECONDS);

        // The slot is free again once the holder is FULL: the declined family now promotes.
        startPromotion(mgr, declined);
        awaitState(declined, FullFileState.State.FULL);
        assertEquals(4, declinedReader.readCount());
    }

    // ---------- failure in the middle of a multi-part download ----------

    public void testMiddlePartFailureLeavesNoFileOrEntryAndReleasesSlotAndBytes() throws Exception {
        // The reader throws on the 3rd of 4 parts. The partial file and its cache entry must be gone, the family back
        // in BLOCK with retryAt set, and the slot and bytes released (a 1-slot manager then promotes another file).
        String name = "_mid.faiss";
        Path path = dir.resolve(name);
        FailingPartStreamReader failing = new FailingPartStreamReader(3);
        FileCache fileCache = newFileCache();
        TransferManager tm = new TransferManager(failing, fileCache, threadPool);
        AtomicLong clock = new AtomicLong(700);
        WarmFullFileManager mgr = newManager(fileCache, clock::get, 1, 0.5);

        FullFileState st = newState(tm, fileCache, name, FILE_LENGTH);
        startPromotion(mgr, st);

        awaitState(st, FullFileState.State.BLOCK);
        assertEquals(700 + WarmFullFileManager.RETRY_DELAY_MILLIS, st.retryAt());
        assertNull("nothing published", st.localMaster());
        assertEquals("two parts were read before the failure", 2, failing.streamsReturned());
        assertFalse("partial file deleted", Files.exists(path));
        assertNull("no cache entry left at the whole-file key", fileCache.get(path));
        assertEquals("bytes released", 0L, mgr.stats().reservedBytesPerSegment()[0]);
        assertEquals("no usage left in the cache", 0L, fileCache.usage());

        // The slot was released: a following promotion of another file succeeds with the single slot.
        PositionDependentStreamReader good = new PositionDependentStreamReader();
        TransferManager goodTm = new TransferManager(good, fileCache, threadPool);
        FullFileState ok = newState(goodTm, fileCache, "_after_mid.faiss", FILE_LENGTH);
        startPromotion(mgr, ok);
        awaitState(ok, FullFileState.State.FULL);
        assertEquals(4, good.readCount());
        assertEquals(FILE_LENGTH, mgr.stats().reservedBytesPerSegment()[0]);
    }

    // ---------- every reader stream is closed ----------

    public void testEveryReaderStreamIsClosedOnSuccess() throws Exception {
        CloseCountingStreamReader counting = new CloseCountingStreamReader(new PositionDependentStreamReader());
        FileCache fileCache = newFileCache();
        TransferManager tm = new TransferManager(counting, fileCache, threadPool);
        WarmFullFileManager mgr = newManager(fileCache, 4, 0.5);

        FullFileState st = newState(tm, fileCache, "_closed_ok.faiss", FILE_LENGTH);
        startPromotion(mgr, st);
        awaitState(st, FullFileState.State.FULL);

        assertEquals("one stream per part", 4, counting.opened());
        assertEquals("every stream closed", counting.opened(), counting.closed());
    }

    public void testEveryReaderStreamIsClosedOnMiddlePartFailure() throws Exception {
        CloseCountingStreamReader counting = new CloseCountingStreamReader(new FailingPartStreamReader(3));
        FileCache fileCache = newFileCache();
        TransferManager tm = new TransferManager(counting, fileCache, threadPool);
        WarmFullFileManager mgr = newManager(fileCache, 4, 0.5);

        FullFileState st = newState(tm, fileCache, "_closed_fail.faiss", FILE_LENGTH);
        startPromotion(mgr, st);
        awaitState(st, FullFileState.State.BLOCK);

        assertEquals("the two parts before the failure were opened", 2, counting.opened());
        assertEquals("both were closed", counting.opened(), counting.closed());
    }

    // ---------- pre-existing entries at the whole-file key ----------

    public void testEntryPutAfterAdmissionCheckDeclinesAndReleasesBudgetAndSlot() throws Exception {
        // A completed whole-file entry lands at the key after promote() has checked the key and before the fetch is
        // submitted. fetchBlobAsync then returns no future for it, and the promotion must decline, giving back its slot
        // and its reserved bytes, with the family back in BLOCK.
        String name = "_late_entry.faiss";
        Path path = dir.resolve(name);
        PositionDependentStreamReader reader = new PositionDependentStreamReader();
        FileCache fileCache = newFileCache();
        TransferManager tm = new TransferManager(reader, fileCache, threadPool);
        WarmFullFileManager mgr = newManager(fileCache, 1, 0.5);
        Files.write(path, wrongBytes(FILE_LENGTH));
        IndexInput late = localDirectory.openInput(name, IOContext.DEFAULT);
        mgr.setBeforeDownloadHook(() -> {
            fileCache.put(path, new CachedFullFileIndexInput(fileCache, path, late));
            fileCache.decRef(path);
        });

        FullFileState st = newState(tm, fileCache, name, FILE_LENGTH);
        startPromotion(mgr, st);

        assertEquals(FullFileState.State.BLOCK, st.state());
        WarmFullFileStats stats = mgr.stats();
        assertEquals("counted as an error decline", 1L, stats.declined().get(WarmFullFileStats.DeclineReason.ERROR).longValue());
        assertEquals("reserved bytes given back", 0L, stats.reservedBytes());
        assertEquals("slot given back", 0, stats.activeDownloads());
        assertEquals("nothing read from the remote store", 0, reader.readCount());
        fileCache.remove(path);
    }

    public void testStuckHandleWarningFiresOnlyWhileHandlesStayOpen() throws Exception {
        // A FULL family that closes with a handle still open schedules one delayed check. The check warns if the handle
        // is still open when it runs, and stays silent once the handle has closed and the cleanup has run.
        String name = "_stuck.faiss";
        List<Runnable> scheduled = new ArrayList<>();
        List<Long> delays = new ArrayList<>();
        FileCache fileCache = newFileCache();
        TransferManager tm = new TransferManager(new PositionDependentStreamReader(), fileCache, threadPool);
        WarmFullFileManager mgr = new WarmFullFileManager(
            fileCache,
            Runnable::run,
            threadPool::relativeTimeInMillis,
            4,
            0.5,
            PART_SIZE,
            (task, delay) -> {
                scheduled.add(task);
                delays.add(delay);
            }
        );
        FullFileState st = newState(tm, fileCache, name, FILE_LENGTH);
        startPromotion(mgr, st);
        awaitState(st, FullFileState.State.FULL);
        st.incHandles();

        st.closeFamily();
        assertEquals("one check scheduled when the family closes with a handle open", 1, scheduled.size());
        assertEquals(WarmFullFileManager.STUCK_HANDLE_WARN_DELAY_MILLIS, delays.get(0).longValue());

        String loggerName = WarmFullFileManager.class.getName();
        try (MockLogAppender appender = MockLogAppender.createForLoggers(LogManager.getLogger(WarmFullFileManager.class))) {
            appender.addExpectation(
                new MockLogAppender.SeenEventExpectation(
                    "warns while the handle is open",
                    loggerName,
                    Level.WARN,
                    "*" + name + "*open handle*"
                )
            );
            scheduled.get(0).run();
            appender.assertAllExpectationsMatched();
        }

        st.decHandles(); // the last handle closes: the deferred cleanup runs on the (inline) executor
        assertBusy(() -> assertTrue(st.cleanupDone()), 10, TimeUnit.SECONDS);
        try (MockLogAppender appender = MockLogAppender.createForLoggers(LogManager.getLogger(WarmFullFileManager.class))) {
            appender.addExpectation(
                new MockLogAppender.UnseenEventExpectation("silent once cleaned up", loggerName, Level.WARN, "*" + name + "*open handle*")
            );
            scheduled.get(0).run();
            appender.assertAllExpectationsMatched();
        }
    }

    public void testLiveCachedFullFileEntryAtKeyIsRemovedAndRedownloaded() throws Exception {
        // A completed whole-file entry with WRONG bytes sits at the key. It must be removed (not served), and the file
        // re-downloaded from the reader.
        String name = "_stale_full.faiss";
        Path path = dir.resolve(name);
        PositionDependentStreamReader reader = new PositionDependentStreamReader();
        FileCache fileCache = newFileCache();
        TransferManager tm = new TransferManager(reader, fileCache, threadPool);
        WarmFullFileManager mgr = newManager(fileCache, 4, 0.5);

        Files.write(path, wrongBytes(FILE_LENGTH));
        IndexInput wrong = localDirectory.openInput(name, IOContext.DEFAULT);
        CachedFullFileIndexInput stale = new CachedFullFileIndexInput(fileCache, path, wrong);
        fileCache.put(path, stale);
        fileCache.decRef(path);
        assertEquals(FILE_LENGTH, fileCache.usage());

        FullFileState st = newState(tm, fileCache, name, FILE_LENGTH);
        startPromotion(mgr, st);
        awaitState(st, FullFileState.State.FULL);

        assertEquals("re-downloaded in 4 parts", 4, reader.readCount());
        assertTrue("stale entry closed by its removal", stale.isClosed());
        assertArrayEquals("served bytes come from the reader", expectedBytes(FILE_LENGTH), readAll(st.localMaster(), FILE_LENGTH));
        try (IndexInput onDisk = localDirectory.openInput(name, IOContext.DEFAULT)) {
            assertArrayEquals("file on disk was rewritten", expectedBytes(FILE_LENGTH), readAll(onDisk, FILE_LENGTH));
        }
        CachedIndexInput current = fileCache.get(path);
        assertNotNull(current);
        assertNotSame("the stale entry is no longer in the cache", stale, current);
        fileCache.decRef(path);
        assertEquals("exactly the family's reference", Integer.valueOf(1), fileCache.getRef(path));
    }

    public void testRestoredPlaceholderAtKeyIsRemovedAndRedownloaded() throws Exception {
        // A restore placeholder (no open handle, file with WRONG bytes on disk) sits at the key. No remote checksum is
        // known for the family (markRemoteMode was never called), so the leftover cannot be validated and is
        // re-downloaded: the placeholder is removed and the file rewritten from the reader.
        String name = "_restored.faiss";
        Path path = dir.resolve(name);
        PositionDependentStreamReader reader = new PositionDependentStreamReader();
        FileCache fileCache = newFileCache();
        TransferManager tm = new TransferManager(reader, fileCache, threadPool);
        WarmFullFileManager mgr = newManager(fileCache, 4, 0.5);

        Files.write(path, wrongBytes(FILE_LENGTH));
        FileCache.RestoredCachedIndexInput restored = new FileCache.RestoredCachedIndexInput(FILE_LENGTH);
        fileCache.put(path, restored);
        fileCache.decRef(path);

        FullFileState st = newState(tm, fileCache, name, FILE_LENGTH);
        startPromotion(mgr, st);
        awaitState(st, FullFileState.State.FULL);

        assertEquals("re-downloaded in 4 parts", 4, reader.readCount());
        assertArrayEquals("served bytes come from the reader", expectedBytes(FILE_LENGTH), readAll(st.localMaster(), FILE_LENGTH));
        CachedIndexInput current = fileCache.get(path);
        assertNotNull(current);
        assertNotSame("the placeholder is no longer in the cache", restored, current);
        fileCache.decRef(path);
        assertEquals("exactly the family's reference", Integer.valueOf(1), fileCache.getRef(path));
    }

    public void testInFlightPlaceholderIsReusedWithoutSecondDownload() throws Exception {
        // A download of the same whole file is already in flight (its reader blocks). promote() must join it rather
        // than start another: one download's worth of reads, FULL, and exactly the family's one reference afterwards.
        String name = "_inflight.faiss";
        Path path = dir.resolve(name);
        CountDownLatch release = new CountDownLatch(1);
        PositionDependentStreamReader inner = new PositionDependentStreamReader();
        FileCache fileCache = newFileCache();
        TransferManager tm = new TransferManager(blockingReader(release, inner), fileCache, threadPool);
        WarmFullFileManager mgr = newManager(fileCache, 4, 0.5);

        BlobFetchRequest sameRequest = BlobFetchRequest.builder()
            .directory(localDirectory)
            .fileName(name)
            .blobParts(WarmFullFileManager.buildParts(name, FILE_LENGTH, PART_SIZE))
            .build();
        CompletableFuture<IndexInput> inFlight = tm.fetchBlobAsync(sameRequest);
        assertFalse(inFlight.isDone());
        CachedIndexInput placeholder = fileCache.get(path);
        assertNotNull("the in-flight placeholder is in the cache", placeholder);
        fileCache.decRef(path);

        FullFileState st = newState(tm, fileCache, name, FILE_LENGTH);
        startPromotion(mgr, st);
        assertEquals("waiting on the shared download", FullFileState.State.PROMOTING, st.state());
        assertEquals("no read until the latch opens", 0, inner.readCount());

        release.countDown();
        awaitState(st, FullFileState.State.FULL);
        assertTrue(inFlight.isDone());
        assertEquals("one download's worth of part reads", 4, inner.readCount());
        assertArrayEquals(expectedBytes(FILE_LENGTH), readAll(st.localMaster(), FILE_LENGTH));
        CachedIndexInput current = fileCache.get(path);
        assertSame("the placeholder entry itself was reused", placeholder, current);
        fileCache.decRef(path);
        assertEquals("exactly the family's reference, nothing leaked", Integer.valueOf(1), fileCache.getRef(path));
        assertEquals(FILE_LENGTH, mgr.stats().reservedBytesPerSegment()[0]);
    }

    public void testStrayFileWithoutEntryIsDeletedAndRedownloaded() throws Exception {
        // A file with WRONG bytes exists at the path but nothing in the cache refers to it. No remote checksum is known
        // for the family, so the leftover cannot be validated and is re-downloaded: deleted, then rewritten from the reader.
        String name = "_stray.faiss";
        Path path = dir.resolve(name);
        PositionDependentStreamReader reader = new PositionDependentStreamReader();
        FileCache fileCache = newFileCache();
        TransferManager tm = new TransferManager(reader, fileCache, threadPool);
        WarmFullFileManager mgr = newManager(fileCache, 4, 0.5);

        Files.write(path, wrongBytes(FILE_LENGTH));
        assertNull(fileCache.get(path));

        FullFileState st = newState(tm, fileCache, name, FILE_LENGTH);
        startPromotion(mgr, st);
        awaitState(st, FullFileState.State.FULL);

        assertEquals("re-downloaded, one read per part", 4, reader.readCount());
        assertArrayEquals("served bytes come from the reader", expectedBytes(FILE_LENGTH), readAll(st.localMaster(), FILE_LENGTH));
        try (IndexInput onDisk = localDirectory.openInput(name, IOContext.DEFAULT)) {
            assertArrayEquals("stray file replaced on disk", expectedBytes(FILE_LENGTH), readAll(onDisk, FILE_LENGTH));
        }
        assertNotNull(fileCache.get(path));
        fileCache.decRef(path);
        assertEquals(Integer.valueOf(1), fileCache.getRef(path));
    }

    // ---------- whole files left on disk, validated against the remote checksum ----------

    public void testCompleteRestoredLeftoverIsReusedWithoutDownload() throws Exception {
        // A restore placeholder stands for a complete whole file whose length and footer checksum match the remote
        // store's. The file is kept and served without any remote read; the promotion is counted as reused, not started.
        String name = "_reuse_restored.faiss";
        Path path = dir.resolve(name);
        byte[] content = footeredContent(FILE_LENGTH);
        FixedContentStreamReader reader = new FixedContentStreamReader(content);
        FileCache fileCache = newFileCache();
        TransferManager tm = new TransferManager(reader, fileCache, threadPool);
        WarmFullFileManager mgr = newManager(fileCache, 4, 0.5);

        Files.write(path, content);
        FileCache.RestoredCachedIndexInput restored = putRestoredPlaceholder(fileCache, path, content.length);

        FullFileState st = newStateWithChecksum(tm, fileCache, name, content.length, footerChecksum(content));
        startPromotion(mgr, st);
        awaitState(st, FullFileState.State.FULL);

        assertEquals("nothing read from the remote store", 0, reader.readCount());
        assertServedWithOneReference(st, fileCache, path, content);
        assertNotSame("the placeholder was replaced by a live entry", restored, fileCache.get(path));
        fileCache.decRef(path);
        WarmFullFileStats stats = mgr.stats();
        assertEquals("counted as reused", 1L, stats.reused());
        assertEquals("no download started", 0L, stats.started());
        assertEquals("no bytes downloaded", 0L, stats.downloadedBytes());
        assertEquals(1L, stats.promoted());
    }

    public void testCompleteStrayFileIsReusedWithoutDownload() throws Exception {
        // A complete whole file is on disk with no cache entry at all; its length and footer checksum match the remote
        // store's. It is kept and served without any remote read, with a fresh entry holding exactly the family's reference.
        String name = "_reuse_stray.faiss";
        Path path = dir.resolve(name);
        byte[] content = footeredContent(FILE_LENGTH);
        FixedContentStreamReader reader = new FixedContentStreamReader(content);
        FileCache fileCache = newFileCache();
        TransferManager tm = new TransferManager(reader, fileCache, threadPool);
        WarmFullFileManager mgr = newManager(fileCache, 4, 0.5);

        Files.write(path, content);
        assertNull(fileCache.get(path));

        FullFileState st = newStateWithChecksum(tm, fileCache, name, content.length, footerChecksum(content));
        startPromotion(mgr, st);
        awaitState(st, FullFileState.State.FULL);

        assertEquals("nothing read from the remote store", 0, reader.readCount());
        assertServedWithOneReference(st, fileCache, path, content);
        WarmFullFileStats stats = mgr.stats();
        assertEquals("counted as reused", 1L, stats.reused());
        assertEquals("no download started", 0L, stats.started());
        assertEquals("no bytes downloaded", 0L, stats.downloadedBytes());
        assertEquals(1L, stats.promoted());
    }

    public void testTruncatedLeftoverIsDeletedAndRedownloaded() throws Exception {
        // The file under a restore placeholder is shorter than the family's length (a write that did not finish): it
        // fails the length check, the placeholder and the file are removed, and the whole file is downloaded again.
        String name = "_truncated.faiss";
        Path path = dir.resolve(name);
        byte[] content = footeredContent(FILE_LENGTH);
        FixedContentStreamReader reader = new FixedContentStreamReader(content);
        FileCache fileCache = newFileCache();
        TransferManager tm = new TransferManager(reader, fileCache, threadPool);
        WarmFullFileManager mgr = newManager(fileCache, 4, 0.5);

        Files.write(path, Arrays.copyOf(content, content.length - 100));
        putRestoredPlaceholder(fileCache, path, content.length);

        FullFileState st = newStateWithChecksum(tm, fileCache, name, content.length, footerChecksum(content));
        startPromotion(mgr, st);
        awaitState(st, FullFileState.State.FULL);

        assertEquals("re-downloaded, one read per part", partsFor(name, content.length), reader.readCount());
        assertArrayEquals("file on disk was rewritten", content, Files.readAllBytes(path));
        assertServedWithOneReference(st, fileCache, path, content);
        WarmFullFileStats stats = mgr.stats();
        assertEquals("not reused", 0L, stats.reused());
        assertEquals("one download started", 1L, stats.started());
        assertEquals(content.length, stats.downloadedBytes());
    }

    public void testFullLengthLeftoverWithWrongFooterChecksumIsRedownloaded() throws Exception {
        // The file under a restore placeholder has the right length but its footer carries a different CRC than the
        // remote store's: it fails the footer check, is removed with its placeholder, and the whole file is downloaded again.
        String name = "_wrong_footer.faiss";
        Path path = dir.resolve(name);
        byte[] content = footeredContent(FILE_LENGTH);
        FixedContentStreamReader reader = new FixedContentStreamReader(content);
        FileCache fileCache = newFileCache();
        TransferManager tm = new TransferManager(reader, fileCache, threadPool);
        WarmFullFileManager mgr = newManager(fileCache, 4, 0.5);

        byte[] wrongFooter = content.clone();
        for (int i = wrongFooter.length - 4; i < wrongFooter.length; i++) {
            wrongFooter[i] ^= (byte) 0xff; // the CRC is the last four bytes of the footer
        }
        Files.write(path, wrongFooter);
        putRestoredPlaceholder(fileCache, path, content.length);

        FullFileState st = newStateWithChecksum(tm, fileCache, name, content.length, footerChecksum(content));
        startPromotion(mgr, st);
        awaitState(st, FullFileState.State.FULL);

        assertEquals("re-downloaded, one read per part", partsFor(name, content.length), reader.readCount());
        assertArrayEquals("file on disk was rewritten", content, Files.readAllBytes(path));
        assertServedWithOneReference(st, fileCache, path, content);
        WarmFullFileStats stats = mgr.stats();
        assertEquals("not reused", 0L, stats.reused());
        assertEquals("one download started", 1L, stats.started());
        assertEquals(content.length, stats.downloadedBytes());
    }

    public void testCorruptedMiddleBytesAreRejectedOnlyWithEntireFileCheck() throws Exception {
        // A leftover with the right length and an intact footer but one flipped byte in the middle. With the entire-file
        // check on, the content checksum catches it and the file is re-downloaded. With the check off, only the length
        // and the footer are compared, so the corrupted copy is accepted and served as is.
        byte[] content = footeredContent(FILE_LENGTH);
        byte[] corrupted = content.clone();
        corrupted[corrupted.length / 2] ^= (byte) 0xff;
        String checksum = footerChecksum(content);
        assertEquals("the footer still reads the same", checksum, footerChecksum(corrupted));

        String verifiedName = "_mid_verified.faiss";
        Path verifiedPath = dir.resolve(verifiedName);
        FixedContentStreamReader verifiedReader = new FixedContentStreamReader(content);
        FileCache verifiedCache = newFileCache();
        TransferManager verifiedTm = new TransferManager(verifiedReader, verifiedCache, threadPool);
        WarmFullFileManager verifying = newManager(verifiedCache, true);
        Files.write(verifiedPath, corrupted);
        putRestoredPlaceholder(verifiedCache, verifiedPath, content.length);

        FullFileState verified = newStateWithChecksum(verifiedTm, verifiedCache, verifiedName, content.length, checksum);
        startPromotion(verifying, verified);
        awaitState(verified, FullFileState.State.FULL);

        assertEquals("re-downloaded, one read per part", partsFor(verifiedName, content.length), verifiedReader.readCount());
        assertServedWithOneReference(verified, verifiedCache, verifiedPath, content);
        assertEquals("not reused", 0L, verifying.stats().reused());
        assertEquals(1L, verifying.stats().started());

        String unverifiedName = "_mid_unverified.faiss";
        Path unverifiedPath = dir.resolve(unverifiedName);
        FixedContentStreamReader unverifiedReader = new FixedContentStreamReader(content);
        FileCache unverifiedCache = newFileCache();
        TransferManager unverifiedTm = new TransferManager(unverifiedReader, unverifiedCache, threadPool);
        WarmFullFileManager footerOnly = newManager(unverifiedCache, false);
        Files.write(unverifiedPath, corrupted);
        putRestoredPlaceholder(unverifiedCache, unverifiedPath, content.length);

        FullFileState unverified = newStateWithChecksum(unverifiedTm, unverifiedCache, unverifiedName, content.length, checksum);
        startPromotion(footerOnly, unverified);
        awaitState(unverified, FullFileState.State.FULL);

        assertEquals("nothing read from the remote store", 0, unverifiedReader.readCount());
        assertServedWithOneReference(unverified, unverifiedCache, unverifiedPath, corrupted);
        assertEquals("the corrupted copy was reused", 1L, footerOnly.stats().reused());
        assertEquals(0L, footerOnly.stats().started());
    }

    public void testFreshDownloadWithMismatchedChecksumEndsInBlockWithNothingLeft() throws Exception {
        // The download completes with the right length but its footer checksum is not the remote store's: the entry and
        // the file are dropped, the family returns to BLOCK with a retry time, the mismatch is counted, and the slot and
        // the budget are released.
        String name = "_bad_download.faiss";
        Path path = dir.resolve(name);
        byte[] content = footeredContent(FILE_LENGTH);
        FixedContentStreamReader reader = new FixedContentStreamReader(content);
        FileCache fileCache = newFileCache();
        TransferManager tm = new TransferManager(reader, fileCache, threadPool);
        AtomicLong clock = new AtomicLong(900);
        WarmFullFileManager mgr = newManager(fileCache, clock::get, 1, 0.5);

        String otherChecksum = Long.toString(Long.parseLong(footerChecksum(content)) + 1);
        FullFileState st = newStateWithChecksum(tm, fileCache, name, content.length, otherChecksum);
        startPromotion(mgr, st);

        awaitState(st, FullFileState.State.BLOCK);
        assertEquals(900 + WarmFullFileManager.RETRY_DELAY_MILLIS, st.retryAt());
        assertNull("nothing published", st.localMaster());
        assertEquals("downloaded once", partsFor(name, content.length), reader.readCount());
        assertBusy(() -> {
            assertNull("entry removed", fileCache.get(path));
            assertFalse("file deleted", Files.exists(path));
        }, 10, TimeUnit.SECONDS);
        WarmFullFileStats stats = mgr.stats();
        assertEquals(1L, stats.declined().get(WarmFullFileStats.DeclineReason.CHECKSUM_MISMATCH).longValue());
        assertEquals(1L, stats.failed());
        assertEquals(1L, stats.started());
        assertEquals(0L, stats.promoted());
        assertEquals("budget released", 0L, stats.reservedBytesPerSegment()[0]);
        assertEquals("slot released", 0, stats.activeDownloads());
    }

    // ---------- concurrent promote() of one family ----------

    public void testConcurrentPromoteOfOneFamilyDownloadsOnce() throws Exception {
        // One state CAS'd to PROMOTING once, then promote(st) from 4 threads at once. The file is downloaded once, the
        // family ends FULL holding exactly one cache reference and one file's worth of budget, and all 4 slots are
        // free afterwards.
        String name = "_race.faiss";
        Path path = dir.resolve(name);
        PositionDependentStreamReader reader = new PositionDependentStreamReader();
        FileCache fileCache = newFileCache();
        TransferManager tm = new TransferManager(reader, fileCache, threadPool);
        WarmFullFileManager mgr = newManager(fileCache, 4, 0.5);

        FullFileState st = newState(tm, fileCache, name, FILE_LENGTH);
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
            assertEquals("one file's worth of budget", FILE_LENGTH, mgr.stats().reservedBytesPerSegment()[0]);
            assertEquals("exactly the family's reference", Integer.valueOf(1), fileCache.getRef(path));
        }, 30, TimeUnit.SECONDS);
        assertEquals(FILE_LENGTH, localDirectory.fileLength(name));

        // All 4 slots are free: 4 more families are admitted at once (none declined) while their reader blocks.
        CountDownLatch release = new CountDownLatch(1);
        PositionDependentStreamReader laterReader = new PositionDependentStreamReader();
        TransferManager laterTm = new TransferManager(blockingReader(release, laterReader), fileCache, threadPool);
        List<FullFileState> later = new ArrayList<>();
        for (int i = 0; i < callers; i++) {
            FullFileState s = newState(laterTm, fileCache, "_after_race" + i + ".faiss", FILE_LENGTH);
            startPromotion(mgr, s);
            assertEquals("admitted (slot " + i + " was free)", FullFileState.State.PROMOTING, s.state());
            later.add(s);
        }
        assertEquals("budget: the raced family plus 4 in flight", FILE_LENGTH * (callers + 1), mgr.stats().reservedBytesPerSegment()[0]);
        release.countDown();
        for (FullFileState s : later) {
            awaitState(s, FullFileState.State.FULL);
        }
        assertEquals(4 * callers, laterReader.readCount());
    }

    // ---------- stats() and the promotion INFO line ----------

    public void testStatsCountPromotionAndMeasureDownloadTimeAndLogAtInfo() throws Exception {
        String name = "_stats.faiss";
        CountDownLatch release = new CountDownLatch(1);
        PositionDependentStreamReader reader = new PositionDependentStreamReader();
        FileCache fileCache = newFileCache();
        TransferManager tm = new TransferManager(blockingReader(release, reader), fileCache, threadPool);
        AtomicLong clock = new AtomicLong(5_000);
        WarmFullFileManager mgr = newManager(fileCache, clock::get, 2, 0.5);

        WarmFullFileStats before = mgr.stats();
        assertEquals(0L, before.promoted());
        assertEquals(0L, before.totalDeclined());
        assertEquals(0L, before.reservedBytes());
        assertEquals(0, before.activeDownloads());
        assertEquals("every reason is present", WarmFullFileStats.DeclineReason.values().length, before.declined().size());

        String loggerName = WarmFullFileManager.class.getName();
        try (MockLogAppender appender = MockLogAppender.createForLoggers(LogManager.getLogger(WarmFullFileManager.class))) {
            appender.addExpectation(
                new MockLogAppender.SeenEventExpectation(
                    "INFO line with bytes, time and parts",
                    loggerName,
                    Level.INFO,
                    "promoted [*" + name + "] to a whole file: 4096 bytes in 250 ms (4 parts of 1024 bytes, cache segment 0)"
                )
            );
            FullFileState st = newState(tm, fileCache, name, FILE_LENGTH);
            startPromotion(mgr, st); // the reader blocks on the latch, so the download is in flight

            WarmFullFileStats inFlight = mgr.stats();
            assertEquals("slot held while downloading", 1, inFlight.activeDownloads());
            assertEquals("bytes reserved while downloading", FILE_LENGTH, inFlight.reservedBytes());
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
                assertEquals(FILE_LENGTH, after.downloadedBytes());
                assertEquals(FILE_LENGTH, after.promotedBytes());
                assertArrayEquals("one segment, the file's bytes", new long[] { FILE_LENGTH }, after.reservedBytesPerSegment());
                assertEquals("download time is measured with the manager clock", 250L, after.downloadMillis());
                assertEquals("FULL keeps the budget reserved", FILE_LENGTH, after.reservedBytes());
                assertEquals("slot released at FULL", 0, after.activeDownloads());
                assertEquals(0L, after.totalDeclined());
                appender.assertAllExpectationsMatched();
            }, 10, TimeUnit.SECONDS);
        }
    }

    public void testStatsCountDeclinesByReason() throws Exception {
        FileCache fileCache = newFileCache();
        PositionDependentStreamReader reader = new PositionDependentStreamReader();
        TransferManager tm = new TransferManager(reader, fileCache, threadPool);

        // Budget: fraction 0 declines every file before any read.
        WarmFullFileManager budgetMgr = newManager(fileCache, new AtomicLong()::get, 4, 0.0);
        FullFileState overBudget = newState(tm, fileCache, "_budget.faiss", FILE_LENGTH);
        startPromotion(budgetMgr, overBudget);
        assertEquals(FullFileState.State.BLOCK, overBudget.state());
        assertEquals(1L, budgetMgr.stats().declined().get(WarmFullFileStats.DeclineReason.BUDGET_EXCEEDED).longValue());
        assertEquals(1L, budgetMgr.stats().totalDeclined());
        assertEquals("requested but never started", 1L, budgetMgr.stats().requested());
        assertEquals(0L, budgetMgr.stats().started());
        assertEquals("an admission decline is not a failure", 0L, budgetMgr.stats().failed());

        WarmFullFileManager mgr = newManager(fileCache, new AtomicLong()::get, 4, 0.5);

        // Too large: at the mmap chunk size, declined before taking a slot or reserving bytes.
        FullFileState huge = newState(tm, fileCache, "_huge.faiss", WarmFullFileManager.MAX_WHOLE_FILE_BYTES);
        startPromotion(mgr, huge);
        assertEquals(FullFileState.State.BLOCK, huge.state());

        // Not mmap: an NIO-served directory, declined permanently after the download.
        try (NIOFSDirectory nio = new NIOFSDirectory(dir)) {
            FullFileState onNio = new FullFileState(tm, nio, fileCache, "_nio.faiss", FILE_LENGTH, dir.resolve("_nio.faiss"));
            startPromotion(mgr, onNio);
            assertBusy(() -> assertEquals(Long.MAX_VALUE, onNio.retryAt()), 10, TimeUnit.SECONDS);
        }

        // Download failed: the second part read throws.
        TransferManager failingTm = new TransferManager(new FailingPartStreamReader(2), fileCache, threadPool);
        FullFileState failing = newState(failingTm, fileCache, "_failing.faiss", FILE_LENGTH);
        startPromotion(mgr, failing);
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
        assertEquals("only the NIO download completed", FILE_LENGTH, stats.downloadedBytes());
        assertEquals("nothing promoted", 0L, stats.promoted());
        assertEquals("every decline released its bytes", 0L, stats.reservedBytes());
        assertEquals("every decline released its slot", 0, stats.activeDownloads());
        assertNotNull(stats.toString());
    }
}

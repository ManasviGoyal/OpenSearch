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
import org.apache.lucene.store.AlreadyClosedException;
import org.apache.lucene.store.FSDirectory;
import org.apache.lucene.store.FSLockFactory;
import org.apache.lucene.store.IOContext;
import org.apache.lucene.store.IndexInput;
import org.apache.lucene.store.IndexOutput;
import org.apache.lucene.store.MMapDirectory;
import org.apache.lucene.store.NIOFSDirectory;
import org.opensearch.common.lucene.store.ByteArrayIndexInput;
import org.opensearch.common.settings.Settings;
import org.opensearch.common.unit.TimeValue;
import org.opensearch.common.util.concurrent.ThreadContext;
import org.opensearch.index.store.remote.file.CleanerDaemonThreadLeakFilter;
import org.opensearch.index.store.remote.filecache.CachedFullFileIndexInput;
import org.opensearch.index.store.remote.filecache.CachedIndexInput;
import org.opensearch.index.store.remote.filecache.FileCache;
import org.opensearch.index.store.remote.filecache.FileCacheFactory;
import org.opensearch.index.store.remote.utils.BlobFetchRequest;
import org.opensearch.index.store.remote.utils.FileTypeUtils;
import org.opensearch.index.store.remote.utils.TransferManager;
import org.opensearch.storage.warmfullfile.WarmFullFileManager.DeclineReason;
import org.opensearch.test.MockLogAppender;
import org.opensearch.test.OpenSearchTestCase;
import org.opensearch.test.junit.annotations.TestLogging;
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
import java.util.Deque;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedDeque;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executor;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.LongSupplier;
import java.util.stream.Stream;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.spy;

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
        return withFaissEligible(
            new WarmFullFileManager(fileCache, new ThreadContext(Settings.EMPTY), Runnable::run, clock, slots, budgetFraction, PART_SIZE)
        );
    }

    /** As above, with a hook that runs after admission and before the first remote read of every download. */
    private static WarmFullFileManager newManager(
        FileCache fileCache,
        LongSupplier clock,
        int slots,
        double budgetFraction,
        Runnable beforeDownloadHook
    ) {
        return withFaissEligible(
            new WarmFullFileManager(
                fileCache,
                new ThreadContext(Settings.EMPTY),
                Runnable::run,
                clock,
                slots,
                budgetFraction,
                PART_SIZE,
                beforeDownloadHook
            )
        );
    }

    /** Tells the manager the setting lists {@code faiss}, as the settings listener does in production. */
    private static WarmFullFileManager withFaissEligible(WarmFullFileManager manager) {
        manager.onExtensionsChanged(Set.of("faiss"));
        // No wait after an eviction, unless a test sets one.
        manager.onEvictionBackoffChanged(TimeValue.ZERO);
        return manager;
    }

    private FullFileState newState(TransferManager tm, FileCache fileCache, String name, long length) {
        return new FullFileState(tm, localDirectory, fileCache, name, length, localDirectory.getDirectory().resolve(name));
    }

    // The query phases the tests have open, by the family whose shard they are on; newest first.
    private final Map<FullFileState, Deque<PhaseHold>> openQueries = new ConcurrentHashMap<>();

    /** Opens a query phase on the family's shard, as the warm search listener does. */
    private void openQuery(FullFileState st) {
        openQueries.computeIfAbsent(st, key -> new ConcurrentLinkedDeque<>()).push(st.tracker().acquire());
    }

    /** Ends the query phase opened last on the family's shard. */
    private void closeQuery(FullFileState st) {
        openQueries.get(st).pop().close();
    }

    /** What the trigger does: move the family BLOCK -> PROMOTING and hand it to the manager. */
    private static void startPromotion(WarmFullFileManager mgr, FullFileState st) {
        assertTrue(st.onAccess(mgr.relativeTimeInMillis()));
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
        if (target == FullFileState.State.FULL) {
            awaitPromotionThreadsIdle();
        }
    }

    /**
     * A download's completion callback turns the family FULL and only then gives back its cache reference and its slot,
     * and counts the promotion. This waits for the pool threads that run it, so that a test reads none of those half
     * done.
     */
    private void awaitPromotionThreadsIdle() throws Exception {
        ThreadPoolExecutor recovery = (ThreadPoolExecutor) threadPool.executor(ThreadPool.Names.REMOTE_RECOVERY);
        ThreadPoolExecutor generic = (ThreadPoolExecutor) threadPool.generic();
        assertBusy(() -> {
            assertEquals(0, recovery.getActiveCount());
            assertEquals(0, generic.getActiveCount());
        }, 10, TimeUnit.SECONDS);
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
    private static void assertServedUnreferenced(FullFileState st, FileCache fileCache, Path path, byte[] content) throws IOException {
        assertArrayEquals("served bytes", content, readAll(st.localMaster(), content.length));
        assertNotNull(fileCache.get(path));
        fileCache.decRef(path);
        assertEquals("the family holds no reference; a search scope would", Integer.valueOf(0), fileCache.getRef(path));
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
        assertEquals("bytes reserved while downloading", FILE_LENGTH, mgr.reservedBytesPerSegment()[0]);

        st.closeFamily(); // e.g. the shard closed mid-download
        release.countDown();

        assertBusy(() -> assertEquals("bytes released", 0L, mgr.reservedBytesPerSegment()[0]), 30, TimeUnit.SECONDS);
        assertEquals(FullFileState.State.CLOSED, st.state());
        assertNull("nothing published", st.localMaster());
        assertFalse(st.cleanupDone());
        assertNull("the cancelled download's entry is gone", fileCache.getRef(dir.resolve(name)));
        assertEquals("counted as a close, not a download failure", 1L, mgr.counters().get("declined.FAMILY_CLOSED").longValue());
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
        assertEquals("the one slot is held", 1, mgr.counters().get("activeDownloads").longValue());
        assertEquals(FILE_LENGTH, mgr.reservedBytesPerSegment()[0]);

        st.closeFamily();

        assertBusy(() -> {
            assertEquals("slot released", 0, mgr.counters().get("activeDownloads").longValue());
            assertEquals("budget released", 0L, mgr.reservedBytesPerSegment()[0]);
            assertNull("no entry left", fileCache.get(path));
            assertFalse("no partial file left", Files.exists(path));
        }, 30, TimeUnit.SECONDS);
        assertEquals("the reader was never released", 1L, release.getCount());
        assertEquals(FullFileState.State.CLOSED, st.state());
        assertNull(st.localMaster());
        Map<String, Long> stats = mgr.counters();
        assertEquals(1L, stats.get("declined.FAMILY_CLOSED").longValue());
        assertEquals(0L, stats.get("declined.DOWNLOAD_FAILED").longValue());
        assertEquals(0L, stats.get("promoted").longValue());
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
            assertEquals(0, mgr.counters().get("activeDownloads").longValue());
            assertEquals(0L, mgr.reservedBytesPerSegment()[0]);
            assertNull(fileCache.get(path));
            assertFalse(Files.exists(path));
        }, 30, TimeUnit.SECONDS);
        assertEquals("the third part was never requested", 2, inner.readCount());
        assertEquals(FullFileState.State.CLOSED, st.state());
        assertEquals(1L, mgr.counters().get("declined.FAMILY_CLOSED").longValue());
        assertEquals(0L, mgr.counters().get("declined.DOWNLOAD_FAILED").longValue());
    }

    public void testConcurrentFamilyCloseAndPromotionDoNotDeadlock() throws Exception {
        // Race: completions publishing FULL while the families' entries are removed. Keys spread over 4
        // segments, removal runs closeFamily() inside the FileCache listener under a segment lock, and the cleanup
        // re-enters the cache. If that re-entry ever happened on the listener thread, two families whose keys hash to
        // each other's segments could deadlock AB-BA; the timeout below catches that.
        int families = 24;
        FileCache fileCache = FileCacheFactory.createConcurrentLRUFileCache(10_000_000, 4);
        TransferManager tm = new TransferManager(new PositionDependentStreamReader(), fileCache, threadPool);
        WarmFullFileManager mgr = withFaissEligible(
            new WarmFullFileManager(
                fileCache,
                threadPool.getThreadContext(),
                threadPool.generic(),
                threadPool::relativeTimeInMillis,
                families,
                0.5,
                PART_SIZE
            )
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
                if (st.onAccess(mgr.relativeTimeInMillis())) {
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
                assertEquals("budget fully released in segment " + s, 0L, mgr.reservedBytesPerSegment()[s]);
            }
        }, 30, TimeUnit.SECONDS);
    }

    public void testNioDirectoryIsDeclinedPermanentlyBeforeAnyDownload() throws Exception {
        // A local directory known to serve the file with NIO (as hybridfs does for extensions on its NIO list) is
        // declined at admission: no download, no slot, no budget, nothing on disk or in the cache.
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
            assertEquals("nothing downloaded", 0, reader.readCount());
            assertTrue("the trigger never fires again", mgr.relativeTimeInMillis() < st.retryAt());
            assertNull("no whole-file cache entry", fileCache.get(path));
            assertFalse("nothing on disk", Files.exists(path));
            assertEquals("no budget taken", 0L, mgr.reservedBytesPerSegment()[0]);
            assertEquals("never started", 0L, mgr.counters().get("started").longValue());
            assertEquals(1L, mgr.counters().get("declined.NOT_MMAP").longValue());
            assertEquals("an admission decline is not a failure", 0L, mgr.counters().get("failed").longValue());
        }

        // The slot was never taken: an mmap-served file promotes.
        FullFileState ok = newState(tm, fileCache, "_ok.faiss", FILE_LENGTH);
        startPromotion(mgr, ok);
        awaitState(ok, FullFileState.State.FULL);
    }

    public void testUnrecognizedDirectoryIsDeclinedPermanentlyAfterTheDownloadWhenNotMmap() throws Exception {
        // A directory type the admission check does not know is given the benefit of the doubt: the file is
        // downloaded, the mapping is opened, and an NIO input is declined permanently and cleaned up there.
        String name = "_custom.faiss";
        Path path = dir.resolve(name);
        FileCache fileCache = newFileCache();
        PositionDependentStreamReader reader = new PositionDependentStreamReader();
        TransferManager tm = new TransferManager(reader, fileCache, threadPool);
        WarmFullFileManager mgr = newManager(fileCache, 1, 0.5);

        try (NIOFSDirectory nio = new NIOFSDirectory(dir); FSDirectory custom = new FSDirectory(dir, FSLockFactory.getDefault()) {
            @Override
            public IndexInput openInput(String fileName, IOContext context) throws IOException {
                return nio.openInput(fileName, context);
            }
        }) {
            FullFileState st = new FullFileState(tm, custom, fileCache, name, FILE_LENGTH, path);
            startPromotion(mgr, st);

            assertBusy(() -> assertEquals(Long.MAX_VALUE, st.retryAt()), 10, TimeUnit.SECONDS);
            assertEquals(FullFileState.State.BLOCK, st.state());
            assertNull("nothing published", st.localMaster());
            assertEquals("downloaded once (4 parts)", 4, reader.readCount());
            assertNull("useless whole file removed from the cache", fileCache.get(path));
            assertFalse("and deleted from disk", Files.exists(path));
            assertEquals("budget released", 0L, mgr.reservedBytesPerSegment()[0]);
            assertEquals(1L, mgr.counters().get("declined.NOT_MMAP").longValue());
            assertEquals("a download that did not reach FULL is a failure", 1L, mgr.counters().get("failed").longValue());
        }

        // The slot was released: an mmap-served file now promotes.
        FullFileState ok = newState(tm, fileCache, "_ok.faiss", FILE_LENGTH);
        startPromotion(mgr, ok);
        awaitState(ok, FullFileState.State.FULL);
    }

    public void testAnErrorCompletingADownloadUnwindsAndIsHandedOn() throws Exception {
        // The completion callback throws an Error once the file is in place. The download's future would keep it where
        // nothing reads it, so the manager unwinds the attempt and hands the error on.
        String name = "_fatal.faiss";
        Path path = dir.resolve(name);
        FileCache fileCache = newFileCache();
        TransferManager tm = new TransferManager(new PositionDependentStreamReader(), fileCache, threadPool);
        Error fatal = new Error("simulated");
        AtomicReference<Error> handedOn = new AtomicReference<>();
        WarmFullFileManager mgr = withFaissEligible(
            new WarmFullFileManager(
                fileCache,
                new ThreadContext(Settings.EMPTY),
                Runnable::run,
                threadPool::relativeTimeInMillis,
                1,
                0.5,
                PART_SIZE
            ) {
                @Override
                void onFatalError(Error error) {
                    handedOn.set(error);
                }
            }
        );

        try (MMapDirectory mmap = new MMapDirectory(dir); FSDirectory failing = new FSDirectory(dir, FSLockFactory.getDefault()) {
            @Override
            public IndexInput openInput(String fileName, IOContext context) throws IOException {
                return mmap.openInput(fileName, context);
            }

            @Override
            public long fileLength(String fileName) throws IOException {
                if (Files.exists(path)) {
                    throw fatal;
                }
                return super.fileLength(fileName);
            }
        }) {
            FullFileState st = new FullFileState(tm, failing, fileCache, name, FILE_LENGTH, path);
            startPromotion(mgr, st);

            assertBusy(() -> assertSame("the error is handed on", fatal, handedOn.get()), 10, TimeUnit.SECONDS);
            awaitState(st, FullFileState.State.BLOCK);
            assertNull("nothing published", st.localMaster());
            assertBusy(() -> {
                Map<String, Long> stats = mgr.counters();
                assertEquals(1L, stats.get("declined.ERROR").longValue());
                assertEquals("reserved bytes given back", 0L, stats.get("reservedBytes").longValue());
                assertEquals("slot given back", 0L, stats.get("activeDownloads").longValue());
            }, 10, TimeUnit.SECONDS);
        }
        fileCache.remove(path);
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
            // The retry time is set before the line is logged, so the last WARN may land a moment after the loop.
            assertBusy(appender::assertAllExpectationsMatched, 10, TimeUnit.SECONDS);
        }
    }

    @TestLogging(value = "org.opensearch.storage.warmfullfile.WarmFullFileManager:DEBUG", reason = "the admission decline line is DEBUG")
    public void testFailureDeclinesLogAtWarnAndAdmissionDeclinesAtDebug() throws Exception {
        // A decline that means something went wrong (here a checksum mismatch on a fresh download) is a WARN. An
        // admission decline (no budget) is the normal steady state on a full cache and stays at DEBUG.
        FileCache fileCache = newFileCache();
        String loggerName = WarmFullFileManager.class.getName();
        try (MockLogAppender appender = MockLogAppender.createForLoggers(LogManager.getLogger(WarmFullFileManager.class))) {
            appender.addExpectation(
                new MockLogAppender.SeenEventExpectation(
                    "checksum mismatch is a WARN",
                    loggerName,
                    Level.WARN,
                    "*whole-file promotion declined (downloaded checksum mismatch) for *_warn.faiss*"
                )
            );
            appender.addExpectation(
                new MockLogAppender.SeenEventExpectation(
                    "budget decline is a DEBUG",
                    loggerName,
                    Level.DEBUG,
                    "*whole-file promotion declined (per-segment whole-file budget exceeded) for *_debug.faiss*"
                )
            );
            appender.addExpectation(
                new MockLogAppender.UnseenEventExpectation("budget decline is not a WARN", loggerName, Level.WARN, "*_debug.faiss*")
            );

            byte[] content = footeredContent(FILE_LENGTH);
            TransferManager tm = new TransferManager(new FixedContentStreamReader(content), fileCache, threadPool);
            WarmFullFileManager failing = newManager(fileCache, 1, 0.5);
            String otherChecksum = Long.toString(Long.parseLong(footerChecksum(content)) + 1);
            FullFileState failed = newStateWithChecksum(tm, fileCache, "_warn.faiss", content.length, otherChecksum);
            startPromotion(failing, failed);
            awaitState(failed, FullFileState.State.BLOCK);

            WarmFullFileManager noBudget = newManager(fileCache, 1, 0.0);
            FullFileState declined = newState(tm, fileCache, "_debug.faiss", FILE_LENGTH);
            startPromotion(noBudget, declined);
            assertEquals(FullFileState.State.BLOCK, declined.state());

            assertBusy(appender::assertAllExpectationsMatched, 10, TimeUnit.SECONDS);
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
        assertTrue(closed.onAccess(mgr.relativeTimeInMillis()));
        closed.closeFamily();
        mgr.promote(closed);

        assertEquals(FullFileState.State.CLOSED, closed.state());
        assertEquals("no remote read", 0, reader.readCount());
        assertEquals("no bytes reserved", 0L, mgr.reservedBytesPerSegment()[0]);
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
        assertEquals("both calls reserved budget while in flight", 2 * FILE_LENGTH, mgr.reservedBytesPerSegment()[0]);
        release.countDown();

        awaitState(st, FullFileState.State.FULL);
        assertBusy(() -> assertEquals("the losing handler released its budget", FILE_LENGTH, mgr.reservedBytesPerSegment()[0]));
        assertNotNull("the winning mapping is kept", st.localMaster());
        assertArrayEquals(expectedBytes(FILE_LENGTH), readAll(st.localMaster(), FILE_LENGTH));
        assertEquals("downloaded once", 4, reader.readCount());
        assertEquals("the family holds no reference", Integer.valueOf(0), fileCache.getRef(path));
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
        AtomicInteger hookRuns = new AtomicInteger();
        CountDownLatch hookEntered = new CountDownLatch(1);
        CountDownLatch release = new CountDownLatch(1);
        WarmFullFileManager mgr = newManager(fileCache, clock::get, 1, 0.5, () -> {
            hookRuns.incrementAndGet();
            hookEntered.countDown();
            try {
                release.await();
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
        });

        FullFileState held = newState(tm, fileCache, "_held.faiss", FILE_LENGTH);
        assertTrue(held.onAccess(mgr.relativeTimeInMillis()));
        threadPool.generic().execute(() -> mgr.promote(held));
        assertTrue("hook entered", hookEntered.await(30, TimeUnit.SECONDS));

        assertEquals("nothing read while held", 0, reader.readCount());
        assertEquals("budget held", FILE_LENGTH, mgr.reservedBytesPerSegment()[0]);
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
    }

    public void testRaisingTheDownloadCapAdmitsAFileDeclinedForNoSlot() throws Exception {
        // One slot, held by a download parked in the hook: a second family is declined for no slot. Raising the cap
        // to two admits it without waiting for the first to finish.
        FileCache fileCache = newFileCache();
        PositionDependentStreamReader reader = new PositionDependentStreamReader();
        TransferManager tm = new TransferManager(reader, fileCache, threadPool);
        AtomicLong clock = new AtomicLong(5_000);
        CountDownLatch hookEntered = new CountDownLatch(1);
        CountDownLatch release = new CountDownLatch(1);
        WarmFullFileManager mgr = newManager(fileCache, clock::get, 1, 0.5, () -> {
            hookEntered.countDown();
            try {
                release.await();
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
        });

        FullFileState held = newState(tm, fileCache, "_held.faiss", FILE_LENGTH);
        assertTrue(held.onAccess(mgr.relativeTimeInMillis()));
        threadPool.generic().execute(() -> mgr.promote(held));
        assertTrue("hook entered", hookEntered.await(30, TimeUnit.SECONDS));

        FullFileState other = newState(tm, fileCache, "_other.faiss", FILE_LENGTH);
        startPromotion(mgr, other);
        assertEquals("declined for no slot", FullFileState.State.BLOCK, other.state());
        assertEquals(1L, mgr.counters().get("declined.NO_SLOT").longValue());
        assertEquals(1L, mgr.counters().get("activeDownloads").longValue());

        mgr.onMaxConcurrentDownloadsChanged(2);
        clock.set(other.retryAt());
        // The second download runs on the calling thread and also parks in the hook, so promote it off-thread.
        assertTrue(other.onAccess(mgr.relativeTimeInMillis()));
        threadPool.generic().execute(() -> mgr.promote(other));
        assertBusy(() -> assertEquals("both admitted", 2L, mgr.counters().get("activeDownloads").longValue()), 30, TimeUnit.SECONDS);
        assertEquals("no new decline", 1L, mgr.counters().get("declined.NO_SLOT").longValue());

        release.countDown();
        awaitState(held, FullFileState.State.FULL);
        awaitState(other, FullFileState.State.FULL);
        // A download gives its slot back just after its family turns FULL.
        assertBusy(() -> assertEquals(0L, mgr.counters().get("activeDownloads").longValue()), 30, TimeUnit.SECONDS);
    }

    public void testLoweringTheDownloadCapBelowTheRunningCountAdmitsNothingUntilEnoughFinish() throws Exception {
        // Two slots, both held by downloads parked in the hook. Lowering the cap to one does not stop them; a new
        // family is declined until one of the two has finished, and admitted after that.
        FileCache fileCache = newFileCache();
        PositionDependentStreamReader reader = new PositionDependentStreamReader();
        TransferManager tm = new TransferManager(reader, fileCache, threadPool);
        AtomicLong clock = new AtomicLong(5_000);
        CountDownLatch hooksEntered = new CountDownLatch(2);
        CountDownLatch releaseFirst = new CountDownLatch(1);
        CountDownLatch releaseSecond = new CountDownLatch(1);
        AtomicInteger hookRuns = new AtomicInteger();
        WarmFullFileManager mgr = newManager(fileCache, clock::get, 2, 0.5, () -> {
            int n = hookRuns.incrementAndGet();
            hooksEntered.countDown();
            try {
                (n == 1 ? releaseFirst : releaseSecond).await();
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
        });

        FullFileState a = newState(tm, fileCache, "_a.faiss", FILE_LENGTH);
        FullFileState b = newState(tm, fileCache, "_b.faiss", FILE_LENGTH);
        assertTrue(a.onAccess(mgr.relativeTimeInMillis()));
        threadPool.generic().execute(() -> mgr.promote(a));
        assertTrue(b.onAccess(mgr.relativeTimeInMillis()));
        threadPool.generic().execute(() -> mgr.promote(b));
        assertTrue("both hooks entered", hooksEntered.await(30, TimeUnit.SECONDS));
        assertEquals(2L, mgr.counters().get("activeDownloads").longValue());

        mgr.onMaxConcurrentDownloadsChanged(1);
        assertEquals("the two in flight keep running", 2L, mgr.counters().get("activeDownloads").longValue());
        assertEquals(FullFileState.State.PROMOTING, a.state());
        assertEquals(FullFileState.State.PROMOTING, b.state());

        FullFileState c = newState(tm, fileCache, "_c.faiss", FILE_LENGTH);
        startPromotion(mgr, c);
        assertEquals("over the new cap: declined", FullFileState.State.BLOCK, c.state());
        assertEquals(1L, mgr.counters().get("declined.NO_SLOT").longValue());

        // One finishes: still at the cap (one running, cap one), so a retry is declined again.
        releaseFirst.countDown();
        assertBusy(() -> assertEquals(1L, mgr.counters().get("activeDownloads").longValue()), 30, TimeUnit.SECONDS);
        clock.set(c.retryAt());
        startPromotion(mgr, c);
        assertEquals("still at the cap", FullFileState.State.BLOCK, c.state());
        assertEquals(2L, mgr.counters().get("declined.NO_SLOT").longValue());

        // The other finishes: under the cap, the next request is admitted and runs to FULL on the calling thread.
        releaseSecond.countDown();
        assertBusy(() -> assertEquals(0L, mgr.counters().get("activeDownloads").longValue()), 30, TimeUnit.SECONDS);
        awaitState(a, FullFileState.State.FULL);
        awaitState(b, FullFileState.State.FULL);
        clock.set(c.retryAt());
        // The hook would park this third download too; let it through.
        releaseFirst.countDown();
        startPromotion(mgr, c);
        awaitState(c, FullFileState.State.FULL);
        assertEquals(2L, mgr.counters().get("declined.NO_SLOT").longValue());
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
        assertEquals(FILE_LENGTH, mgr.reservedBytesPerSegment()[0]);
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
            assertEquals("no bytes reserved", 0L, mgr.reservedBytesPerSegment()[0]);
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
        assertEquals(FILE_LENGTH, mgr.reservedBytesPerSegment()[0]);
        assertEquals(1, mgr.counters().get("activeDownloads").longValue());

        FullFileState declined = newState(freeTm, fileCache, "_noslot.faiss", FILE_LENGTH);
        startPromotion(mgr, declined);

        assertEquals("declined on the slot", FullFileState.State.BLOCK, declined.state());
        assertEquals(5_000 + WarmFullFileManager.NO_SLOT_RETRY_DELAY_MILLIS, declined.retryAt());
        assertTrue("the slot retry is shorter than the failure retry", declined.retryAt() < 5_000 + WarmFullFileManager.RETRY_DELAY_MILLIS);
        assertEquals("only the holder's bytes are reserved", FILE_LENGTH, mgr.reservedBytesPerSegment()[0]);
        assertEquals("no remote read for the declined family", 0, declinedReader.readCount());
        assertFalse("no file for the declined family", Files.exists(dir.resolve("_noslot.faiss")));
        assertNull("no cache entry for the declined family", fileCache.get(dir.resolve("_noslot.faiss")));
        assertEquals("the holder's download has not started reading", 0, holderReader.readCount());
        Map<String, Long> whileHeld = mgr.counters();
        assertEquals(1L, whileHeld.get("declined.NO_SLOT").longValue());
        assertEquals(2L, whileHeld.get("requested").longValue());
        assertEquals("only the holder started", 1L, whileHeld.get("started").longValue());
        assertEquals("the holder still holds the slot", 1, whileHeld.get("activeDownloads").longValue());

        release.countDown();
        awaitState(holder, FullFileState.State.FULL);
        assertEquals(4, holderReader.readCount());
        assertBusy(() -> {
            assertEquals(1L, mgr.counters().get("promoted").longValue());
            assertEquals(0, mgr.counters().get("activeDownloads").longValue());
        }, 10, TimeUnit.SECONDS);

        // The slot is free again once the holder is FULL: at its retry time the declined family promotes.
        clock.set(declined.retryAt());
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
        assertEquals("bytes released", 0L, mgr.reservedBytesPerSegment()[0]);
        assertEquals("no usage left in the cache", 0L, fileCache.usage());

        // The slot was released: a following promotion of another file succeeds with the single slot.
        PositionDependentStreamReader good = new PositionDependentStreamReader();
        TransferManager goodTm = new TransferManager(good, fileCache, threadPool);
        FullFileState ok = newState(goodTm, fileCache, "_after_mid.faiss", FILE_LENGTH);
        startPromotion(mgr, ok);
        awaitState(ok, FullFileState.State.FULL);
        assertEquals(4, good.readCount());
        assertEquals(FILE_LENGTH, mgr.reservedBytesPerSegment()[0]);
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
        Files.write(path, wrongBytes(FILE_LENGTH));
        IndexInput late = localDirectory.openInput(name, IOContext.DEFAULT);
        WarmFullFileManager mgr = newManager(fileCache, threadPool::relativeTimeInMillis, 1, 0.5, () -> {
            fileCache.put(path, new CachedFullFileIndexInput(fileCache, path, late));
            fileCache.decRef(path);
        });

        FullFileState st = newState(tm, fileCache, name, FILE_LENGTH);
        startPromotion(mgr, st);

        assertEquals(FullFileState.State.BLOCK, st.state());
        Map<String, Long> stats = mgr.counters();
        assertEquals("counted as an error decline", 1L, stats.get("declined.ERROR").longValue());
        assertEquals("reserved bytes given back", 0L, stats.get("reservedBytes").longValue());
        assertEquals("slot given back", 0, stats.get("activeDownloads").longValue());
        assertEquals("nothing read from the remote store", 0, reader.readCount());
        fileCache.remove(path);
    }

    public void testFamiliesClosedUnderAScopeAreCountedUntilTheyRelease() throws Exception {
        // A FULL family that closes while a search scope still reads it keeps its mapping until that scope ends. The
        // stats count such families and the bytes they hold, and drop them once the deferred cleanup has run.
        String name = "_awaiting.faiss";
        FileCache fileCache = newFileCache();
        TransferManager tm = new TransferManager(new PositionDependentStreamReader(), fileCache, threadPool);
        WarmFullFileManager mgr = newManager(fileCache, 4, 0.5);
        FullFileState st = newState(tm, fileCache, name, FILE_LENGTH);
        startPromotion(mgr, st);
        awaitState(st, FullFileState.State.FULL);
        assertEquals(0, mgr.counters().get("familiesAwaitingRelease").longValue());
        openQuery(st);

        st.closeFamily();
        Map<String, Long> closed = mgr.counters();
        assertEquals("one family waiting on its scope", 1, closed.get("familiesAwaitingRelease").longValue());
        assertEquals("holding the whole file's bytes", FILE_LENGTH, closed.get("bytesAwaitingRelease").longValue());
        assertFalse(st.cleanupDone());

        closeQuery(st); // the last scope ends: the deferred cleanup runs on the (inline) executor
        assertBusy(() -> assertTrue(st.cleanupDone()), 10, TimeUnit.SECONDS);
        Map<String, Long> released = mgr.counters();
        assertEquals("released once the scope ended", 0, released.get("familiesAwaitingRelease").longValue());
        assertEquals(0L, released.get("bytesAwaitingRelease").longValue());
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
        assertEquals("the family holds no reference", Integer.valueOf(0), fileCache.getRef(path));
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
        assertEquals("the family holds no reference", Integer.valueOf(0), fileCache.getRef(path));
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
        assertSame("the family's entry replaced the placeholder", st.entry(), current);
        fileCache.decRef(path);
        assertEquals("the family holds no reference, nothing leaked", Integer.valueOf(0), fileCache.getRef(path));
        assertEquals(FILE_LENGTH, mgr.reservedBytesPerSegment()[0]);
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
        assertEquals(Integer.valueOf(0), fileCache.getRef(path));
    }

    // ---------- demotion when the setting drops an extension ----------

    /** Promotes {@code name} to FULL through a real download of footered content and returns its family. */
    private FullFileState promoteToFull(WarmFullFileManager mgr, FileCache fileCache, String name, byte[] content, TransferManager tm)
        throws Exception {
        FullFileState st = newStateWithChecksum(tm, fileCache, name, content.length, footerChecksum(content));
        startPromotion(mgr, st);
        awaitState(st, FullFileState.State.FULL);
        return st;
    }

    public void testDemotionWithoutHandlesReleasesMappingFileAndBudget() throws Exception {
        // The setting drops the family's extension while no handle is open: the family goes back to BLOCK at once and
        // the mapping, the cache entry with its file, and the budget are released; with the extension back, the next
        // promotion downloads the file again.
        String name = "_demote.faiss";
        Path path = dir.resolve(name);
        byte[] content = footeredContent(FILE_LENGTH);
        FixedContentStreamReader reader = new FixedContentStreamReader(content);
        FileCache fileCache = newFileCache();
        TransferManager tm = new TransferManager(reader, fileCache, threadPool);
        WarmFullFileManager mgr = newManager(fileCache, 4, 0.5);
        mgr.onExtensionsChanged(Set.of("faiss"));
        FullFileState st = promoteToFull(mgr, fileCache, name, content, tm);
        int parts = partsFor(name, content.length);
        assertEquals(parts, reader.readCount());
        assertEquals(content.length, mgr.reservedBytesPerSegment()[0]);

        mgr.onExtensionsChanged(Set.of());

        assertEquals(FullFileState.State.BLOCK, st.state());
        assertBusy(() -> {
            assertTrue("deferred release ran", st.cleanupDone());
            assertNull("mapping closed", st.localMaster());
            assertEquals("budget released", 0L, mgr.reservedBytesPerSegment()[0]);
            assertNull("whole file dropped from the cache", fileCache.get(path));
            assertFalse("whole file deleted", Files.exists(path));
        }, 30, TimeUnit.SECONDS);
        assertFalse(st.isMappingReleasePending());
        assertEquals(1L, mgr.counters().get("demoted").longValue());

        mgr.onExtensionsChanged(Set.of("faiss"));
        startPromotion(mgr, st);
        awaitState(st, FullFileState.State.FULL);
        assertEquals("downloaded again after the demotion", 2 * parts, reader.readCount());
        assertEquals(content.length, mgr.reservedBytesPerSegment()[0]);
        assertEquals(2L, mgr.counters().get("promoted").longValue());
    }

    public void testReleasingAWholeFileUnmapsItWhileAnotherFileOfTheSegmentStaysOpen() throws Exception {
        // Another file of the same segment is open on the shard's directory, which maps a segment's files as one
        // group that is unmapped only when all of them are closed. The whole file has a mapping of its own, so its
        // release unmaps it: a view cut before the release can no longer be read.
        String name = "_7.faiss";
        byte[] content = footeredContent(FILE_LENGTH);
        FileCache fileCache = newFileCache();
        TransferManager tm = new TransferManager(new FixedContentStreamReader(content), fileCache, threadPool);
        WarmFullFileManager mgr = newManager(fileCache, 4, 0.5);
        Files.write(dir.resolve("_7.veq"), footeredContent(FILE_LENGTH));

        try (IndexInput sameSegment = localDirectory.openInput("_7.veq", IOContext.DEFAULT)) {
            FullFileState st = promoteToFull(mgr, fileCache, name, content, tm);
            IndexInput view = st.localMaster().slice("view", 0, 16);
            assertEquals(content[0], view.readByte());

            mgr.onExtensionsChanged(Set.of());
            assertBusy(() -> assertTrue("release ran", st.cleanupDone()), 30, TimeUnit.SECONDS);

            expectThrows(AlreadyClosedException.class, view::readByte);
            assertEquals("the other file is still readable", footeredContent(FILE_LENGTH)[0], sameSegment.readByte());
        }
    }

    public void testAnExtensionChangeDemotesOnTheExecutorNotTheCallingThread() throws Exception {
        // The setting drops the extension on the thread that applies cluster settings. That call only records the new
        // set, so nothing new is promoted; the FULL family is demoted by the task handed to the executor.
        String name = "_demote_off_thread.faiss";
        byte[] content = footeredContent(FILE_LENGTH);
        FileCache fileCache = newFileCache();
        TransferManager tm = new TransferManager(new FixedContentStreamReader(content), fileCache, threadPool);
        List<Runnable> deferred = new ArrayList<>();
        AtomicBoolean defer = new AtomicBoolean();
        Executor executor = task -> {
            if (defer.get()) {
                deferred.add(task);
            } else {
                task.run();
            }
        };
        WarmFullFileManager mgr = new WarmFullFileManager(
            fileCache,
            new ThreadContext(Settings.EMPTY),
            executor,
            threadPool::relativeTimeInMillis,
            4,
            0.5,
            PART_SIZE
        );
        mgr.onExtensionsChanged(Set.of("faiss"));
        FullFileState st = promoteToFull(mgr, fileCache, name, content, tm);

        defer.set(true);
        mgr.onExtensionsChanged(Set.of());

        assertFalse("the new set applies at once", mgr.isExtensionEligible("faiss"));
        assertEquals("the family is untouched on the calling thread", FullFileState.State.FULL, st.state());
        assertEquals("one task handed to the executor", 1, deferred.size());
        assertEquals(0L, mgr.counters().get("demoted").longValue());

        defer.set(false);
        deferred.get(0).run();

        assertEquals(FullFileState.State.BLOCK, st.state());
        assertEquals(1L, mgr.counters().get("demoted").longValue());
        assertBusy(() -> {
            assertTrue("deferred release ran", st.cleanupDone());
            assertEquals("budget released", 0L, mgr.reservedBytesPerSegment()[0]);
        }, 30, TimeUnit.SECONDS);
    }

    public void testASecondDemotionAfterARepromotionReleasesAgain() throws Exception {
        // Demote, release, promote, demote with a real download each time: the second demotion releases the new
        // mapping, the new whole-file entry and the new budget reservation just as the first did.
        String name = "_demote_twice.faiss";
        Path path = dir.resolve(name);
        byte[] content = footeredContent(FILE_LENGTH);
        FixedContentStreamReader reader = new FixedContentStreamReader(content);
        FileCache fileCache = newFileCache();
        TransferManager tm = new TransferManager(reader, fileCache, threadPool);
        WarmFullFileManager mgr = newManager(fileCache, 4, 0.5);
        mgr.onExtensionsChanged(Set.of("faiss"));
        FullFileState st = promoteToFull(mgr, fileCache, name, content, tm);
        int parts = partsFor(name, content.length);

        mgr.onExtensionsChanged(Set.of());
        assertBusy(() -> assertTrue(st.cleanupDone()), 30, TimeUnit.SECONDS);
        mgr.onExtensionsChanged(Set.of("faiss"));
        startPromotion(mgr, st);
        awaitState(st, FullFileState.State.FULL);
        assertEquals(2 * parts, reader.readCount());
        assertEquals(content.length, mgr.reservedBytesPerSegment()[0]);
        assertFalse("the new period owes nothing yet", st.cleanupDone());

        mgr.onExtensionsChanged(Set.of());

        assertEquals(FullFileState.State.BLOCK, st.state());
        assertBusy(() -> {
            assertTrue("second release ran", st.cleanupDone());
            assertNull("second mapping closed", st.localMaster());
            assertEquals("second budget released", 0L, mgr.reservedBytesPerSegment()[0]);
            assertNull("second whole file dropped from the cache", fileCache.get(path));
            assertFalse("second whole file deleted", Files.exists(path));
        }, 30, TimeUnit.SECONDS);
        assertFalse(st.isMappingReleasePending());
        assertEquals(2L, mgr.counters().get("demoted").longValue());
        assertEquals(0L, mgr.counters().get("familiesAwaitingRelease").longValue());
    }

    public void testDemotionUnderAScopeWaitsForTheScopeToEnd() throws Exception {
        // A search scope reads the file when the extension is dropped: the family is BLOCK at once, but the mapping,
        // the entry and the budget stay until that scope ends, and the family cannot be promoted again in between.
        String name = "_demote_handle.faiss";
        Path path = dir.resolve(name);
        byte[] content = footeredContent(FILE_LENGTH);
        FileCache fileCache = newFileCache();
        TransferManager tm = new TransferManager(new FixedContentStreamReader(content), fileCache, threadPool);
        WarmFullFileManager mgr = newManager(fileCache, 4, 0.5);
        mgr.onExtensionsChanged(Set.of("faiss"));
        FullFileState st = promoteToFull(mgr, fileCache, name, content, tm);
        openQuery(st);

        mgr.onExtensionsChanged(Set.of());

        assertEquals(FullFileState.State.BLOCK, st.state());
        assertTrue("release waits for the scope", st.isMappingReleasePending());
        assertNotNull("mapping kept under the scope", st.localMaster());
        assertEquals("budget still held", content.length, mgr.reservedBytesPerSegment()[0]);
        assertNotNull(fileCache.get(path));
        fileCache.decRef(path);
        assertFalse(st.cleanupDone());

        closeQuery(st);
        assertBusy(() -> {
            assertTrue(st.cleanupDone());
            assertNull(st.localMaster());
            assertEquals(0L, mgr.reservedBytesPerSegment()[0]);
            assertNull(fileCache.get(path));
        }, 30, TimeUnit.SECONDS);
        assertFalse(st.isMappingReleasePending());
    }

    public void testExtensionRemovedDuringTheDownloadDemotesAtPublish() throws Exception {
        // The setting drops the extension while the download is still running: the family is not FULL at that
        // moment, so the settings change cannot demote it; the publish does, as soon as the family reaches FULL.
        String name = "_late_demote.faiss";
        Path path = dir.resolve(name);
        byte[] content = footeredContent(FILE_LENGTH);
        CountDownLatch release = new CountDownLatch(1);
        FileCache fileCache = newFileCache();
        TransferManager tm = new TransferManager(blockingReader(release, new FixedContentStreamReader(content)), fileCache, threadPool);
        WarmFullFileManager mgr = newManager(fileCache, 4, 0.5);
        mgr.onExtensionsChanged(Set.of("faiss"));
        FullFileState st = newStateWithChecksum(tm, fileCache, name, content.length, footerChecksum(content));
        startPromotion(mgr, st); // the reader blocks on the latch

        mgr.onExtensionsChanged(Set.of());
        assertEquals("still downloading", FullFileState.State.PROMOTING, st.state());
        release.countDown();

        assertBusy(() -> {
            assertEquals(FullFileState.State.BLOCK, st.state());
            assertTrue(st.cleanupDone());
            assertNull(fileCache.get(path));
            assertEquals(0L, mgr.reservedBytesPerSegment()[0]);
        }, 30, TimeUnit.SECONDS);
        assertEquals(1L, mgr.counters().get("promoted").longValue());
        assertEquals(1L, mgr.counters().get("demoted").longValue());
    }

    public void testSettingsChangeThatKeepsTheExtensionDuringTheDownloadLeavesTheFamilyFull() throws Exception {
        // The setting changes while the download runs but still lists the family's extension: the publish finds the
        // family eligible and it stays FULL.
        String name = "_kept.faiss";
        byte[] content = footeredContent(FILE_LENGTH);
        CountDownLatch release = new CountDownLatch(1);
        FileCache fileCache = newFileCache();
        TransferManager tm = new TransferManager(blockingReader(release, new FixedContentStreamReader(content)), fileCache, threadPool);
        WarmFullFileManager mgr = newManager(fileCache, 4, 0.5);
        FullFileState st = newStateWithChecksum(tm, fileCache, name, content.length, footerChecksum(content));
        startPromotion(mgr, st);

        mgr.onExtensionsChanged(Set.of("faiss", "veq"));
        release.countDown();

        awaitState(st, FullFileState.State.FULL);
        assertNotNull(st.localMaster());
        assertEquals(0L, mgr.counters().get("demoted").longValue());
        assertEquals(content.length, mgr.reservedBytesPerSegment()[0]);
    }

    public void testReAddingTheExtensionWhileAScopeReadsDoesNotBringTheFileBack() throws Exception {
        // A search scope keeps reading, so the demoted family still holds its mapping. Listing the extension again
        // changes nothing for it: it serves blocks and cannot be promoted until the scope ends and the release has
        // run. The next promotion after that downloads the file again.
        String name = "_relisted.faiss";
        Path path = dir.resolve(name);
        byte[] content = footeredContent(FILE_LENGTH);
        FixedContentStreamReader reader = new FixedContentStreamReader(content);
        FileCache fileCache = newFileCache();
        TransferManager tm = new TransferManager(reader, fileCache, threadPool);
        WarmFullFileManager mgr = newManager(fileCache, 4, 0.5);
        FullFileState st = promoteToFull(mgr, fileCache, name, content, tm);
        int parts = partsFor(name, content.length);
        IndexInput master = st.localMaster();
        openQuery(st);

        mgr.onExtensionsChanged(Set.of());
        assertEquals(FullFileState.State.BLOCK, st.state());
        assertTrue(st.isMappingReleasePending());

        mgr.onExtensionsChanged(Set.of("faiss"));

        assertEquals("still on blocks", FullFileState.State.BLOCK, st.state());
        assertTrue("the release is still owed", st.isMappingReleasePending());
        assertSame("the scope keeps its mapping", master, st.localMaster());
        assertFalse("no promotion while the release is pending", st.onAccess(mgr.relativeTimeInMillis()));
        assertEquals(1L, mgr.counters().get("demoted").longValue());
        assertEquals(1L, mgr.counters().get("promoted").longValue());
        assertEquals("no remote read", parts, reader.readCount());

        closeQuery(st);
        assertBusy(() -> {
            assertTrue(st.cleanupDone());
            assertNull(st.localMaster());
            assertNull(fileCache.get(path));
            assertEquals(0L, mgr.reservedBytesPerSegment()[0]);
        }, 30, TimeUnit.SECONDS);

        startPromotion(mgr, st);
        awaitState(st, FullFileState.State.FULL);
        assertEquals("downloaded again", 2 * parts, reader.readCount());
        assertEquals(2L, mgr.counters().get("promoted").longValue());
    }

    public void testReAddingTheExtensionAfterTheReleasePromotesAgain() throws Exception {
        // The demoted family's release already ran (no handle was open). Listing the extension again does nothing by
        // itself, and the next promotion downloads the file again.
        String name = "_relisted_late.faiss";
        byte[] content = footeredContent(FILE_LENGTH);
        FixedContentStreamReader reader = new FixedContentStreamReader(content);
        FileCache fileCache = newFileCache();
        TransferManager tm = new TransferManager(reader, fileCache, threadPool);
        WarmFullFileManager mgr = newManager(fileCache, 4, 0.5);
        FullFileState st = promoteToFull(mgr, fileCache, name, content, tm);
        int parts = partsFor(name, content.length);

        mgr.onExtensionsChanged(Set.of());
        assertBusy(() -> assertTrue(st.cleanupDone()), 30, TimeUnit.SECONDS);
        mgr.onExtensionsChanged(Set.of("faiss"));

        assertEquals(FullFileState.State.BLOCK, st.state());
        assertFalse(st.isMappingReleasePending());
        startPromotion(mgr, st);
        awaitState(st, FullFileState.State.FULL);
        assertEquals("downloaded again", 2 * parts, reader.readCount());
    }

    public void testOnlyFamiliesOfRemovedExtensionsAreDemoted() throws Exception {
        // Two FULL families of different extensions; the setting keeps one: only the other is demoted.
        byte[] content = footeredContent(FILE_LENGTH);
        FileCache fileCache = newFileCache();
        TransferManager tm = new TransferManager(new FixedContentStreamReader(content), fileCache, threadPool);
        WarmFullFileManager mgr = newManager(fileCache, 4, 0.5);
        mgr.onExtensionsChanged(Set.of("faiss", "veq"));
        FullFileState graph = promoteToFull(mgr, fileCache, "_7.faiss", content, tm);
        FullFileState codes = promoteToFull(mgr, fileCache, "_7.veq", content, tm);

        mgr.onExtensionsChanged(Set.of("veq"));

        assertEquals(FullFileState.State.BLOCK, graph.state());
        assertEquals(FullFileState.State.FULL, codes.state());
        assertEquals(1L, mgr.counters().get("demoted").longValue());
        assertBusy(() -> assertTrue(graph.cleanupDone()), 30, TimeUnit.SECONDS);
        assertNotNull("the kept family still serves its mapping", codes.localMaster());
        assertEquals("the kept family's bytes stay reserved", content.length, mgr.reservedBytesPerSegment()[0]);
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
        assertServedUnreferenced(st, fileCache, path, content);
        assertNotSame("the placeholder was replaced by a live entry", restored, fileCache.get(path));
        fileCache.decRef(path);
        Map<String, Long> stats = mgr.counters();
        assertEquals("counted as reused", 1L, stats.get("reused").longValue());
        assertEquals("no download started", 0L, stats.get("started").longValue());
        assertEquals("no bytes downloaded", 0L, stats.get("downloadedBytes").longValue());
        assertEquals(1L, stats.get("promoted").longValue());
    }

    public void testRestoredLeftoverSurvivesAnEvictionDuringItsReuse() throws Exception {
        // The restore placeholder is unreferenced in the cache. While the manager validates the file and until
        // fetchBlobAsync has replaced the placeholder, an eviction must not take it, or the listener deletes the file
        // under the manager and the download that follows is counted as a reuse. The manager holds a reference across
        // that window; here the cache overflows inside it, from another entry, and the file is still reused.
        String name = "_reuse_evicted.faiss";
        Path path = dir.resolve(name);
        byte[] content = footeredContent(FILE_LENGTH);
        FixedContentStreamReader reader = new FixedContentStreamReader(content);
        FileCache fileCache = FileCacheFactory.createConcurrentLRUFileCache(content.length + 64, 1);
        TransferManager tm = new TransferManager(reader, fileCache, threadPool);
        Files.write(path, content);
        putRestoredPlaceholder(fileCache, path, content.length);
        Path other = dir.resolve("_other.faiss");
        WarmFullFileManager mgr = newManager(fileCache, threadPool::relativeTimeInMillis, 4, 1.0, () -> {
            // Runs after validation, before fetchBlobAsync: an entry that overflows the cache, dropped at once, so the
            // evictor runs over every unreferenced entry.
            fileCache.put(other, new FileCache.RestoredCachedIndexInput(content.length));
            fileCache.decRef(other);
            assertTrue("the whole file is still on disk", Files.exists(path));
            assertNull("the overflowing entry, not the placeholder, was evicted", fileCache.get(other));
        });

        FullFileState st = newStateWithChecksum(tm, fileCache, name, content.length, footerChecksum(content));
        startPromotion(mgr, st);
        awaitState(st, FullFileState.State.FULL);

        assertEquals("nothing read from the remote store", 0, reader.readCount());
        assertServedUnreferenced(st, fileCache, path, content);
        Map<String, Long> stats = mgr.counters();
        assertEquals("counted as reused", 1L, stats.get("reused").longValue());
        assertEquals("no download started", 0L, stats.get("started").longValue());
        assertEquals("no bytes downloaded", 0L, stats.get("downloadedBytes").longValue());
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
        assertServedUnreferenced(st, fileCache, path, content);
        Map<String, Long> stats = mgr.counters();
        assertEquals("counted as reused", 1L, stats.get("reused").longValue());
        assertEquals("no download started", 0L, stats.get("started").longValue());
        assertEquals("no bytes downloaded", 0L, stats.get("downloadedBytes").longValue());
        assertEquals(1L, stats.get("promoted").longValue());
    }

    public void testAFamilyThatClosesReleasesItsWholeFileAndALaterFamilyDownloadsAgain() throws Exception {
        // The family of a promoted file closes (the index is closed): its whole file is released like an evicted or
        // demoted one, so nothing of it is left in the cache or on disk. A new family for the same file (the index
        // reopened) downloads the file again.
        String name = "_reopen.faiss";
        Path path = dir.resolve(name);
        byte[] content = footeredContent(FILE_LENGTH);
        FixedContentStreamReader reader = new FixedContentStreamReader(content);
        FileCache fileCache = newFileCache();
        TransferManager tm = new TransferManager(reader, fileCache, threadPool);
        WarmFullFileManager mgr = newManager(fileCache, 4, 0.5);
        FullFileState first = promoteToFull(mgr, fileCache, name, content, tm);
        int parts = partsFor(name, content.length);
        assertEquals(parts, reader.readCount());

        first.closeFamily();
        assertBusy(() -> assertTrue(first.cleanupDone()), 30, TimeUnit.SECONDS);
        assertNull("the whole-file entry went with the family", fileCache.get(path));
        assertFalse(Files.exists(path));
        assertEquals(List.of(), retiredFiles());
        assertEquals(0L, fileCache.usage());
        assertEquals(0L, mgr.reservedBytesPerSegment()[0]);

        FullFileState second = newStateWithChecksum(tm, fileCache, name, content.length, footerChecksum(content));
        startPromotion(mgr, second);
        awaitState(second, FullFileState.State.FULL);

        assertEquals("downloaded again", 2 * parts, reader.readCount());
        assertServedUnreferenced(second, fileCache, path, content);
        Map<String, Long> stats = mgr.counters();
        assertEquals(0L, stats.get("reused").longValue());
        assertEquals(2L, stats.get("started").longValue());
        assertEquals(2L, stats.get("promoted").longValue());
        assertEquals(content.length, mgr.reservedBytesPerSegment()[0]);
    }

    public void testANewFamilyPromotesWhileAnEarlierFamilysReleaseWaitsForItsScope() throws Exception {
        // A family closes while a scope still reads its whole file, so its release waits and its entry is still in the
        // cache. A new family for the same file drops that entry, which moves the earlier file aside, and downloads
        // its own. When the scope ends, the earlier family's release deletes its own file and leaves the new family's
        // entry and file alone.
        String name = "_overlap.faiss";
        Path path = dir.resolve(name);
        byte[] content = footeredContent(FILE_LENGTH);
        FixedContentStreamReader reader = new FixedContentStreamReader(content);
        FileCache fileCache = newFileCache();
        TransferManager tm = new TransferManager(reader, fileCache, threadPool);
        WarmFullFileManager mgr = newManager(fileCache, 4, 0.5);
        FullFileState first = promoteToFull(mgr, fileCache, name, content, tm);
        int parts = partsFor(name, content.length);
        openQuery(first);
        IndexInput firstMapping = first.localMaster();

        first.closeFamily();
        assertFalse("the release waits for the scope", first.cleanupDone());

        FullFileState second = newStateWithChecksum(tm, fileCache, name, content.length, footerChecksum(content));
        startPromotion(mgr, second);
        awaitState(second, FullFileState.State.FULL);

        assertEquals("downloaded again", 2 * parts, reader.readCount());
        assertEquals("the earlier file was moved aside", 1, retiredFiles().size());
        assertArrayEquals("and is still read by its scope", content, readAll(firstMapping, content.length));
        assertEquals("both files are on disk and counted", 2L * content.length, fileCache.usage());
        assertEquals("and both hold budget", 2L * content.length, mgr.reservedBytesPerSegment()[0]);

        closeQuery(first);
        assertBusy(() -> assertTrue(first.cleanupDone()), 30, TimeUnit.SECONDS);

        assertEquals("the earlier file is deleted", List.of(), retiredFiles());
        assertEquals(FullFileState.State.FULL, second.state());
        assertTrue("the new family's file is untouched", Files.exists(path));
        assertServedUnreferenced(second, fileCache, path, content);
        assertEquals((long) content.length, fileCache.usage());
        assertEquals(content.length, mgr.reservedBytesPerSegment()[0]);
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
        assertServedUnreferenced(st, fileCache, path, content);
        Map<String, Long> stats = mgr.counters();
        assertEquals("not reused", 0L, stats.get("reused").longValue());
        assertEquals("one download started", 1L, stats.get("started").longValue());
        assertEquals(content.length, stats.get("downloadedBytes").longValue());
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
        assertServedUnreferenced(st, fileCache, path, content);
        Map<String, Long> stats = mgr.counters();
        assertEquals("not reused", 0L, stats.get("reused").longValue());
        assertEquals("one download started", 1L, stats.get("started").longValue());
        assertEquals(content.length, stats.get("downloadedBytes").longValue());
    }

    public void testDownloadIsRenamedIntoPlaceAndLeavesNoTempFile() throws Exception {
        // The promotion downloads to a temp file of its own and renames the complete file into place, so afterwards
        // only the whole file exists and the cache holds nothing but it.
        String name = "_renamed.faiss";
        Path path = dir.resolve(name);
        byte[] content = footeredContent(FILE_LENGTH);
        FixedContentStreamReader reader = new FixedContentStreamReader(content);
        FileCache fileCache = newFileCache();
        TransferManager tm = new TransferManager(reader, fileCache, threadPool);
        WarmFullFileManager mgr = newManager(fileCache, 4, 0.5);

        FullFileState st = newStateWithChecksum(tm, fileCache, name, content.length, footerChecksum(content));
        startPromotion(mgr, st);
        awaitState(st, FullFileState.State.FULL);

        assertEquals("one download, one read per part", partsFor(name, content.length), reader.readCount());
        assertTrue(Files.exists(path));
        try (Stream<Path> files = Files.list(dir)) {
            assertEquals(
                "no temp file is left behind",
                List.of(),
                files.map(file -> file.getFileName().toString()).filter(FileTypeUtils::isPartialDownloadFile).toList()
            );
        }
        assertServedUnreferenced(st, fileCache, path, content);
        assertEquals("the cache holds the whole file and nothing else", content.length, fileCache.usage());
        assertEquals(0L, mgr.counters().get("reused").longValue());
        assertEquals(1L, mgr.counters().get("started").longValue());
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
        Map<String, Long> stats = mgr.counters();
        assertEquals(1L, stats.get("declined.CHECKSUM_MISMATCH").longValue());
        assertEquals(1L, stats.get("failed").longValue());
        assertEquals(1L, stats.get("started").longValue());
        assertEquals(0L, stats.get("promoted").longValue());
        assertEquals("budget released", 0L, mgr.reservedBytesPerSegment()[0]);
        assertEquals("slot released", 0, stats.get("activeDownloads").longValue());
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
        assertTrue(st.onAccess(mgr.relativeTimeInMillis()));

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
            assertEquals("one file's worth of budget", FILE_LENGTH, mgr.reservedBytesPerSegment()[0]);
            assertEquals("the family holds no reference", Integer.valueOf(0), fileCache.getRef(path));
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
        assertEquals("budget: the raced family plus 4 in flight", FILE_LENGTH * (callers + 1), mgr.reservedBytesPerSegment()[0]);
        release.countDown();
        for (FullFileState s : later) {
            awaitState(s, FullFileState.State.FULL);
        }
        assertEquals(4 * callers, laterReader.readCount());
    }

    // ---------- parts copied from local blocks ----------

    /** Registers an idle, open entry of {@code bytes} at the file's block {@code blockId}, with or without the block file. */
    private Path putLocalBlock(FileCache fileCache, String name, int blockId, byte[] bytes, boolean withFile) throws IOException {
        Path blockPath = dir.resolve(name + "_block_" + blockId);
        if (withFile) {
            Files.write(blockPath, bytes);
        }
        fileCache.put(blockPath, new CachedFullFileIndexInput(fileCache, blockPath, new ByteArrayIndexInput(blockPath.toString(), bytes)));
        fileCache.decRef(blockPath); // idle, as a block nobody reads
        return blockPath;
    }

    private static byte[] slice(byte[] content, int from, int to) {
        return java.util.Arrays.copyOfRange(content, from, to);
    }

    public void testPartsHeldAsLocalBlocksAreCopiedInsteadOfRead() throws Exception {
        // Parts 0, 2 and the short last part 4 are in the cache as blocks; parts 1 and 3 are not. The download copies
        // the three from disk and reads only the two from the remote store, and the file is the same bytes.
        String name = "_blocks.faiss";
        byte[] content = footeredContent(FILE_LENGTH); // 4 full parts and a short one
        FixedContentStreamReader reader = new FixedContentStreamReader(content);
        FileCache fileCache = newFileCache();
        TransferManager tm = new TransferManager(reader, fileCache, threadPool);
        WarmFullFileManager mgr = newManager(fileCache, 4, 0.5);
        int part = (int) PART_SIZE;
        Path block0 = putLocalBlock(fileCache, name, 0, slice(content, 0, part), true);
        Path block2 = putLocalBlock(fileCache, name, 2, slice(content, 2 * part, 3 * part), true);
        Path block4 = putLocalBlock(fileCache, name, 4, slice(content, 4 * part, content.length), true);

        FullFileState st = newStateWithChecksum(tm, fileCache, name, content.length, footerChecksum(content));
        startPromotion(mgr, st);
        awaitState(st, FullFileState.State.FULL);

        assertEquals("only the two parts without a block are read", 2, reader.readCount());
        assertServedUnreferenced(st, fileCache, dir.resolve(name), content);
        Map<String, Long> stats = mgr.counters();
        assertEquals(3L, stats.get("partsFromLocalBlocks").longValue());
        long copied = 2L * part + (content.length - 4L * part);
        assertEquals(copied, stats.get("bytesFromLocalBlocks").longValue());
        assertEquals("only the read bytes count as downloaded", content.length - copied, stats.get("downloadedBytes").longValue());
        for (Path block : List.of(block0, block2, block4)) {
            assertNotNull("copied, and left for the cache to evict when it needs the room", fileCache.get(block));
            fileCache.decRef(block);
            assertTrue(Files.exists(block));
        }
    }

    public void testABlockOfTheWrongLengthOrWithoutItsFileIsReadRemotely() throws Exception {
        String name = "_badblocks.faiss";
        byte[] content = footeredContent(FILE_LENGTH);
        FixedContentStreamReader reader = new FixedContentStreamReader(content);
        FileCache fileCache = newFileCache();
        TransferManager tm = new TransferManager(reader, fileCache, threadPool);
        WarmFullFileManager mgr = newManager(fileCache, 4, 0.5);
        int part = (int) PART_SIZE;
        // Block 1: an entry whose length is not the part's. Block 3: an entry with no file behind it.
        Path block1 = putLocalBlock(fileCache, name, 1, slice(content, part, part + 500), true);
        Path block3 = putLocalBlock(fileCache, name, 3, slice(content, 3 * part, 4 * part), false);

        FullFileState st = newStateWithChecksum(tm, fileCache, name, content.length, footerChecksum(content));
        startPromotion(mgr, st);
        awaitState(st, FullFileState.State.FULL);

        assertEquals("every part read", partsFor(name, content.length), reader.readCount());
        assertEquals(0L, mgr.counters().get("partsFromLocalBlocks").longValue());
        assertEquals((long) content.length, mgr.counters().get("downloadedBytes").longValue());
        assertServedUnreferenced(st, fileCache, dir.resolve(name), content);
        assertNotNull("not used, and left to the cache", fileCache.get(block1));
        fileCache.decRef(block1);
        assertNotNull("not used, and left to the cache", fileCache.get(block3));
        fileCache.decRef(block3);
    }

    public void testLocalBlocksAreNotUsedWhenTheRemoteChecksumIsUnknown() throws Exception {
        // The remote store recorded no checksum for the file, so a file assembled from blocks could not be checked.
        // Every part is read from the remote store, and a block holding the wrong bytes does no harm.
        String name = "_nochecksum.faiss";
        byte[] content = footeredContent(FILE_LENGTH);
        FixedContentStreamReader reader = new FixedContentStreamReader(content);
        FileCache fileCache = newFileCache();
        TransferManager tm = new TransferManager(reader, fileCache, threadPool);
        WarmFullFileManager mgr = newManager(fileCache, 4, 0.5);
        int part = (int) PART_SIZE;
        putLocalBlock(fileCache, name, 0, wrongBytes(part), true);
        putLocalBlock(fileCache, name, 2, slice(content, 2 * part, 3 * part), true);

        FullFileState st = newState(tm, fileCache, name, content.length);
        st.markRemoteMode(null);
        startPromotion(mgr, st);
        awaitState(st, FullFileState.State.FULL);

        assertEquals("every part read remotely", partsFor(name, content.length), reader.readCount());
        Map<String, Long> stats = mgr.counters();
        assertEquals(0L, stats.get("partsFromLocalBlocks").longValue());
        assertEquals(0L, stats.get("bytesFromLocalBlocks").longValue());
        assertEquals((long) content.length, stats.get("downloadedBytes").longValue());
        assertServedUnreferenced(st, fileCache, dir.resolve(name), content);
    }

    public void testACorruptLocalBlockFailsTheChecksumAndTheRetryReadsEveryPartRemotely() throws Exception {
        // Block 0 holds the wrong bytes. The assembled file fails the footer checksum, the family remembers that its
        // local blocks are suspect, and the retry reads every part from the remote store and reaches FULL.
        String name = "_corrupt.faiss";
        byte[] content = footeredContent(FILE_LENGTH);
        FixedContentStreamReader reader = new FixedContentStreamReader(content);
        FileCache fileCache = newFileCache();
        TransferManager tm = new TransferManager(reader, fileCache, threadPool);
        AtomicLong clock = new AtomicLong(1_000);
        WarmFullFileManager mgr = newManager(fileCache, clock::get, 4, 0.5);
        int part = (int) PART_SIZE;
        Path block0 = putLocalBlock(fileCache, name, 0, wrongBytes(part), true);
        int parts = partsFor(name, content.length);

        FullFileState st = newStateWithChecksum(tm, fileCache, name, content.length, footerChecksum(content));
        startPromotion(mgr, st);
        awaitState(st, FullFileState.State.BLOCK);
        assertEquals(1L, mgr.counters().get("declined.CHECKSUM_MISMATCH").longValue());
        assertEquals("the other parts were read", parts - 1, reader.readCount());
        assertTrue(st.localBlocksSuspect());
        assertNull("the bad whole file is gone", fileCache.get(dir.resolve(name)));
        assertFalse(Files.exists(dir.resolve(name)));

        clock.addAndGet(WarmFullFileManager.RETRY_DELAY_MILLIS + 1);
        startPromotion(mgr, st);
        awaitState(st, FullFileState.State.FULL);
        assertEquals("the retry reads every part", 2 * parts - 1, reader.readCount());
        assertServedUnreferenced(st, fileCache, dir.resolve(name), content);
        assertNotNull("the corrupt block is left to the cache; the family no longer reads it", fileCache.get(block0));
        fileCache.decRef(block0);
    }

    // ---------- the cache owns the whole file ----------

    /** A family with the file's remote checksum known. */
    private FullFileState newStateFor(TransferManager tm, FileCache fileCache, String name, byte[] content) throws IOException {
        FullFileState st = newState(tm, fileCache, name, content.length);
        st.markRemoteMode(footerChecksum(content));
        return st;
    }

    /** A one-segment manager with room for one footered {@link #FILE_LENGTH} whole file and not two. */
    private WarmFullFileManager newTightBudgetManager(FileCache fileCache, LongSupplier clock) {
        return newManager(fileCache, clock, 4, 0.0005);
    }

    public void testAnEvictedFileIsNotPromotedAgainBeforeItsBackoffAndWaitsLongerEachTime() throws Exception {
        // The cache evicts a whole file: the family cannot be promoted again until the backoff has passed. Evicted
        // again soon after, it waits twice as long.
        String name = "_backoff.faiss";
        Path path = dir.resolve(name);
        byte[] content = footeredContent(FILE_LENGTH);
        FileCache fileCache = newFileCache();
        TransferManager tm = new TransferManager(new FixedContentStreamReader(content), fileCache, threadPool);
        AtomicLong clock = new AtomicLong(1_000_000);
        WarmFullFileManager mgr = newManager(fileCache, clock::get, 4, 0.5);
        mgr.onEvictionBackoffChanged(TimeValue.timeValueSeconds(30));
        FullFileState st = newStateFor(tm, fileCache, name, content);
        startPromotion(mgr, st);
        awaitState(st, FullFileState.State.FULL);

        fileCache.remove(path);
        assertBusy(() -> assertTrue(st.cleanupDone()), 10, TimeUnit.SECONDS);
        assertEquals(clock.get() + 30_000, st.retryAt());
        assertFalse("not promoted again right after the eviction", st.onAccess(clock.get()));
        clock.addAndGet(29_999);
        assertFalse(st.onAccess(clock.get()));

        clock.addAndGet(1);
        startPromotion(mgr, st);
        awaitState(st, FullFileState.State.FULL);
        assertEquals(2L, mgr.counters().get("promoted").longValue());

        fileCache.remove(path);
        assertBusy(() -> assertTrue(st.cleanupDone()), 10, TimeUnit.SECONDS);
        assertEquals("the second eviction in a row waits twice as long", clock.get() + 60_000, st.retryAt());
        assertEquals(2L, mgr.counters().get("evicted").longValue());
    }

    public void testAPromotedFileIsAnUnreferencedEntryThatCachePressureEvicts() throws Exception {
        // A FULL family holds no reference on its whole file, so under capacity pressure the cache evicts the idle
        // file like a block: the family is demoted, its budget goes back, and with no scope reading it the mapping is
        // released at once. The next access promotes the file again.
        String name = "_lru.faiss";
        Path path = dir.resolve(name);
        byte[] content = footeredContent(FILE_LENGTH);
        FixedContentStreamReader reader = new FixedContentStreamReader(content);
        // Room for the whole file and one block, not two.
        FileCache fileCache = FileCacheFactory.createConcurrentLRUFileCache(content.length + PART_SIZE, 1);
        TransferManager tm = new TransferManager(reader, fileCache, threadPool);
        WarmFullFileManager mgr = newManager(fileCache, 4, 1.0);
        FullFileState st = newStateFor(tm, fileCache, name, content);
        startPromotion(mgr, st);
        awaitState(st, FullFileState.State.FULL);
        int parts = partsFor(name, content.length);
        assertServedUnreferenced(st, fileCache, path, content);
        assertEquals(content.length, mgr.reservedBytesPerSegment()[0]);

        // Two blocks of another file take the cache over capacity; the idle whole file is its least recently used entry.
        putLocalBlock(fileCache, "_other.faiss", 0, slice(content, 0, (int) PART_SIZE), false);
        putLocalBlock(fileCache, "_other.faiss", 1, slice(content, 0, (int) PART_SIZE), false);

        assertEquals(FullFileState.State.BLOCK, st.state());
        assertNull("the cache dropped the whole file", fileCache.get(path));
        assertFalse(Files.exists(path));
        Map<String, Long> stats = mgr.counters();
        assertEquals(1L, stats.get("evicted").longValue());
        assertEquals((long) content.length, stats.get("evictedBytes").longValue());
        assertEquals("budget back", 0L, stats.get("reservedBytes").longValue());
        assertBusy(() -> assertTrue("released at once: no scope was reading", st.cleanupDone()), 10, TimeUnit.SECONDS);
        assertNull(st.localMaster());
        assertFalse(st.isMappingReleasePending());

        startPromotion(mgr, st);
        awaitState(st, FullFileState.State.FULL);
        assertEquals("downloaded again", 2 * parts, reader.readCount());
        assertEquals(2L, mgr.counters().get("promoted").longValue());
        assertServedUnreferenced(st, fileCache, path, content);
    }

    public void testPruneDropsAWholeFileUnderAQueryAndTheQueryKeepsItsMapping() throws Exception {
        // _cache/clear prunes the unreferenced entries under a shard. A whole file is never referenced, so it is
        // dropped even while a query phase is open on its shard: the family goes back to blocks at once, and the
        // mapping the query may be reading stays until that phase has ended.
        String name = "_prune.faiss";
        Path path = dir.resolve(name);
        byte[] content = footeredContent(FILE_LENGTH);
        FileCache fileCache = newFileCache();
        TransferManager tm = new TransferManager(new FixedContentStreamReader(content), fileCache, threadPool);
        WarmFullFileManager mgr = newManager(fileCache, 4, 0.5);
        FullFileState st = newStateFor(tm, fileCache, name, content);
        startPromotion(mgr, st);
        awaitState(st, FullFileState.State.FULL);
        openQuery(st);
        IndexInput mapping = st.localMaster();

        fileCache.prune();

        assertEquals("dropped under the query", FullFileState.State.BLOCK, st.state());
        assertNull(fileCache.get(path));
        assertEquals(1L, mgr.counters().get("evicted").longValue());
        assertFalse("the release waits for the query", st.cleanupDone());
        assertArrayEquals("the query's mapping still reads", content, readAll(mapping, content.length));
        assertEquals("the budget is held until the release", (long) content.length, mgr.reservedBytesPerSegment()[0]);

        closeQuery(st);
        assertBusy(() -> assertTrue(st.cleanupDone()), 10, TimeUnit.SECONDS);
        assertNull(st.localMaster());
        assertFalse(Files.exists(path));
        assertEquals(List.of(), retiredFiles());
        assertEquals(0L, mgr.reservedBytesPerSegment()[0]);
    }

    public void testAnExplicitRemovalUnderAScopeDemotesAtOnceAndUnmapsWhenTheScopeEnds() throws Exception {
        // An explicit remove ignores references: the family is demoted at once, but the mapping stays readable until
        // the scope ends. Until then the file, moved aside, still occupies the disk, so the cache keeps counting its
        // bytes and the budget stays held; the release closes the mapping, deletes the file and gives both back.
        String name = "_remove.faiss";
        Path path = dir.resolve(name);
        byte[] content = footeredContent(FILE_LENGTH);
        FileCache fileCache = newFileCache();
        TransferManager tm = new TransferManager(new FixedContentStreamReader(content), fileCache, threadPool);
        WarmFullFileManager mgr = newManager(fileCache, 4, 0.5);
        FullFileState st = newStateFor(tm, fileCache, name, content);
        startPromotion(mgr, st);
        awaitState(st, FullFileState.State.FULL);
        openQuery(st);

        fileCache.remove(path);

        assertEquals("demoted at once: new reads go to blocks", FullFileState.State.BLOCK, st.state());
        Map<String, Long> stats = mgr.counters();
        assertEquals(1L, stats.get("evicted").longValue());
        assertEquals("the budget is held", (long) content.length, stats.get("reservedBytes").longValue());
        assertNull("the entry is gone", fileCache.get(path));
        assertEquals("the cache still counts the file", (long) content.length, fileCache.usage());
        assertEquals("the file was moved aside", 1, retiredFiles().size());
        assertEquals("the scope keeps the mapping", 1L, stats.get("familiesAwaitingRelease").longValue());
        assertEquals((long) content.length, stats.get("bytesAwaitingRelease").longValue());
        assertFalse(st.cleanupDone());
        assertFalse("the file's own name is free", Files.exists(path));
        assertArrayEquals("the mapping still reads under the scope", content, readAll(st.localMaster(), content.length));

        closeQuery(st);
        assertBusy(() -> assertTrue(st.cleanupDone()), 10, TimeUnit.SECONDS);
        assertNull(st.localMaster());
        assertEquals(0L, mgr.counters().get("familiesAwaitingRelease").longValue());
        assertFalse(st.isMappingReleasePending());
        assertEquals("the file is deleted", List.of(), retiredFiles());
        assertEquals("the cache counts it no longer", 0L, fileCache.usage());
        assertEquals("the budget is back", 0L, mgr.counters().get("reservedBytes").longValue());
    }

    /** The whole files the cache moved aside for their release, in the test directory. */
    private List<String> retiredFiles() throws IOException {
        try (Stream<Path> files = Files.list(dir)) {
            return files.map(file -> file.getFileName().toString()).filter(FileTypeUtils::isRetiredFile).sorted().toList();
        }
    }

    public void testAFailureRightAfterPublishingGivesBackTheSlotAndTheCacheReference() throws Exception {
        // The cache throws while the entry of the family that just turned FULL is put in it, as it does when an
        // eviction it triggers cannot delete a file. The family stays FULL and serves; the attempt's download slot
        // and its reference on the entry are given back, so later promotions are not shut out and the file can be
        // evicted.
        String name = "_put_fails.faiss";
        Path path = dir.resolve(name);
        byte[] content = footeredContent(FILE_LENGTH);
        FileCache fileCache = spy(newFileCache());
        TransferManager tm = new TransferManager(new FixedContentStreamReader(content), fileCache, threadPool);
        WarmFullFileManager mgr = newManager(fileCache, 1, 0.5);
        FullFileState st = newStateFor(tm, fileCache, name, content);
        doAnswer(invocation -> {
            Object result = invocation.callRealMethod();
            if (invocation.getArgument(1) instanceof PromotedWholeFileEntry) {
                throw new IllegalStateException("simulated failure of an eviction listener");
            }
            return result;
        }).when(fileCache).put(any(), any());

        startPromotion(mgr, st);
        awaitState(st, FullFileState.State.FULL);

        assertEquals("the one slot is free again", 0L, mgr.counters().get("activeDownloads").longValue());
        assertArrayEquals("the family serves", content, readAll(st.localMaster(), content.length));
        assertBusy(
            () -> assertEquals("no reference is left on the entry", Integer.valueOf(0), fileCache.getRef(path)),
            10,
            TimeUnit.SECONDS
        );

        // The slot is usable: another file is promoted through it.
        FullFileState next = newStateFor(tm, fileCache, "_next.faiss", content);
        startPromotion(mgr, next);
        assertBusy(() -> assertEquals(FullFileState.State.FULL, next.state()), 30, TimeUnit.SECONDS);
        assertEquals(0L, mgr.counters().get("declined.NO_SLOT").longValue());
    }

    public void testAFamilyDemotedBeforeItsEntryIsCachedLeavesNothingBehind() throws Exception {
        // The extension is taken off the list in the instant between the family turning FULL and its entry being put
        // in the cache. The release runs first and finds no entry; the promotion then drops the entry it puts, so no
        // file, no cache entry and no counted bytes are left for a release that has already run.
        String name = "_orphan.faiss";
        Path path = dir.resolve(name);
        byte[] content = footeredContent(FILE_LENGTH);
        FileCache fileCache = spy(newFileCache());
        TransferManager tm = new TransferManager(new FixedContentStreamReader(content), fileCache, threadPool);
        WarmFullFileManager mgr = newManager(fileCache, 4, 0.5);
        FullFileState st = newStateFor(tm, fileCache, name, content);
        doAnswer(invocation -> {
            if (invocation.getArgument(1) instanceof PromotedWholeFileEntry) {
                assertEquals(FullFileState.State.FULL, st.state());
                mgr.onExtensionsChanged(Set.of()); // demotes, and the release runs here, before the put
                assertTrue(st.cleanupDone());
            }
            return invocation.callRealMethod();
        }).when(fileCache).put(any(), any());

        startPromotion(mgr, st);
        assertBusy(() -> assertEquals(1L, mgr.counters().get("promoted").longValue()), 30, TimeUnit.SECONDS);
        awaitPromotionThreadsIdle();

        assertEquals(FullFileState.State.BLOCK, st.state());
        assertNull("no entry is left in the cache", fileCache.get(path));
        assertFalse("the file is deleted", Files.exists(path));
        assertEquals(List.of(), retiredFiles());
        assertEquals("no bytes are left counted", 0L, fileCache.usage());
        assertEquals("the budget is back", 0L, mgr.reservedBytesPerSegment()[0]);
        assertEquals(0L, mgr.counters().get("activeDownloads").longValue());
    }

    public void testABudgetShortfallEvictsAWholeFileAndTheRetryFits() throws Exception {
        // The budget has room for one whole file and holds one. A second file does not fit: the first is evicted to
        // make room, and the second is declined for now, since the room comes back only with the release, and retried
        // soon.
        byte[] content = footeredContent(FILE_LENGTH);
        FileCache fileCache = newFileCache();
        TransferManager tm = new TransferManager(new FixedContentStreamReader(content), fileCache, threadPool);
        AtomicLong clock = new AtomicLong(1_000);
        WarmFullFileManager mgr = newTightBudgetManager(fileCache, clock::get);
        FullFileState a = newStateFor(tm, fileCache, "_a.faiss", content);
        startPromotion(mgr, a);
        awaitState(a, FullFileState.State.FULL);
        clock.addAndGet(1_000);

        FullFileState b = newStateFor(tm, fileCache, "_b.faiss", content);
        startPromotion(mgr, b);
        awaitState(b, FullFileState.State.BLOCK);

        assertEquals(1L, mgr.counters().get("declined.BUDGET_EXCEEDED").longValue());
        assertEquals("retried soon: room is on its way", 2_000 + WarmFullFileManager.NO_SLOT_RETRY_DELAY_MILLIS, b.retryAt());
        assertEquals("the file in the way was evicted", FullFileState.State.BLOCK, a.state());
        assertEquals(1L, mgr.counters().get("evictedForBudget").longValue());
        assertEquals(1L, mgr.counters().get("evicted").longValue());
        assertBusy(() -> assertTrue(a.cleanupDone()), 10, TimeUnit.SECONDS);
        assertEquals("its budget is back", 0L, mgr.reservedBytesPerSegment()[0]);

        clock.addAndGet(WarmFullFileManager.NO_SLOT_RETRY_DELAY_MILLIS);
        startPromotion(mgr, b);
        awaitState(b, FullFileState.State.FULL);
        assertEquals(content.length, mgr.reservedBytesPerSegment()[0]);
        assertEquals("nothing more was evicted for it", 1L, mgr.counters().get("evictedForBudget").longValue());
    }

    public void testABudgetShortfallEvictsTheLeastRecentlyUsedWholeFile() throws Exception {
        // Room for two whole files, both taken. The one a query used last stays; the other makes room for a third.
        byte[] content = footeredContent(FILE_LENGTH);
        FileCache fileCache = newFileCache();
        TransferManager tm = new TransferManager(new FixedContentStreamReader(content), fileCache, threadPool);
        AtomicLong clock = new AtomicLong(1_000);
        WarmFullFileManager mgr = newManager(fileCache, clock::get, 4, 0.001);
        FullFileState older = newStateFor(tm, fileCache, "_older.faiss", content);
        startPromotion(mgr, older);
        awaitState(older, FullFileState.State.FULL);
        clock.addAndGet(10_000);
        FullFileState newer = newStateFor(tm, fileCache, "_newer.faiss", content);
        startPromotion(mgr, newer);
        awaitState(newer, FullFileState.State.FULL);
        // A query reads the file that was promoted first, so it is now the one used last.
        clock.addAndGet(10_000);
        older.touch(clock.get());

        FullFileState third = newStateFor(tm, fileCache, "_third.faiss", content);
        startPromotion(mgr, third);
        awaitState(third, FullFileState.State.BLOCK);

        assertEquals("the file used longest ago was evicted", FullFileState.State.BLOCK, newer.state());
        assertEquals("the file used last stays", FullFileState.State.FULL, older.state());
        assertEquals(1L, mgr.counters().get("evictedForBudget").longValue());
    }

    public void testBytesOfAPendingReleaseCountAsRoomAndNothingMoreIsEvicted() throws Exception {
        // Room for two whole files, both taken. One is evicted by the cache while a query is open on its shard, so
        // its release, and its budget, wait for that query. A third file does not fit yet, but enough bytes are
        // already on their way back: the other whole file is left alone, and the third is admitted after the release.
        byte[] content = footeredContent(FILE_LENGTH);
        FileCache fileCache = newFileCache();
        TransferManager tm = new TransferManager(new FixedContentStreamReader(content), fileCache, threadPool);
        AtomicLong clock = new AtomicLong(1_000);
        WarmFullFileManager mgr = newManager(fileCache, clock::get, 4, 0.001);
        FullFileState leaving = newStateFor(tm, fileCache, "_leaving.faiss", content);
        startPromotion(mgr, leaving);
        awaitState(leaving, FullFileState.State.FULL);
        FullFileState staying = newStateFor(tm, fileCache, "_staying.faiss", content);
        startPromotion(mgr, staying);
        awaitState(staying, FullFileState.State.FULL);
        openQuery(leaving);
        fileCache.remove(leaving.fullFilePath());
        assertEquals(FullFileState.State.BLOCK, leaving.state());
        assertEquals("the budget of both is still held", 2L * content.length, mgr.reservedBytesPerSegment()[0]);

        FullFileState third = newStateFor(tm, fileCache, "_third.faiss", content);
        startPromotion(mgr, third);
        awaitState(third, FullFileState.State.BLOCK);

        assertEquals("retried soon", clock.get() + WarmFullFileManager.NO_SLOT_RETRY_DELAY_MILLIS, third.retryAt());
        assertEquals("nothing was evicted for it", 0L, mgr.counters().get("evictedForBudget").longValue());
        assertEquals(FullFileState.State.FULL, staying.state());

        closeQuery(leaving);
        assertBusy(() -> assertTrue(leaving.cleanupDone()), 10, TimeUnit.SECONDS);
        clock.addAndGet(WarmFullFileManager.NO_SLOT_RETRY_DELAY_MILLIS);
        startPromotion(mgr, third);
        awaitState(third, FullFileState.State.FULL);
        assertEquals(FullFileState.State.FULL, staying.state());
        assertEquals(2L * content.length, mgr.reservedBytesPerSegment()[0]);
    }

    public void testAFileLargerThanTheBudgetEvictsNothing() throws Exception {
        // No eviction could make the file fit, so none is made: an ordinary decline. Raising the fraction admits the
        // file on its retry.
        byte[] content = footeredContent(FILE_LENGTH);
        FileCache fileCache = newFileCache();
        TransferManager tm = new TransferManager(new FixedContentStreamReader(content), fileCache, threadPool);
        AtomicLong clock = new AtomicLong(1_000);
        WarmFullFileManager mgr = newTightBudgetManager(fileCache, clock::get);
        FullFileState a = newStateFor(tm, fileCache, "_a.faiss", content);
        startPromotion(mgr, a);
        awaitState(a, FullFileState.State.FULL);
        mgr.onBudgetFractionChanged(0.0001); // less than one file from here on

        FullFileState b = newStateFor(tm, fileCache, "_b.faiss", content);
        startPromotion(mgr, b);
        awaitState(b, FullFileState.State.BLOCK);

        assertEquals("an ordinary decline", clock.get() + WarmFullFileManager.RETRY_DELAY_MILLIS, b.retryAt());
        assertEquals(0L, mgr.counters().get("evictedForBudget").longValue());
        assertEquals(FullFileState.State.FULL, a.state());

        mgr.onBudgetFractionChanged(1.0);
        clock.addAndGet(WarmFullFileManager.RETRY_DELAY_MILLIS);
        startPromotion(mgr, b);
        awaitState(b, FullFileState.State.FULL);
        assertEquals(2L * content.length, mgr.reservedBytesPerSegment()[0]);
        assertEquals(FullFileState.State.FULL, a.state());
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

        Map<String, Long> before = mgr.counters();
        assertEquals(0L, before.get("promoted").longValue());
        assertEquals(0L, before.get("declined").longValue());
        assertEquals(0L, before.get("reservedBytes").longValue());
        assertEquals(0, before.get("activeDownloads").longValue());
        assertEquals(
            "every reason is present",
            DeclineReason.values().length,
            before.keySet().stream().filter(k -> k.startsWith("declined.")).count()
        );

        String loggerName = WarmFullFileManager.class.getName();
        try (MockLogAppender appender = MockLogAppender.createForLoggers(LogManager.getLogger(WarmFullFileManager.class))) {
            appender.addExpectation(
                new MockLogAppender.SeenEventExpectation(
                    "INFO line with bytes, time and parts",
                    loggerName,
                    Level.INFO,
                    "promoted [*"
                        + name
                        + "] to a whole file: 4096 bytes in 250 ms (4 parts of 1024 bytes, 0 from local blocks, cache segment 0)"
                )
            );
            FullFileState st = newState(tm, fileCache, name, FILE_LENGTH);
            startPromotion(mgr, st); // the reader blocks on the latch, so the download is in flight

            Map<String, Long> inFlight = mgr.counters();
            assertEquals("slot held while downloading", 1, inFlight.get("activeDownloads").longValue());
            assertEquals("bytes reserved while downloading", FILE_LENGTH, inFlight.get("reservedBytes").longValue());
            assertEquals(0L, inFlight.get("promoted").longValue());

            clock.addAndGet(250); // all download time passes before the first byte is served
            release.countDown();
            awaitState(st, FullFileState.State.FULL);

            // The counters and the INFO line follow the state change to FULL, so poll for them.
            assertBusy(() -> {
                Map<String, Long> after = mgr.counters();
                assertEquals(1L, after.get("requested").longValue());
                assertEquals(1L, after.get("started").longValue());
                assertEquals(1L, after.get("promoted").longValue());
                assertEquals(0L, after.get("failed").longValue());
                assertEquals(FILE_LENGTH, after.get("downloadedBytes").longValue());
                assertEquals(FILE_LENGTH, after.get("promotedBytes").longValue());
                assertArrayEquals("one segment, the file's bytes", new long[] { FILE_LENGTH }, mgr.reservedBytesPerSegment());
                assertEquals("download time is measured with the manager clock", 250L, after.get("downloadMillis").longValue());
                assertEquals("FULL keeps the budget reserved", FILE_LENGTH, after.get("reservedBytes").longValue());
                assertEquals("slot released at FULL", 0, after.get("activeDownloads").longValue());
                assertEquals(0L, after.get("declined").longValue());
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
        assertEquals(1L, budgetMgr.counters().get("declined.BUDGET_EXCEEDED").longValue());
        assertEquals(1L, budgetMgr.counters().get("declined").longValue());
        assertEquals("requested but never started", 1L, budgetMgr.counters().get("requested").longValue());
        assertEquals(0L, budgetMgr.counters().get("started").longValue());
        assertEquals("an admission decline is not a failure", 0L, budgetMgr.counters().get("failed").longValue());

        WarmFullFileManager mgr = newManager(fileCache, new AtomicLong()::get, 4, 0.5);

        // Too large: at the mmap chunk size, declined before taking a slot or reserving bytes.
        FullFileState huge = newState(tm, fileCache, "_huge.faiss", WarmFullFileManager.MAX_WHOLE_FILE_BYTES);
        startPromotion(mgr, huge);
        assertEquals(FullFileState.State.BLOCK, huge.state());

        // Not mmap: an NIO directory, declined permanently at admission, before any download.
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

        Map<String, Long> stats = mgr.counters();
        assertEquals(1L, stats.get("declined.TOO_LARGE").longValue());
        assertEquals(1L, stats.get("declined.NOT_MMAP").longValue());
        assertEquals(1L, stats.get("declined.DOWNLOAD_FAILED").longValue());
        assertEquals(0L, stats.get("declined.NO_SLOT").longValue());
        assertEquals(3L, stats.get("declined").longValue());
        assertEquals("three promote() calls", 3L, stats.get("requested").longValue());
        assertEquals("only the failing download started", 1L, stats.get("started").longValue());
        assertEquals("the failed part started and did not reach FULL", 1L, stats.get("failed").longValue());
        assertEquals("no download completed", 0L, stats.get("downloadedBytes").longValue());
        assertEquals("nothing promoted", 0L, stats.get("promoted").longValue());
        assertEquals("every decline released its bytes", 0L, stats.get("reservedBytes").longValue());
        assertEquals("every decline released its slot", 0, stats.get("activeDownloads").longValue());
        assertNotNull(stats.toString());
    }
}

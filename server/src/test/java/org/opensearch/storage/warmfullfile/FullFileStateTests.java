/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.storage.warmfullfile;

import com.carrotsearch.randomizedtesting.annotations.ThreadLeakFilters;

import org.apache.lucene.store.IndexInput;
import org.opensearch.common.lucene.store.ByteArrayIndexInput;
import org.opensearch.index.store.remote.file.CleanerDaemonThreadLeakFilter;
import org.opensearch.index.store.remote.filecache.CachedIndexInput;
import org.opensearch.index.store.remote.filecache.FileCache;
import org.opensearch.index.store.remote.filecache.FileCacheFactory;
import org.opensearch.test.OpenSearchTestCase;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Deque;
import java.util.List;
import java.util.concurrent.AbstractExecutorService;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.Executor;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Consumer;
import java.util.stream.Stream;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;

@ThreadLeakFilters(filters = CleanerDaemonThreadLeakFilter.class)
public class FullFileStateTests extends OpenSearchTestCase {

    private static Path tempDir;

    @Override
    public void setUp() throws Exception {
        super.setUp();
        tempDir = createTempDir();
    }

    private FullFileState newState() {
        // The reference-type fields are only consumed by the manager; null is fine for state-machine tests.
        return new FullFileState(null, null, null, "_5.faiss", 1_700_000_000L, Path.of("/cache/_5.faiss"));
    }

    public void testStartsInBlock() {
        FullFileState st = newState();
        assertEquals(FullFileState.State.BLOCK, st.state());
        assertNull(st.localMaster());
        assertNull(st.entry());
        assertEquals(0, st.tracker().openHolds());
        assertEquals(0L, st.retryAt());
    }

    public void testCompletePromotionPublishesOnceAndOnlyFromPromoting() throws Exception {
        FullFileState st = newState();
        try (
            IndexInput first = new ByteArrayIndexInput("first", new byte[4]);
            IndexInput second = new ByteArrayIndexInput("second", new byte[4])
        ) {
            assertEquals("not promoting", FullFileState.Completion.NOT_PROMOTING, complete(st, first));
            assertNull(st.localMaster());
            assertEquals(FullFileState.State.BLOCK, st.state());

            assertTrue(st.onAccess(0));
            assertEquals(FullFileState.Completion.FULL, complete(st, first));
            assertSame(first, st.localMaster());
            assertSame("the entry owns the mapping", first, st.entry().mapping());
            assertEquals(FullFileState.State.FULL, st.state());

            assertEquals("a second completion for the same download", FullFileState.Completion.PUBLISHED_BY_OTHER, complete(st, second));
            assertSame("the first mapping stays published", first, st.localMaster());
            assertEquals(FullFileState.State.FULL, st.state());
        }
    }

    private static FullFileState.Completion complete(FullFileState st, IndexInput mapping) {
        return st.completePromotion(
            new PromotedWholeFileEntry(null, st.fullFilePath(), mapping, st),
            null,
            () -> false,
            () -> {},
            () -> {},
            () -> {},
            () -> {}
        );
    }

    public void testRemoteModeIsOffUntilMarkedAndThenFinal() {
        FullFileState st = newState();
        assertFalse("a new family is not promotable until marked remote", st.isRemoteMode());
        assertNull("no checksum is known before the family is marked remote", st.expectedChecksum());
        st.markRemoteMode("123456789");
        assertTrue(st.isRemoteMode());
        assertEquals("the remote store's checksum travels with the mark", "123456789", st.expectedChecksum());
        st.markRemoteMode(null);
        assertTrue("marking is idempotent and never resets", st.isRemoteMode());
        assertEquals("remote mode is independent of the promotion state", FullFileState.State.BLOCK, st.state());
    }

    public void testOnAccessStartsAPromotionOnceAndAbandonPromotionEndsIt() {
        FullFileState st = newState();
        assertTrue(st.onAccess(0));
        assertEquals(FullFileState.State.PROMOTING, st.state());
        assertFalse("already promoting", st.onAccess(0));
        assertEquals(FullFileState.State.PROMOTING, st.state());

        assertTrue(st.abandonPromotion(12_345L));
        assertEquals(FullFileState.State.BLOCK, st.state());
        assertEquals(12_345L, st.retryAt());
        assertFalse("not promoting any more", st.abandonPromotion(99L));
        assertEquals("the retry time is set regardless", 99L, st.retryAt());

        assertFalse("before the retry time", st.onAccess(98L));
        assertEquals(FullFileState.State.BLOCK, st.state());
        assertTrue("at the retry time", st.onAccess(99L));
        assertEquals(FullFileState.State.PROMOTING, st.state());
    }

    public void testOnAccessWaitsForAPendingRelease() throws Exception {
        // A demoted family still holding its mapping must not be promoted again, or the file would be mapped twice.
        Fixture f = new Fixture();
        f.goFull();
        f.openQuery();
        assertTrue(f.st.demote());
        assertFalse("release pending", f.st.onAccess(Long.MAX_VALUE));
        assertEquals(FullFileState.State.BLOCK, f.st.state());

        f.closeQuery();
        f.runCleanupOnce();
        assertTrue("release done", f.st.onAccess(Long.MAX_VALUE));
        assertEquals(FullFileState.State.PROMOTING, f.st.state());
    }

    public void testFileLengthAccessor() {
        FullFileState st = newState();
        assertEquals(1_700_000_000L, st.fileLength());
    }

    // ---------------- use of the whole file ----------------

    public void testAQueryPhaseTakesNoCacheReferenceOnTheWholeFile() {
        // "In use" is counted on the shard's tracker, not on the file: a query phase open on the shard leaves the
        // whole file unreferenced in the cache, so the cache can evict it like a block.
        Fixture f = new Fixture();
        f.goFull();
        f.openQuery();
        f.openQuery();
        assertEquals(2, f.st.tracker().openHolds());
        assertEquals("no reference per query", Integer.valueOf(0), f.fileCache.getRef(f.path));

        f.closeQuery();
        f.closeQuery();
        assertEquals(0, f.st.tracker().openHolds());
        assertTrue("nothing owed, nothing scheduled", f.executor.tasks.isEmpty());
    }

    public void testTouchRefreshesTheCacheEntryAtMostOncePerSecond() {
        // A view refreshes the position of the whole file's cache entry, since nothing holds a reference on it; at
        // most once a second, and without leaving a reference behind.
        Fixture f = new Fixture();
        f.goFull();
        long hitsBefore = f.fileCache.stats().hitCount();

        f.st.touch(10_000);
        f.st.touch(10_500);
        f.st.touch(10_999);
        assertEquals("one refresh within a second", hitsBefore + 1, f.fileCache.stats().hitCount());

        f.st.touch(11_000);
        assertEquals("another once a second has passed", hitsBefore + 2, f.fileCache.stats().hitCount());
        assertEquals("no reference is left behind", Integer.valueOf(0), f.fileCache.getRef(f.path));
    }

    // ---------------- backoff after eviction ----------------

    public void testEachEvictionInARowDoublesTheWaitUpToTheCap() {
        FullFileState st = newState();
        long first = 30_000;
        long max = 100_000;
        long quiet = 600_000;

        assertEquals(30_000, st.backOffAfterEviction(1_000, first, max, quiet));
        assertEquals(1_000 + 30_000, st.retryAt());
        assertFalse("not before the wait is over", st.onAccess(1_000 + 29_999));

        assertEquals(60_000, st.backOffAfterEviction(40_000, first, max, quiet));
        assertEquals(40_000 + 60_000, st.retryAt());
        assertEquals("capped", 100_000, st.backOffAfterEviction(110_000, first, max, quiet));
        assertEquals(100_000, st.backOffAfterEviction(220_000, first, max, quiet));
        assertEquals(220_000 + 100_000, st.retryAt());
        assertTrue("once the wait is over", st.onAccess(320_000));
    }

    public void testAnEvictionAfterAQuietSpellStartsTheWaitAgain() {
        FullFileState st = newState();
        long first = 30_000;
        long max = 600_000;
        long quiet = 600_000;
        assertEquals(30_000, st.backOffAfterEviction(0, first, max, quiet));
        assertEquals(60_000, st.backOffAfterEviction(100_000, first, max, quiet));
        long waitEnded = 100_000 + 60_000;

        assertEquals("one millisecond short of quiet", 120_000, st.backOffAfterEviction(waitEnded + quiet - 1, first, max, quiet));
        waitEnded = waitEnded + quiet - 1 + 120_000;
        assertEquals("ten minutes past the end of the wait", 30_000, st.backOffAfterEviction(waitEnded + quiet, first, max, quiet));
    }

    public void testAFileEvictedAsSoonAsItsWaitEndsStaysAtTheCap() {
        // A file that is promoted and evicted again every time its wait ends keeps the longest wait. The wait is not
        // counted as a quiet spell, however long it is.
        FullFileState st = newState();
        long first = 30_000;
        long max = 600_000;
        long quiet = 600_000;
        long now = 0;
        long delay = 0;
        for (int eviction = 0; eviction < 12; eviction++) {
            delay = st.backOffAfterEviction(now, first, max, quiet);
            now += delay + 60_000; // the wait, then a minute to be promoted and evicted again
        }
        assertEquals(max, delay);
        assertEquals("and the next one too", max, st.backOffAfterEviction(now, first, max, quiet));
    }

    public void testNoWaitWhenTheBackoffIsOffAndALaterRetryTimeIsKept() {
        FullFileState st = newState();
        assertEquals(0, st.backOffAfterEviction(5_000, 0, 600_000, 600_000));
        assertEquals(5_000, st.retryAt());

        // A promotion declined for good is not brought back by an eviction.
        assertTrue(st.onAccess(5_000));
        assertTrue(st.abandonPromotion(Long.MAX_VALUE));
        st.backOffAfterEviction(6_000, 30_000, 600_000, 600_000);
        assertEquals(Long.MAX_VALUE, st.retryAt());
    }

    // ---------------- removal by the cache ----------------

    public void testTheCacheDroppingAFullFileDemotesItAndReleasesItOffThread() throws Exception {
        Fixture f = new Fixture();
        f.goFull();

        f.fileCache.remove(f.path); // eviction, prune, clear and remove all run the entry's close under the segment lock

        assertEquals(FullFileState.State.BLOCK, f.st.state());
        assertEquals("counted as an eviction", 1, f.evictions.get());
        assertEquals("the budget is held until the file is deleted", 0, f.budgetReleases.get());
        assertEquals("no scope: the release is scheduled at once", 1, f.executor.tasks.size());
        verify(f.master, never()).close();
        assertTrue("until it runs, the family cannot be promoted again", f.st.isMappingReleasePending());

        f.runCleanupOnce();
        verify(f.master).close();
        assertNull(f.st.localMaster());
        assertTrue(f.st.cleanupDone());
        assertEquals("the budget was released once", 1, f.budgetReleases.get());
        assertEquals(1, f.cleanupsDone.get());
        assertFalse(f.st.isMappingReleasePending());
        assertTrue("the next access promotes again", f.st.onAccess(Long.MAX_VALUE));
    }

    public void testTheCacheDroppingAFileUnderAScopeWaitsForTheScopeToUnmap() throws Exception {
        Fixture f = new Fixture();
        f.goFull();
        f.openQuery();

        f.fileCache.remove(f.path);

        assertEquals("demoted at once: new reads go to blocks", FullFileState.State.BLOCK, f.st.state());
        assertEquals(1, f.evictions.get());
        assertEquals("the budget is held: the mapped file still occupies the disk", 0, f.budgetReleases.get());
        assertEquals("reported: scopes still read the mapping", 1, f.awaitingReports.get());
        assertTrue("the scope keeps the mapping", f.executor.tasks.isEmpty());
        assertSame(f.master, f.st.localMaster());

        f.closeQuery();
        assertEquals("the last scope out schedules the unmap", 1, f.executor.tasks.size());
        f.runCleanupOnce();
        verify(f.master).close();
        assertNull(f.st.localMaster());
        assertEquals("the key is not touched again: the cache dropped it", null, f.fileCache.get(f.path));
        assertEquals("the budget goes back with the release", 1, f.budgetReleases.get());
    }

    public void testAFailedDeleteKeepsTheBudgetUntilTheReleaseIsRunAgain() throws Exception {
        // The release cannot delete the file the cache moved aside: the budget stays held, the family is reported, and
        // nothing is retried by itself. Run again once the delete can succeed, the release completes.
        Fixture f = new Fixture();
        f.goFull();
        PromotedWholeFileEntry entry = f.st.entry();
        f.fileCache.remove(f.path);
        Path undeletable = Files.createDirectory(tempDir.resolve("_5.faiss.x.retired"));
        Files.createFile(undeletable.resolve("blocker"));
        entry.onFileRetired(undeletable); // a non-empty directory cannot be deleted as a file

        f.runCleanupOnce();

        verify(f.master).close();
        assertEquals("the budget is held", 0, f.budgetReleases.get());
        assertFalse(f.st.cleanupDone());
        assertEquals(0, f.cleanupsDone.get());
        assertEquals("reported as awaiting its release", 1, f.awaitingReports.get());
        assertTrue("nothing is rescheduled by itself", f.executor.tasks.isEmpty());
        assertTrue("no promotion while the release is pending", f.st.isMappingReleasePending());

        // Empty the directory, whatever the test file system has put in it besides the blocker.
        try (Stream<Path> files = Files.list(undeletable)) {
            for (Path file : files.toList()) {
                Files.delete(file);
            }
        }
        f.st.retryRelease();
        f.runCleanupOnce();

        assertFalse(Files.exists(undeletable));
        assertEquals(1, f.budgetReleases.get());
        assertTrue(f.st.cleanupDone());
        assertEquals(1, f.cleanupsDone.get());
        f.st.retryRelease();
        assertTrue("a completed release is not run again", f.executor.tasks.isEmpty());
    }

    public void testAnEntryOfAnEarlierPeriodOrOfADemotedFamilyChangesNothing() throws Exception {
        Fixture f = new Fixture();
        f.goFull();
        PromotedWholeFileEntry first = f.st.entry();
        assertTrue(f.st.demote());
        f.runCleanupOnce(); // removes the demoted file from the cache, which runs onEntryRemoved for its own entry
        assertTrue(f.st.cleanupDone());
        assertEquals(0, f.evictions.get());
        assertEquals(1, f.budgetReleases.get());

        f.startSecondPeriod();
        f.st.onEntryRemoved(first);
        assertEquals("a stale entry is ignored", FullFileState.State.FULL, f.st.state());
        assertEquals(0, f.evictions.get());
        assertEquals("the second period's budget is untouched", 1, f.budgetReleases.get());
        assertTrue(f.executor.tasks.isEmpty());
    }

    // ---------------- deferred close ----------------

    /** Records tasks instead of running them, so a test can prove the cleanup never runs on the calling thread. */
    private static final class RecordingExecutor implements Executor {
        final List<Runnable> tasks = new CopyOnWriteArrayList<>();

        @Override
        public void execute(Runnable command) {
            tasks.add(command);
        }

        void runAll() {
            for (Runnable r : tasks) {
                r.run();
            }
        }
    }

    private static final class Fixture {
        final FileCache fileCache = FileCacheFactory.createConcurrentLRUFileCache(1_000_000, 1);
        // Under the test's sandbox, since the cache deletes the file of an entry it drops; one directory per test.
        final Path path = tempDir.resolve("_5.faiss");
        final IndexInput master = mock(IndexInput.class);
        final RecordingExecutor executor = new RecordingExecutor();
        // Counts the periods whose budget was given back: the production release is a once-only Releasable, so a
        // period counts once however often its release runs.
        final AtomicInteger budgetReleases = new AtomicInteger();
        final AtomicInteger awaitingReports = new AtomicInteger();
        final AtomicInteger cleanupsDone = new AtomicInteger();
        final AtomicInteger evictions = new AtomicInteger();
        final AtomicBoolean nodeStopping = new AtomicBoolean();
        final FullFileState st;

        Fixture() {
            st = new FullFileState(null, null, fileCache, "_5.faiss", 10, path);
        }

        void goFull() {
            assertTrue(st.onAccess(0));
            assertEquals(FullFileState.Completion.FULL, complete(master));
        }

        /** Publishes {@code mapping} and puts its entry in the cache unreferenced, as the manager does. */
        FullFileState.Completion complete(IndexInput mapping) {
            PromotedWholeFileEntry entry = new PromotedWholeFileEntry(fileCache, path, mapping, st);
            AtomicBoolean released = new AtomicBoolean(false);
            FullFileState.Completion completion = st.completePromotion(entry, executor, nodeStopping::get, () -> {
                if (released.compareAndSet(false, true)) {
                    budgetReleases.incrementAndGet();
                }
            }, awaitingReports::incrementAndGet, cleanupsDone::incrementAndGet, evictions::incrementAndGet);
            if (completion == FullFileState.Completion.FULL) {
                CachedIndexInput previous = fileCache.put(path, entry);
                assertNull("no entry of an earlier period at the key", previous);
                fileCache.decRef(path);
            }
            return completion;
        }

        // The query phases this fixture has open on the family's shard, newest first.
        final Deque<PhaseHold> queries = new ArrayDeque<>();

        /** Opens a query phase on the family's shard, as the warm search listener does. */
        void openQuery() {
            queries.push(st.tracker().acquire());
        }

        /** Ends the query phase opened last. */
        void closeQuery() {
            queries.pop().close();
        }

        /** Runs the one cleanup task scheduled since the last call, as the pool would. */
        void runCleanupOnce() {
            assertEquals("exactly one cleanup scheduled", 1, executor.tasks.size());
            executor.tasks.remove(0).run();
        }

        /** Promotes the released family again, as the manager does: a new entry, a new teardown and a new mapping. */
        IndexInput startSecondPeriod() {
            IndexInput second = mock(IndexInput.class);
            assertTrue(st.onAccess(Long.MAX_VALUE));
            assertEquals(FullFileState.Completion.FULL, complete(second));
            assertFalse("a new period owes nothing yet", st.cleanupDone());
            assertFalse(st.isMappingReleasePending());
            return second;
        }
    }

    public void testCloseWhileFullWithNoScopesDefersCleanupOffThread() throws Exception {
        Fixture f = new Fixture();
        f.goFull();

        f.st.closeFamily();

        assertEquals(FullFileState.State.CLOSED, f.st.state());
        assertEquals("budget held until the cleanup runs", 0, f.budgetReleases.get());
        assertEquals("cleanup scheduled once", 1, f.executor.tasks.size());
        // Not run inline: the caller may be the FileCache removal listener holding a segment lock.
        verify(f.master, never()).close();
        assertFalse(f.st.cleanupDone());

        f.executor.runAll();
        verify(f.master).close();
        assertEquals("budget released at the unmap", 1, f.budgetReleases.get());
        assertTrue(f.st.cleanupDone());
        assertEquals("cleanup-done hook ran once", 1, f.cleanupsDone.get());
        assertNull(f.st.localMaster());
        assertEquals("no report without scopes", 0, f.awaitingReports.get());
        assertNull("a closed family's whole file is released with it", f.fileCache.get(f.path));
    }

    public void testCloseWhileFullWaitsForTheLastScope() throws Exception {
        Fixture f = new Fixture();
        f.goFull();
        f.openQuery();
        f.openQuery();

        f.st.closeFamily();
        assertEquals("budget held while scopes read", 0, f.budgetReleases.get());
        assertEquals("reported: closed with scopes reading", 1, f.awaitingReports.get());
        assertEquals("cleanup-done hook not yet run", 0, f.cleanupsDone.get());
        assertTrue("no cleanup while scopes read", f.executor.tasks.isEmpty());

        f.closeQuery();
        assertTrue("one scope still reads", f.executor.tasks.isEmpty());

        f.closeQuery();
        assertEquals("the last scope out schedules the cleanup", 1, f.executor.tasks.size());
        f.executor.runAll();
        verify(f.master).close();
        assertEquals("budget released at the unmap", 1, f.budgetReleases.get());
    }

    public void testCloseOfADemotedFamilyWithScopesReportsAndReleasesOnTheLastOne() throws Exception {
        // Demoted with a query phase open on the shard: the mapping and the budget are still held, the demotion reports
        // the family once, a close that follows changes nothing about that, and the last query out releases everything.
        Fixture f = new Fixture();
        f.goFull();
        f.openQuery();
        assertTrue(f.st.demote());
        assertEquals("reported: demoted with scopes reading", 1, f.awaitingReports.get());
        assertTrue(f.executor.tasks.isEmpty());

        f.st.closeFamily();
        assertEquals(FullFileState.State.CLOSED, f.st.state());
        assertEquals("reported once for the release it waits for", 1, f.awaitingReports.get());
        assertTrue("no cleanup while the scope reads", f.executor.tasks.isEmpty());
        assertEquals(0, f.budgetReleases.get());

        f.closeQuery();
        f.runCleanupOnce();
        verify(f.master).close();
        assertEquals(1, f.budgetReleases.get());
        assertEquals(1, f.cleanupsDone.get());
    }

    public void testCloseWhileBlockOrPromotingOwesNoCleanup() throws Exception {
        // BLOCK
        Fixture blocked = new Fixture();
        blocked.st.closeFamily();
        assertEquals(FullFileState.State.CLOSED, blocked.st.state());
        assertEquals(0, blocked.budgetReleases.get());
        assertTrue(blocked.executor.tasks.isEmpty());

        // PROMOTING, closed before the completion handler publishes: the publish is refused and installs nothing, so
        // the handler unwinds its own mapping; closeFamily must not ALSO clean up (double close).
        Fixture promoting = new Fixture();
        assertTrue(promoting.st.onAccess(0));
        promoting.st.closeFamily();
        assertEquals("publish refused after the close", FullFileState.Completion.NOT_PROMOTING, promoting.complete(promoting.master));
        assertNull(promoting.st.localMaster());
        assertEquals(0, promoting.budgetReleases.get());
        assertTrue(promoting.executor.tasks.isEmpty());
        verify(promoting.master, never()).close();
    }

    public void testRepeatedCloseFamilyIsIdempotent() throws Exception {
        Fixture f = new Fixture();
        f.goFull();
        f.st.closeFamily();
        f.st.closeFamily();
        assertEquals(1, f.executor.tasks.size());
        f.executor.runAll();
        assertEquals(1, f.budgetReleases.get());
    }

    public void testConcurrentCloseAndLastScopeEndCleanUpExactlyOnce() throws Exception {
        for (int i = 0; i < 300; i++) {
            Fixture f = new Fixture();
            f.goFull();
            f.openQuery();
            CyclicBarrier barrier = new CyclicBarrier(2);
            Thread closer = new Thread(() -> {
                await(barrier);
                f.st.closeFamily();
            });
            Thread lastScope = new Thread(() -> {
                await(barrier);
                f.closeQuery();
            });
            closer.start();
            lastScope.start();
            closer.join();
            lastScope.join();
            assertEquals("iteration " + i + ": cleanup scheduled exactly once", 1, f.executor.tasks.size());
            f.executor.runAll();
            assertEquals("iteration " + i + ": budget released exactly once", 1, f.budgetReleases.get());
        }
    }

    public void testDemoteNeverShowsBlockWithoutAPendingRelease() throws Exception {
        // The trigger reads the state and then isMappingReleasePending. Whenever it observes BLOCK on a family that
        // still holds its mapping it must also observe the pending release, or it would map the file a second time.
        for (int i = 0; i < 20_000; i++) {
            Fixture f = new Fixture();
            f.goFull();
            f.openQuery();
            CyclicBarrier barrier = new CyclicBarrier(2);
            boolean[] exposed = new boolean[1];
            Thread demoter = new Thread(() -> {
                await(barrier);
                f.st.demote();
            });
            Thread trigger = new Thread(() -> {
                await(barrier);
                while (f.st.state() == FullFileState.State.FULL) {
                    Thread.onSpinWait();
                }
                exposed[0] = f.st.isMappingReleasePending() == false;
            });
            demoter.start();
            trigger.start();
            demoter.join();
            trigger.join();
            assertFalse("iteration " + i + ": BLOCK was visible before the pending release", exposed[0]);
        }
    }

    public void testDemoteOfANonFullFamilyLeavesTheReleaseFlagAlone() {
        Fixture f = new Fixture();
        assertFalse(f.st.demote());
        assertFalse("a family that never held a mapping owes no release", f.st.isMappingReleasePending());
        f.goFull();
        f.openQuery();
        assertTrue(f.st.demote());
        assertFalse("already demoted", f.st.demote());
        assertTrue("the first demotion's release is still owed", f.st.isMappingReleasePending());
    }

    public void testASecondFullPeriodAfterADemotionIsReleasedByItsDemote() throws Exception {
        // Demote, release, promote, demote: the second demotion owes a release of its own, and the one-shot flags of the
        // first period must not swallow it.
        Fixture f = new Fixture();
        f.goFull();
        assertTrue(f.st.demote());
        f.runCleanupOnce();
        assertFirstPeriodReleased(f);
        assertNull("a demoted family drops its whole file from the cache", f.fileCache.get(f.path));

        IndexInput second = f.startSecondPeriod();

        assertTrue(f.st.demote());
        f.runCleanupOnce();
        assertSecondPeriodReleased(f, second);
        assertNull("a demoted family drops its whole file from the cache", f.fileCache.get(f.path));
    }

    public void testASecondFullPeriodAfterADemotionIsReleasedByItsClose() throws Exception {
        // Demote, release, promote, close: the close releases the second period's budget and mapping.
        Fixture f = new Fixture();
        f.goFull();
        assertTrue(f.st.demote());
        f.runCleanupOnce();
        assertFirstPeriodReleased(f);

        IndexInput second = f.startSecondPeriod();

        f.st.closeFamily();
        assertEquals("budget held until the cleanup", 1, f.budgetReleases.get());
        f.runCleanupOnce();
        assertSecondPeriodReleased(f, second);
        assertEquals(FullFileState.State.CLOSED, f.st.state());
        assertNull("a closed family's whole file leaves the cache with it", f.fileCache.getRef(f.path));
    }

    private static void assertFirstPeriodReleased(Fixture f) throws Exception {
        assertTrue(f.st.cleanupDone());
        assertEquals(1, f.budgetReleases.get());
        assertEquals(1, f.cleanupsDone.get());
        verify(f.master).close();
        assertNull(f.st.localMaster());
        assertFalse(f.st.isMappingReleasePending());
        assertEquals(FullFileState.State.BLOCK, f.st.state());
    }

    private static void assertSecondPeriodReleased(Fixture f, IndexInput second) throws Exception {
        assertTrue("the second release ran", f.st.cleanupDone());
        assertEquals("the second period's budget was released", 2, f.budgetReleases.get());
        assertEquals(2, f.cleanupsDone.get());
        verify(second).close();
        assertNull(f.st.localMaster());
        assertFalse(f.st.isMappingReleasePending());
    }

    public void testConcurrentDemoteAndCloseScheduleTheReleaseOnce() throws Exception {
        // A FULL family with a scope reading is demoted by a settings change while it closes on the FileCache listener
        // thread. Whatever the order, once the scope ends exactly one cleanup is scheduled.
        int wrong = 0;
        final int iterations = 20_000;
        for (int i = 0; i < iterations; i++) {
            Fixture f = new Fixture();
            f.goFull();
            f.openQuery();
            CyclicBarrier barrier = new CyclicBarrier(2);
            Thread demoter = new Thread(() -> {
                await(barrier);
                f.st.demote();
            });
            Thread closer = new Thread(() -> {
                await(barrier);
                f.st.closeFamily();
            });
            demoter.start();
            closer.start();
            demoter.join();
            closer.join();
            assertEquals(FullFileState.State.CLOSED, f.st.state());
            assertTrue("the scope keeps the mapping", f.executor.tasks.isEmpty());
            f.closeQuery();
            if (f.executor.tasks.size() != 1) {
                wrong++;
            }
        }
        assertEquals("iterations whose mapping release was not scheduled exactly once", 0, wrong);
    }

    public void testEveryWayOutOfFullEndsInTheSameRelease() throws Exception {
        // The cache dropping the file, a demotion and a close each take the family out of FULL at once, report it
        // while a scope still reads, and leave the mapping to one release that runs when the scope ends.
        List<Consumer<Fixture>> waysOut = List.of(f -> f.fileCache.remove(f.path), f -> assertTrue(f.st.demote()), f -> f.st.closeFamily());
        for (Consumer<Fixture> wayOut : waysOut) {
            Fixture f = new Fixture();
            f.goFull();
            f.openQuery();

            wayOut.accept(f);

            assertNotEquals(FullFileState.State.FULL, f.st.state());
            assertEquals("reported: a scope still reads the mapping", 1, f.awaitingReports.get());
            assertTrue("the scope keeps the mapping", f.executor.tasks.isEmpty());
            assertSame(f.master, f.st.localMaster());
            verify(f.master, never()).close();

            f.closeQuery();
            f.runCleanupOnce();

            verify(f.master).close();
            assertNull(f.st.localMaster());
            assertTrue(f.st.cleanupDone());
            assertEquals(1, f.cleanupsDone.get());
            assertEquals(1, f.budgetReleases.get());
            assertFalse(f.st.isMappingReleasePending());
            assertTrue("nothing further is scheduled", f.executor.tasks.isEmpty());
        }
    }

    public void testAReleaseOnAStoppingNodeClosesTheMappingAndLeavesTheFile() throws Exception {
        // The node is stopping when a family closes with its shard: the release closes the mapping and nothing else.
        // The entry and the file stay, for the next start to validate and serve again, and nothing is given back.
        Fixture f = new Fixture();
        f.goFull();
        PromotedWholeFileEntry entry = f.st.entry();
        f.nodeStopping.set(true);

        f.st.closeFamily();
        f.runCleanupOnce();

        verify(f.master).close();
        assertNull(f.st.localMaster());
        assertSame("the entry stays in the cache", entry, f.fileCache.get(f.path));
        f.fileCache.decRef(f.path);
        assertFalse("nothing was moved aside or owed", entry.owedToCache());
        assertEquals(0, f.budgetReleases.get());
        assertFalse(f.st.cleanupDone());
        assertEquals(0, f.cleanupsDone.get());
    }

    public void testAReleaseAfterItsExecutorShutDownLeavesTheFileToo() throws Exception {
        // The pool the release runs on is shut down by the time a queued release runs: treated like a stopping node.
        Fixture f = new Fixture();
        List<Runnable> queued = new ArrayList<>();
        AtomicBoolean shutdown = new AtomicBoolean();
        ExecutorService pool = new AbstractExecutorService() {
            @Override
            public void execute(Runnable command) {
                queued.add(command);
            }

            @Override
            public void shutdown() {
                shutdown.set(true);
            }

            @Override
            public List<Runnable> shutdownNow() {
                shutdown.set(true);
                return List.of();
            }

            @Override
            public boolean isShutdown() {
                return shutdown.get();
            }

            @Override
            public boolean isTerminated() {
                return false;
            }

            @Override
            public boolean awaitTermination(long timeout, TimeUnit unit) {
                return false;
            }
        };
        assertTrue(f.st.onAccess(0));
        PromotedWholeFileEntry entry = new PromotedWholeFileEntry(f.fileCache, f.path, f.master, f.st);
        assertEquals(
            FullFileState.Completion.FULL,
            f.st.completePromotion(
                entry,
                pool,
                () -> false,
                f.budgetReleases::incrementAndGet,
                () -> {},
                f.cleanupsDone::incrementAndGet,
                () -> {}
            )
        );
        f.fileCache.put(f.path, entry);
        f.fileCache.decRef(f.path);

        assertTrue(f.st.demote());
        assertEquals("the release is queued", 1, queued.size());
        pool.shutdown();
        queued.get(0).run();

        verify(f.master).close();
        assertSame("the entry stays in the cache", entry, f.fileCache.get(f.path));
        f.fileCache.decRef(f.path);
        assertFalse(f.st.cleanupDone());
        assertEquals(0, f.cleanupsDone.get());
        assertEquals(0, f.budgetReleases.get());
    }

    private static void await(CyclicBarrier barrier) {
        try {
            barrier.await();
        } catch (Exception e) {
            throw new AssertionError(e);
        }
    }
}

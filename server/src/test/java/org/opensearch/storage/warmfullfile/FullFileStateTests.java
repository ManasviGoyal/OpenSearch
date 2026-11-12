/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */
package org.opensearch.storage.warmfullfile;

import org.apache.lucene.store.IndexInput;
import org.opensearch.common.lucene.store.ByteArrayIndexInput;
import org.opensearch.index.store.remote.filecache.FileCache;
import org.opensearch.index.store.remote.filecache.FileCacheFactory;
import org.opensearch.test.OpenSearchTestCase;

import java.nio.file.Path;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.Executor;
import java.util.concurrent.atomic.AtomicInteger;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;

public class FullFileStateTests extends OpenSearchTestCase {

    private FullFileState newState() {
        // The reference-type fields are only consumed by the manager; null is fine for state-machine tests.
        return new FullFileState(null, null, null, "_5.faiss", 1_700_000_000L, Path.of("/cache/_5.faiss"));
    }

    public void testStartsInBlock() {
        FullFileState st = newState();
        assertEquals(FullFileState.State.BLOCK, st.state());
        assertNull(st.localMaster());
        assertEquals(0, st.handleCount());
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
            assertEquals(FullFileState.State.FULL, st.state());

            assertEquals("a second completion for the same download", FullFileState.Completion.PUBLISHED_BY_OTHER, complete(st, second));
            assertSame("the first mapping stays published", first, st.localMaster());
            assertEquals(FullFileState.State.FULL, st.state());
        }
    }

    private static FullFileState.Completion complete(FullFileState st, IndexInput mapping) {
        return st.completePromotion(mapping, null, () -> {}, () -> {}, () -> {});
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
        f.st.incHandles();
        assertTrue(f.st.demote());
        assertFalse("release pending", f.st.onAccess(Long.MAX_VALUE));
        assertEquals(FullFileState.State.BLOCK, f.st.state());

        f.st.decHandles();
        f.runCleanupOnce();
        assertTrue("release done", f.st.onAccess(Long.MAX_VALUE));
        assertEquals(FullFileState.State.PROMOTING, f.st.state());
    }

    public void testHandleCount() {
        FullFileState st = newState();
        assertEquals(1, st.incHandles());
        assertEquals(2, st.incHandles());
        assertEquals(1, st.decHandles());
        assertEquals(0, st.decHandles());
        assertEquals(0, st.handleCount());
    }

    public void testFileLengthAccessor() {
        FullFileState st = newState();
        assertEquals(1_700_000_000L, st.fileLength());
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
        final Path path = Path.of("/cache/_5.faiss");
        final IndexInput master = mock(IndexInput.class);
        final RecordingExecutor executor = new RecordingExecutor();
        final AtomicInteger budgetReleases = new AtomicInteger();
        final AtomicInteger stuckReports = new AtomicInteger();
        final AtomicInteger cleanupsDone = new AtomicInteger();
        final FullFileState st;

        Fixture() {
            // The family's reference on the whole-file entry (put takes one, as the manager's get() does).
            fileCache.put(path, new FileCache.RestoredCachedIndexInput(10));
            st = new FullFileState(null, null, fileCache, "_5.faiss", 10, path);
        }

        void goFull() {
            assertTrue(st.onAccess(0));
            assertEquals(FullFileState.Completion.FULL, complete(master));
        }

        FullFileState.Completion complete(IndexInput mapping) {
            return st.completePromotion(
                mapping,
                executor,
                budgetReleases::incrementAndGet,
                stuckReports::incrementAndGet,
                cleanupsDone::incrementAndGet
            );
        }

        /** Runs the one cleanup task scheduled since the last call, as the pool would. */
        void runCleanupOnce() {
            assertEquals("exactly one cleanup scheduled", 1, executor.tasks.size());
            executor.tasks.remove(0).run();
        }

        /** Promotes the released family again, as the manager does: a new entry, a new teardown and a new mapping. */
        IndexInput startSecondPeriod() {
            fileCache.put(path, new FileCache.RestoredCachedIndexInput(10));
            IndexInput second = mock(IndexInput.class);
            assertTrue(st.onAccess(Long.MAX_VALUE));
            assertEquals(FullFileState.Completion.FULL, complete(second));
            assertFalse("a new period owes nothing yet", st.cleanupDone());
            assertFalse(st.isMappingReleasePending());
            return second;
        }
    }

    public void testCloseWhileFullWithNoHandlesDefersCleanupOffThread() throws Exception {
        Fixture f = new Fixture();
        f.goFull();

        f.st.closeFamily();

        assertEquals(FullFileState.State.CLOSED, f.st.state());
        assertEquals("budget held until the cleanup runs", 0, f.budgetReleases.get());
        assertEquals("cleanup scheduled once", 1, f.executor.tasks.size());
        // Not run inline: the caller may be the FileCache removal listener holding a segment lock.
        verify(f.master, never()).close();
        assertEquals(Integer.valueOf(1), f.fileCache.getRef(f.path));
        assertFalse(f.st.cleanupDone());

        f.executor.runAll();
        verify(f.master).close();
        assertEquals("family reference released", Integer.valueOf(0), f.fileCache.getRef(f.path));
        assertEquals("budget released with the reference", 1, f.budgetReleases.get());
        assertTrue(f.st.cleanupDone());
        assertEquals("cleanup-done hook ran once", 1, f.cleanupsDone.get());
        assertNull(f.st.localMaster());
        assertEquals("no stuck-handle report without handles", 0, f.stuckReports.get());
    }

    public void testCloseWhileFullWaitsForLastHandle() throws Exception {
        Fixture f = new Fixture();
        f.goFull();
        f.st.incHandles();
        f.st.incHandles();

        f.st.closeFamily();
        assertEquals("budget held while handles are open", 0, f.budgetReleases.get());
        assertEquals("reported: closed with live handles", 1, f.stuckReports.get());
        assertEquals("cleanup-done hook not yet run", 0, f.cleanupsDone.get());
        assertTrue("no cleanup while handles are open", f.executor.tasks.isEmpty());

        f.st.decHandles();
        assertTrue("one handle still open", f.executor.tasks.isEmpty());

        f.st.decHandles();
        assertEquals("last close schedules the cleanup", 1, f.executor.tasks.size());
        f.executor.runAll();
        verify(f.master).close();
        assertEquals(Integer.valueOf(0), f.fileCache.getRef(f.path));
        assertEquals("budget released with the reference", 1, f.budgetReleases.get());
    }

    public void testCloseOfADemotedFamilyWithOpenHandlesReportsAndReleasesOnLastClose() throws Exception {
        // Demoted with a handle open: the mapping, the reference and the budget are still held, so a close reports the
        // live handle like a FULL close does, and the last handle close releases everything.
        Fixture f = new Fixture();
        f.goFull();
        f.st.incHandles();
        assertTrue(f.st.demote());
        assertTrue(f.executor.tasks.isEmpty());

        f.st.closeFamily();
        assertEquals(FullFileState.State.CLOSED, f.st.state());
        assertEquals("reported: closed with live handles", 1, f.stuckReports.get());
        assertTrue("no cleanup while the handle is open", f.executor.tasks.isEmpty());
        assertEquals(0, f.budgetReleases.get());

        f.st.decHandles();
        f.runCleanupOnce();
        verify(f.master).close();
        assertEquals(Integer.valueOf(0), f.fileCache.getRef(f.path));
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
        // the handler unwinds its own mapping; closeFamily must not ALSO clean up (double close/decRef).
        Fixture promoting = new Fixture();
        assertTrue(promoting.st.onAccess(0));
        promoting.st.incHandles();
        promoting.st.closeFamily();
        assertEquals("publish refused after the close", FullFileState.Completion.NOT_PROMOTING, promoting.complete(promoting.master));
        assertNull(promoting.st.localMaster());
        promoting.st.decHandles();
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

    public void testConcurrentCloseAndLastHandleCloseCleansUpExactlyOnce() throws Exception {
        for (int i = 0; i < 300; i++) {
            Fixture f = new Fixture();
            f.goFull();
            f.st.incHandles();
            CyclicBarrier barrier = new CyclicBarrier(2);
            Thread closer = new Thread(() -> {
                await(barrier);
                f.st.closeFamily();
            });
            Thread lastHandle = new Thread(() -> {
                await(barrier);
                f.st.decHandles();
            });
            closer.start();
            lastHandle.start();
            closer.join();
            lastHandle.join();
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
            f.st.incHandles();
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
        f.st.incHandles();
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
        assertEquals(
            "a closed family leaves the entry, unreferenced, to the directory's close",
            Integer.valueOf(0),
            f.fileCache.getRef(f.path)
        );
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

    public void testConcurrentRestoreAndCloseNeverLoseTheMappingRelease() throws Exception {
        // A demoted family (handle open, mapping held) is restored by a settings change while the family closes on the
        // FileCache listener thread. Whatever the order, once the handle closes exactly one cleanup must be scheduled:
        // the mapping and the family's cache reference have to be released.
        int lost = 0;
        int restoredThenClosed = 0;
        final int iterations = 20_000;
        for (int i = 0; i < iterations; i++) {
            Fixture f = new Fixture();
            f.goFull();
            f.st.incHandles();
            assertTrue(f.st.demote());
            CyclicBarrier barrier = new CyclicBarrier(2);
            boolean[] restored = new boolean[1];
            Thread restorer = new Thread(() -> {
                await(barrier);
                restored[0] = f.st.restore();
            });
            Thread closer = new Thread(() -> {
                await(barrier);
                f.st.closeFamily();
            });
            restorer.start();
            closer.start();
            restorer.join();
            closer.join();
            assertEquals(FullFileState.State.CLOSED, f.st.state());
            f.st.decHandles();
            if (restored[0]) {
                restoredThenClosed++;
            }
            if (f.executor.tasks.size() != 1) {
                lost++;
            }
        }
        logger.info("restore won {} of {} races; cleanup lost {} times", restoredThenClosed, iterations, lost);
        assertEquals("iterations whose mapping release was never scheduled", 0, lost);
    }

    public void testReleaseAfterEvictionReleasesADemotedFamilyUnderItsOpenHandle() throws Exception {
        // The eviction policy demotes a family whose reader still holds a handle, and releases its mapping once the
        // searches running at the demotion have ended, without waiting for the handle.
        Fixture f = new Fixture();
        f.goFull();
        f.st.incHandles();
        assertTrue(f.st.demote());
        assertEquals("the open handle holds the release", 0, f.executor.tasks.size());

        assertTrue(f.st.releaseAfterEviction());
        assertFalse("scheduled once", f.st.releaseAfterEviction());
        f.runCleanupOnce();
        assertTrue(f.st.cleanupDone());
        assertNull(f.st.localMaster());
        assertEquals(1, f.budgetReleases.get());
        assertFalse(f.st.isMappingReleasePending());
        assertEquals("the handle is still open", 1, f.st.handleCount());

        f.st.decHandles();
        assertEquals("the handle's close owes nothing more", 0, f.executor.tasks.size());
    }

    public void testReleaseAfterEvictionIsANoOpUnlessAReleaseIsOwedAndNotScheduled() throws Exception {
        Fixture f = new Fixture();
        assertFalse("never FULL: nothing owed", f.st.releaseAfterEviction());
        f.goFull();
        assertFalse("FULL: not demoted", f.st.releaseAfterEviction());

        f.st.incHandles();
        assertTrue(f.st.demote());
        assertTrue("restored before the release", f.st.restore());
        assertFalse("restored: nothing owed", f.st.releaseAfterEviction());
        assertEquals(FullFileState.State.FULL, f.st.state());
        assertEquals(0, f.executor.tasks.size());

        // A demotion with no handle open schedules its own release; the eviction finds it scheduled.
        f.st.decHandles();
        assertTrue(f.st.demote());
        assertEquals(1, f.executor.tasks.size());
        assertFalse(f.st.releaseAfterEviction());
        f.runCleanupOnce();
        assertTrue(f.st.cleanupDone());
    }

    private static void await(CyclicBarrier barrier) {
        try {
            barrier.await();
        } catch (Exception e) {
            throw new AssertionError(e);
        }
    }
}

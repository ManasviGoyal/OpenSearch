/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */
package org.opensearch.storage.warmfullfile;

import org.opensearch.test.OpenSearchTestCase;

import java.nio.file.Path;

public class FullFileStateTests extends OpenSearchTestCase {

    private FullFileState newState() {
        // The reference-type fields are only consumed by the manager; null is fine for state-machine tests.
        return new FullFileState(null, null, null, null, "_5.faiss", 1_700_000_000L, Path.of("/cache/_5.faiss"));
    }

    public void testStartsInBlock() {
        FullFileState st = newState();
        assertEquals(FullFileState.State.BLOCK, st.state());
        assertNull(st.localMaster());
        assertEquals(0, st.handleCount());
        assertEquals(0L, st.retryAt());
    }

    public void testCasTransitions() {
        FullFileState st = newState();
        assertTrue(st.casState(FullFileState.State.BLOCK, FullFileState.State.PROMOTING));
        assertEquals(FullFileState.State.PROMOTING, st.state());
        // a CAS from the wrong expected value must fail and not change the state
        assertFalse(st.casState(FullFileState.State.BLOCK, FullFileState.State.FULL));
        assertEquals(FullFileState.State.PROMOTING, st.state());
        assertTrue(st.casState(FullFileState.State.PROMOTING, FullFileState.State.FULL));
        assertEquals(FullFileState.State.FULL, st.state());
    }

    public void testGetAndSetClosedReturnsPrevious() {
        FullFileState st = newState();
        st.casState(FullFileState.State.BLOCK, FullFileState.State.PROMOTING);
        st.casState(FullFileState.State.PROMOTING, FullFileState.State.FULL);
        assertEquals(FullFileState.State.FULL, st.getAndSetClosed());
        assertEquals(FullFileState.State.CLOSED, st.state());
    }

    public void testHandleCount() {
        FullFileState st = newState();
        assertEquals(1, st.incHandles());
        assertEquals(2, st.incHandles());
        assertEquals(1, st.decHandles());
        assertEquals(0, st.decHandles());
        assertEquals(0, st.handleCount());
    }

    public void testRetryAt() {
        FullFileState st = newState();
        st.setRetryAt(12345L);
        assertEquals(12345L, st.retryAt());
    }

    public void testFileLengthAccessor() {
        FullFileState st = newState();
        assertEquals(1_700_000_000L, st.fileLength());
    }

    // ---------------- deferred close ----------------

    /** Records tasks instead of running them, so a test can prove the cleanup never runs on the calling thread. */
    private static final class RecordingExecutor implements java.util.concurrent.Executor {
        final java.util.List<Runnable> tasks = new java.util.concurrent.CopyOnWriteArrayList<>();

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
        final org.opensearch.index.store.remote.filecache.FileCache fileCache = org.opensearch.index.store.remote.filecache.FileCacheFactory
            .createConcurrentLRUFileCache(1_000_000, 1);
        final Path path = Path.of("/cache/_5.faiss");
        final org.apache.lucene.store.IndexInput master = org.mockito.Mockito.mock(org.apache.lucene.store.IndexInput.class);
        final RecordingExecutor executor = new RecordingExecutor();
        final java.util.concurrent.atomic.AtomicInteger budgetReleases = new java.util.concurrent.atomic.AtomicInteger();
        final java.util.concurrent.atomic.AtomicInteger stuckReports = new java.util.concurrent.atomic.AtomicInteger();
        final FullFileState st;

        Fixture() {
            // The family's reference on the whole-file entry (put takes one, as the manager's get() does).
            fileCache.put(path, new org.opensearch.index.store.remote.filecache.FileCache.RestoredCachedIndexInput(10));
            st = new FullFileState(null, null, null, fileCache, "_5.faiss", 10, path);
        }

        void goFull() {
            st.attachCleanup(executor, budgetReleases::incrementAndGet, stuckReports::incrementAndGet);
            st.setLocalMaster(master);
            assertTrue(st.casState(FullFileState.State.BLOCK, FullFileState.State.PROMOTING));
            assertTrue(st.casState(FullFileState.State.PROMOTING, FullFileState.State.FULL));
        }
    }

    public void testCloseWhileFullWithNoHandlesReleasesBudgetAndDefersCleanupOffThread() throws Exception {
        Fixture f = new Fixture();
        f.goFull();

        f.st.closeFamily();

        assertEquals(FullFileState.State.CLOSED, f.st.state());
        assertEquals("budget released at close", 1, f.budgetReleases.get());
        assertEquals("cleanup scheduled once", 1, f.executor.tasks.size());
        // Not run inline: the caller may be the FileCache removal listener holding a segment lock.
        org.mockito.Mockito.verify(f.master, org.mockito.Mockito.never()).close();
        assertEquals(Integer.valueOf(1), f.fileCache.getRef(f.path));
        assertFalse(f.st.cleanupDone());

        f.executor.runAll();
        org.mockito.Mockito.verify(f.master).close();
        assertEquals("family reference released", Integer.valueOf(0), f.fileCache.getRef(f.path));
        assertTrue(f.st.cleanupDone());
        assertNull(f.st.localMaster());
        assertEquals("no stuck-handle report without handles", 0, f.stuckReports.get());
    }

    public void testCloseWhileFullWaitsForLastHandle() throws Exception {
        Fixture f = new Fixture();
        f.goFull();
        f.st.incHandles();
        f.st.incHandles();

        f.st.closeFamily();
        assertEquals(1, f.budgetReleases.get());
        assertEquals("reported: closed with live handles", 1, f.stuckReports.get());
        assertTrue("no cleanup while handles are open", f.executor.tasks.isEmpty());

        f.st.decHandles();
        assertTrue("one handle still open", f.executor.tasks.isEmpty());

        f.st.decHandles();
        assertEquals("last close schedules the cleanup", 1, f.executor.tasks.size());
        f.executor.runAll();
        org.mockito.Mockito.verify(f.master).close();
        assertEquals(Integer.valueOf(0), f.fileCache.getRef(f.path));
    }

    public void testCloseWhileBlockOrPromotingOwesNoCleanup() throws Exception {
        // BLOCK
        Fixture blocked = new Fixture();
        blocked.st.closeFamily();
        assertEquals(FullFileState.State.CLOSED, blocked.st.state());
        assertEquals(0, blocked.budgetReleases.get());
        assertTrue(blocked.executor.tasks.isEmpty());

        // PROMOTING with the teardown already attached (closed between attach and the CAS to FULL): the manager's
        // completion handler sees the lost CAS and unwinds; closeFamily must not ALSO clean up (double close/decRef).
        Fixture promoting = new Fixture();
        promoting.st.attachCleanup(promoting.executor, promoting.budgetReleases::incrementAndGet, () -> {});
        promoting.st.setLocalMaster(promoting.master);
        assertTrue(promoting.st.casState(FullFileState.State.BLOCK, FullFileState.State.PROMOTING));
        promoting.st.incHandles();
        promoting.st.closeFamily();
        assertFalse("CAS to FULL must now fail", promoting.st.casState(FullFileState.State.PROMOTING, FullFileState.State.FULL));
        promoting.st.decHandles();
        assertEquals(0, promoting.budgetReleases.get());
        assertTrue(promoting.executor.tasks.isEmpty());
        org.mockito.Mockito.verify(promoting.master, org.mockito.Mockito.never()).close();
    }

    public void testRepeatedCloseFamilyIsIdempotent() throws Exception {
        Fixture f = new Fixture();
        f.goFull();
        f.st.closeFamily();
        f.st.closeFamily();
        assertEquals(1, f.budgetReleases.get());
        assertEquals(1, f.executor.tasks.size());
    }

    public void testConcurrentCloseAndLastHandleCloseCleansUpExactlyOnce() throws Exception {
        for (int i = 0; i < 300; i++) {
            Fixture f = new Fixture();
            f.goFull();
            f.st.incHandles();
            java.util.concurrent.CyclicBarrier barrier = new java.util.concurrent.CyclicBarrier(2);
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
            assertEquals("iteration " + i + ": budget released exactly once", 1, f.budgetReleases.get());
        }
    }

    private static void await(java.util.concurrent.CyclicBarrier barrier) {
        try {
            barrier.await();
        } catch (Exception e) {
            throw new AssertionError(e);
        }
    }
}

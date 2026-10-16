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

    public void testCompareAndSetLocalMasterInstallsOnceAndClearsOnlyTheInstalledMapping() throws Exception {
        FullFileState st = newState();
        try (
            IndexInput first = new ByteArrayIndexInput("first", new byte[4]);
            IndexInput second = new ByteArrayIndexInput("second", new byte[4])
        ) {
            assertTrue("first publish installs", st.compareAndSetLocalMaster(null, first));
            assertFalse("second publish is rejected while one is installed", st.compareAndSetLocalMaster(null, second));
            assertSame(first, st.localMaster());
            assertFalse("a handler cannot clear a mapping it did not install", st.compareAndSetLocalMaster(second, null));
            assertSame(first, st.localMaster());
            assertTrue("the installer can clear its own mapping", st.compareAndSetLocalMaster(first, null));
            assertNull(st.localMaster());
        }
    }

    public void testRemoteModeIsOffUntilMarkedAndThenFinal() {
        FullFileState st = newState();
        assertFalse("a new family is not promotable until marked remote", st.isRemoteMode());
        st.markRemoteMode();
        assertTrue(st.isRemoteMode());
        st.markRemoteMode();
        assertTrue("marking is idempotent and never resets", st.isRemoteMode());
        assertEquals("remote mode is independent of the promotion state", FullFileState.State.BLOCK, st.state());
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
        final FullFileState st;

        Fixture() {
            // The family's reference on the whole-file entry (put takes one, as the manager's get() does).
            fileCache.put(path, new FileCache.RestoredCachedIndexInput(10));
            st = new FullFileState(null, null, fileCache, "_5.faiss", 10, path);
        }

        void goFull() {
            st.attachCleanup(executor, budgetReleases::incrementAndGet, stuckReports::incrementAndGet);
            assertTrue(st.compareAndSetLocalMaster(null, master));
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
        verify(f.master, never()).close();
        assertEquals(Integer.valueOf(1), f.fileCache.getRef(f.path));
        assertFalse(f.st.cleanupDone());

        f.executor.runAll();
        verify(f.master).close();
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
        verify(f.master).close();
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
        assertTrue(promoting.st.compareAndSetLocalMaster(null, promoting.master));
        assertTrue(promoting.st.casState(FullFileState.State.BLOCK, FullFileState.State.PROMOTING));
        promoting.st.incHandles();
        promoting.st.closeFamily();
        assertFalse("CAS to FULL must now fail", promoting.st.casState(FullFileState.State.PROMOTING, FullFileState.State.FULL));
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
        assertEquals(1, f.budgetReleases.get());
        assertEquals(1, f.executor.tasks.size());
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
            assertEquals("iteration " + i + ": budget released exactly once", 1, f.budgetReleases.get());
        }
    }

    private static void await(CyclicBarrier barrier) {
        try {
            barrier.await();
        } catch (Exception e) {
            throw new AssertionError(e);
        }
    }
}

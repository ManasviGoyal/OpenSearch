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
import org.opensearch.common.settings.Settings;
import org.opensearch.common.util.concurrent.OpenSearchExecutors;
import org.opensearch.common.util.concurrent.OpenSearchThreadPoolExecutor;
import org.opensearch.common.util.concurrent.ThreadContext;
import org.opensearch.index.store.remote.file.CleanerDaemonThreadLeakFilter;
import org.opensearch.index.store.remote.filecache.FileCache;
import org.opensearch.index.store.remote.filecache.FileCacheFactory;
import org.opensearch.search.internal.SearchContext;
import org.opensearch.test.OpenSearchTestCase;

import java.nio.file.Path;
import java.util.concurrent.TimeUnit;

import static org.mockito.Mockito.mock;

@ThreadLeakFilters(filters = CleanerDaemonThreadLeakFilter.class)
public class FullFileSearchPhaseTrackerTests extends OpenSearchTestCase {

    private ThreadContext threadContext;
    private FullFileSearchPhaseTracker tracker;

    @Override
    public void setUp() throws Exception {
        super.setUp();
        threadContext = new ThreadContext(Settings.EMPTY);
        tracker = new FullFileSearchPhaseTracker(threadContext);
    }

    public void testInPhaseFollowsEnterAndExitOnTheCallingThread() throws Exception {
        assertFalse(tracker.inPhase());
        tracker.enter();
        assertTrue(tracker.inPhase());
        // A slice executed inline nests in its query phase.
        tracker.enter();
        tracker.exit();
        assertTrue("still inside the outer phase", tracker.inPhase());
        tracker.exit();
        assertFalse(tracker.inPhase());

        // A thread that did not get this thread's context is not inside its phase.
        tracker.enter();
        boolean[] seen = new boolean[1];
        Thread other = new Thread(() -> seen[0] = tracker.inPhase());
        other.start();
        other.join();
        assertFalse(seen[0]);
        tracker.exit();
    }

    public void testATaskForkedFromAPhaseIsInsideIt() throws Exception {
        // The pools copy the caller's context onto each task, so a slice or a k-NN per-leaf task sees the phase.
        OpenSearchThreadPoolExecutor forked = newForkPool();
        try {
            assertFalse(forked.submit(tracker::inPhase).get());
            tracker.enter();
            assertTrue(forked.submit(tracker::inPhase).get());

            // A slice reported on the forked thread nests in the same phase.
            forked.submit(tracker::enter).get();
            forked.submit(tracker::exit).get();
            assertTrue(tracker.inPhase());

            tracker.exit();
            assertFalse(tracker.inPhase());
            assertFalse(forked.submit(tracker::inPhase).get());
        } finally {
            forked.shutdownNow();
            forked.awaitTermination(10, TimeUnit.SECONDS);
        }
    }

    public void testTheScopeIsReusedByLaterPhasesOnTheSameContext() {
        tracker.enter();
        tracker.exit();
        // The transient stays on the context once put; a later phase reuses it instead of putting a second one.
        assertNotNull(threadContext.getTransient(FullFileSearchPhaseTracker.SCOPE_TRANSIENT));
        assertFalse(tracker.inPhase());
        tracker.enter();
        assertTrue(tracker.inPhase());
        tracker.exit();
        assertFalse(tracker.inPhase());
    }

    public void testExitWithoutEnterIsIgnored() {
        tracker.exit();
        assertFalse(tracker.inPhase());
        tracker.enter();
        tracker.exit();
        tracker.exit();
        assertFalse(tracker.inPhase());
    }

    public void testListenerHooksEnterAndExitAPhase() {
        SearchContext context = mock(SearchContext.class);

        tracker.onPreQueryPhase(context);
        assertTrue(tracker.inPhase());
        tracker.onPreSliceExecution(context);
        tracker.onSliceExecution(context);
        assertTrue(tracker.inPhase());
        tracker.onQueryPhase(context, 1L);
        assertFalse(tracker.inPhase());

        tracker.onPreFetchPhase(context);
        assertTrue(tracker.inPhase());
        tracker.onFailedFetchPhase(context);
        assertFalse(tracker.inPhase());

        tracker.onPreSliceExecution(context);
        tracker.onFailedSliceExecution(context);
        tracker.onPreQueryPhase(context);
        tracker.onFailedQueryPhase(context);
        assertFalse(tracker.inPhase());
    }

    // ---------------- holds ----------------

    /** A FULL family whose whole file sits in the cache unreferenced, as the manager leaves it. */
    private static final class Promoted {
        final FileCache fileCache = FileCacheFactory.createConcurrentLRUFileCache(1_000_000, 1);
        final Path path = createTempDir().resolve("_5.faiss");
        final FullFileState st = new FullFileState(null, null, fileCache, "_5.faiss", 10, path);

        Promoted() {
            assertTrue(st.onAccess(0));
            PromotedWholeFileEntry entry = new PromotedWholeFileEntry(fileCache, path, mock(IndexInput.class), st);
            assertEquals(
                FullFileState.Completion.FULL,
                st.completePromotion(entry, Runnable::run, () -> false, () -> {}, () -> {}, () -> {}, () -> {})
            );
            fileCache.put(path, entry);
            fileCache.decRef(path);
        }

        int refs() {
            Integer refs = fileCache.getRef(path);
            return refs == null ? -1 : refs;
        }
    }

    public void testHoldsViewOnlyInsideAPhaseAndTakesTheHoldOnce() {
        Promoted p = new Promoted();
        assertFalse("outside a phase", tracker.holdsView(p.st));
        assertEquals(0, p.refs());
        assertEquals(0, p.st.inFlightScopes());

        tracker.enter();
        assertTrue(tracker.holdsView(p.st));
        assertTrue("the same scope asks again", tracker.holdsView(p.st));
        assertEquals("one reference for the scope", 1, p.refs());
        assertEquals(1, p.st.inFlightScopes());

        tracker.enter(); // a slice inside the phase
        assertTrue(tracker.holdsView(p.st));
        tracker.exit();
        assertEquals("the hold lasts to the end of the outermost phase", 1, p.refs());

        tracker.exit();
        assertEquals("given back when the scope ends", 0, p.refs());
        assertEquals(0, p.st.inFlightScopes());
        assertFalse(tracker.holdsView(p.st));
    }

    public void testAForkedTaskSharesTheScopesHold() throws Exception {
        Promoted p = new Promoted();
        OpenSearchThreadPoolExecutor forked = newForkPool();
        try {
            tracker.enter();
            assertTrue("the fork takes the hold for the scope", forked.submit(() -> tracker.holdsView(p.st)).get());
            assertTrue("the owner finds it held", tracker.holdsView(p.st));
            assertEquals("one hold for the scope, not one per thread", 1, p.refs());
            assertEquals(1, p.st.inFlightScopes());
            tracker.exit();
            assertEquals(0, p.refs());
            assertEquals(0, p.st.inFlightScopes());
        } finally {
            forked.shutdownNow();
            forked.awaitTermination(10, TimeUnit.SECONDS);
        }
    }

    public void testAScopeThatFoundTheFileGoneReadsBlocksForItsLength() {
        Promoted p = new Promoted();
        p.fileCache.remove(p.path); // the removal listener demotes the family
        tracker.enter();
        assertFalse(tracker.holdsView(p.st));
        assertEquals("nothing to give back", 0, p.st.inFlightScopes());
        assertFalse(tracker.holdsView(p.st));
        tracker.exit();
    }

    public void testANewScopeTakesANewHold() {
        Promoted p = new Promoted();
        tracker.enter();
        assertTrue(tracker.holdsView(p.st));
        tracker.exit();
        tracker.enter();
        assertTrue(tracker.holdsView(p.st));
        assertEquals(1, p.refs());
        tracker.exit();
        assertEquals(0, p.refs());
    }

    private OpenSearchThreadPoolExecutor newForkPool() {
        return OpenSearchExecutors.newFixed("forked", 1, 10, OpenSearchExecutors.daemonThreadFactory("forked"), threadContext);
    }
}

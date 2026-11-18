/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.storage.warmfullfile;

import org.opensearch.common.settings.Settings;
import org.opensearch.common.util.concurrent.OpenSearchExecutors;
import org.opensearch.common.util.concurrent.OpenSearchThreadPoolExecutor;
import org.opensearch.common.util.concurrent.ThreadContext;
import org.opensearch.search.internal.SearchContext;
import org.opensearch.test.OpenSearchTestCase;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import static org.mockito.Mockito.mock;

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
        assertEquals(1, tracker.inFlight());
        // A slice executed inline nests in its query phase.
        tracker.enter();
        assertEquals(2, tracker.inFlight());
        tracker.exit();
        assertTrue("still inside the outer phase", tracker.inPhase());
        tracker.exit();
        assertFalse(tracker.inPhase());
        assertEquals(0, tracker.inFlight());

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
        OpenSearchThreadPoolExecutor forked = OpenSearchExecutors.newFixed(
            "forked",
            1,
            10,
            OpenSearchExecutors.daemonThreadFactory("forked"),
            threadContext
        );
        try {
            assertFalse(forked.submit(tracker::inPhase).get());
            tracker.enter();
            assertTrue(forked.submit(tracker::inPhase).get());

            // A slice reported on the forked thread nests in the same phase.
            forked.submit(tracker::enter).get();
            assertEquals(2, tracker.inFlight());
            forked.submit(tracker::exit).get();
            assertEquals(1, tracker.inFlight());
            assertTrue(tracker.inPhase());

            tracker.exit();
            assertFalse(tracker.inPhase());
            assertFalse(forked.submit(tracker::inPhase).get());
            assertEquals(0, tracker.inFlight());
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
        assertEquals(0, tracker.inFlight());
    }

    public void testExitWithoutEnterIsIgnored() {
        tracker.exit();
        assertFalse(tracker.inPhase());
        assertEquals(0, tracker.inFlight());
        tracker.enter();
        tracker.exit();
        tracker.exit();
        assertFalse(tracker.inPhase());
        assertEquals(0, tracker.inFlight());
    }

    public void testActionRunsAtOnceWhenNoPhaseIsRunning() {
        AtomicInteger runs = new AtomicInteger();
        tracker.whenQuiesced(tracker.nextGeneration(), runs::incrementAndGet);
        assertEquals(1, runs.get());
    }

    public void testActionWaitsForThePhasesRunningAtTheGenerationAndNotForLaterOnes() throws Exception {
        AtomicInteger runs = new AtomicInteger();

        // A phase running before the generation is taken.
        CountDownLatch earlyEntered = new CountDownLatch(1);
        CountDownLatch earlyMayExit = new CountDownLatch(1);
        Thread early = new Thread(() -> {
            tracker.enter();
            earlyEntered.countDown();
            await(earlyMayExit);
            tracker.exit();
        });
        early.start();
        earlyEntered.await();

        long generation = tracker.nextGeneration();
        tracker.whenQuiesced(generation, runs::incrementAndGet);
        assertEquals("the early phase still runs", 0, runs.get());

        // A phase started after the generation is not waited for.
        CountDownLatch lateEntered = new CountDownLatch(1);
        CountDownLatch lateMayExit = new CountDownLatch(1);
        Thread late = new Thread(() -> {
            tracker.enter();
            lateEntered.countDown();
            await(lateMayExit);
            tracker.exit();
        });
        late.start();
        lateEntered.await();
        assertEquals(2, tracker.inFlight());

        earlyMayExit.countDown();
        early.join();
        assertEquals("released by the end of the early phase, while the late one still runs", 1, runs.get());

        lateMayExit.countDown();
        late.join();
        assertEquals("the late phase's end runs nothing more", 1, runs.get());
        assertEquals(0, tracker.inFlight());
    }

    public void testEveryWaiterOfAGenerationRunsOnceAndInOrder() throws Exception {
        List<Integer> runs = new ArrayList<>();
        CountDownLatch entered = new CountDownLatch(1);
        CountDownLatch mayExit = new CountDownLatch(1);
        Thread phase = new Thread(() -> {
            tracker.enter();
            entered.countDown();
            await(mayExit);
            tracker.exit();
        });
        phase.start();
        entered.await();
        tracker.whenQuiesced(tracker.nextGeneration(), () -> runs.add(1));
        tracker.whenQuiesced(tracker.nextGeneration(), () -> runs.add(2));
        assertTrue(runs.isEmpty());
        mayExit.countDown();
        phase.join();
        assertEquals(List.of(1, 2), runs);
    }

    public void testListenerHooksEnterAndExitAPhase() {
        SearchContext context = mock(SearchContext.class);

        tracker.onPreQueryPhase(context);
        assertTrue(tracker.inPhase());
        tracker.onPreSliceExecution(context);
        assertEquals(2, tracker.inFlight());
        tracker.onSliceExecution(context);
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
        assertEquals(0, tracker.inFlight());
    }

    private static void await(CountDownLatch latch) {
        try {
            latch.await();
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new AssertionError(e);
        }
    }
}

/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.storage.warmfullfile;

import org.opensearch.search.internal.SearchContext;
import org.opensearch.test.OpenSearchTestCase;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicInteger;

import static org.mockito.Mockito.mock;

public class SearchPhaseTrackerTests extends OpenSearchTestCase {

    public void testInPhaseFollowsEnterAndExitOnTheCallingThread() throws Exception {
        SearchPhaseTracker tracker = new SearchPhaseTracker();
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

        // Another thread is not inside this thread's phase.
        tracker.enter();
        boolean[] seen = new boolean[1];
        Thread other = new Thread(() -> seen[0] = tracker.inPhase());
        other.start();
        other.join();
        assertFalse(seen[0]);
        tracker.exit();
    }

    public void testExitWithoutEnterIsIgnored() {
        SearchPhaseTracker tracker = new SearchPhaseTracker();
        tracker.exit();
        assertFalse(tracker.inPhase());
        assertEquals(0, tracker.inFlight());
    }

    public void testActionRunsAtOnceWhenNoPhaseIsRunning() {
        SearchPhaseTracker tracker = new SearchPhaseTracker();
        AtomicInteger runs = new AtomicInteger();
        tracker.whenQuiesced(tracker.nextGeneration(), runs::incrementAndGet);
        assertEquals(1, runs.get());
    }

    public void testActionWaitsForThePhasesRunningAtTheGenerationAndNotForLaterOnes() throws Exception {
        SearchPhaseTracker tracker = new SearchPhaseTracker();
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
        SearchPhaseTracker tracker = new SearchPhaseTracker();
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
        SearchPhaseTracker tracker = new SearchPhaseTracker();
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

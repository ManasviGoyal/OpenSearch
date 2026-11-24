/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.storage.warmfullfile;

import org.opensearch.common.lease.Releasable;
import org.opensearch.test.OpenSearchTestCase;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicInteger;

public class ShardUseTrackerTests extends OpenSearchTestCase {

    public void testWhenDrainedRunsAtOnceWithNoHoldOpen() {
        ShardUseTracker tracker = new ShardUseTracker();
        AtomicInteger ran = new AtomicInteger();

        tracker.whenDrained(ran::incrementAndGet);

        assertEquals(1, ran.get());
        assertEquals(0, tracker.openHolds());
    }

    public void testWhenDrainedWaitsForTheHoldsOpenAtTheCallAndNotForLaterOnes() {
        // A release waits for the queries that were running when it was asked for. A query that starts afterwards
        // reads blocks, so it does not delay the release.
        ShardUseTracker tracker = new ShardUseTracker();
        PhaseHold first = tracker.acquire();
        PhaseHold second = tracker.acquire();
        AtomicInteger ran = new AtomicInteger();

        tracker.whenDrained(ran::incrementAndGet);
        PhaseHold later = tracker.acquire();
        assertEquals(3, tracker.openHolds());
        assertEquals(0, ran.get());

        first.close();
        assertEquals("one of the two is still open", 0, ran.get());
        second.close();
        assertEquals("both closed, the later hold does not count", 1, ran.get());

        later.close();
        assertEquals(1, ran.get());
        assertEquals(0, tracker.openHolds());
    }

    public void testActionsRunInTheOrderTheyWereAskedFor() {
        // Two releases asked for at different times: each waits for its own holds and every older one, so the older
        // release never runs after the newer.
        ShardUseTracker tracker = new ShardUseTracker();
        List<String> order = new ArrayList<>();
        PhaseHold old = tracker.acquire();
        tracker.whenDrained(() -> order.add("first"));
        PhaseHold newer = tracker.acquire();
        tracker.whenDrained(() -> order.add("second"));

        newer.close();
        assertEquals("the older hold is still open", List.of(), order);
        old.close();
        assertEquals(List.of("first", "second"), order);

        tracker.whenDrained(() -> order.add("third"));
        assertEquals(List.of("first", "second", "third"), order);
    }

    public void testAHoldIsOpenUntilItsOwnCountAndEveryForkHaveClosed() {
        // A task forked from a query phase keeps the shard held after the phase itself has ended.
        ShardUseTracker tracker = new ShardUseTracker();
        PhaseHold hold = tracker.acquire();
        Releasable fork = hold.tryFork();
        Releasable another = hold.tryFork();
        assertNotNull(fork);
        assertNotNull(another);
        AtomicInteger ran = new AtomicInteger();
        tracker.whenDrained(ran::incrementAndGet);

        hold.close();
        hold.close();
        assertEquals("closing twice is closing once", 1, tracker.openHolds());
        fork.close();
        fork.close();
        assertEquals(0, ran.get());
        another.close();

        assertEquals(1, ran.get());
        assertEquals(0, tracker.openHolds());
        assertNull("no fork of a hold that is back with the tracker", hold.tryFork());
    }

    public void testAHoldKnowsItsTracker() {
        ShardUseTracker tracker = new ShardUseTracker();
        try (PhaseHold hold = tracker.acquire()) {
            assertSame(tracker, hold.tracker());
        }
    }

    public void testConcurrentHoldsForksAndDrainsLoseNothing() throws Exception {
        ShardUseTracker tracker = new ShardUseTracker();
        int threads = 8;
        int rounds = 2_000;
        AtomicInteger asked = new AtomicInteger();
        AtomicInteger ran = new AtomicInteger();
        CountDownLatch start = new CountDownLatch(1);
        List<Thread> workers = new ArrayList<>();
        for (int t = 0; t < threads; t++) {
            Thread worker = new Thread(() -> {
                try {
                    start.await();
                } catch (InterruptedException e) {
                    throw new AssertionError(e);
                }
                for (int i = 0; i < rounds; i++) {
                    PhaseHold hold = tracker.acquire();
                    Releasable fork = hold.tryFork();
                    if (i % 7 == 0) {
                        asked.incrementAndGet();
                        tracker.whenDrained(ran::incrementAndGet);
                    }
                    hold.close();
                    fork.close();
                }
            });
            workers.add(worker);
            worker.start();
        }
        start.countDown();
        for (Thread worker : workers) {
            worker.join();
        }

        assertEquals(0, tracker.openHolds());
        assertEquals("every action ran exactly once", asked.get(), ran.get());
    }
}

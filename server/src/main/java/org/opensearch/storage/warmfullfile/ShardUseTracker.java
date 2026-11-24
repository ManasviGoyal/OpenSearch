/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.storage.warmfullfile;

import org.opensearch.common.annotation.ExperimentalApi;

import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Deque;
import java.util.List;

/**
 * Counts the query phases that are open on one warm shard, so that a whole file of the shard is unmapped only once
 * no query that could hold a view of it is still running. One tracker per shard; a query phase takes one
 * {@link PhaseHold} whatever number of whole files it reads.
 *
 * <p>Holds are counted in generations. {@link #whenDrained} closes the current generation to new holds and runs its
 * action once that generation and every older one have no hold left, so a release waits only for the queries that
 * were open when it was asked for, never for one that started later.
 *
 * <p>Lock order: a file cache segment's lock, then a file's lock, then this tracker's lock. The tracker's lock is a
 * leaf: nothing is called while it is held, and a drained action runs after it is given back.
 *
 * @opensearch.experimental
 */
@ExperimentalApi
public final class ShardUseTracker {

    /** The holds taken between two {@link #whenDrained} calls, and the actions waiting for them and all older holds. */
    static final class Generation {
        private int holds;
        private final List<Runnable> waiting = new ArrayList<>();
    }

    private final Object lock = new Object();
    // Closed generations that still have a hold, or wait behind an older one that has; oldest first.
    private final Deque<Generation> draining = new ArrayDeque<>();
    private Generation current = new Generation();
    private int openHolds;

    /** Takes a hold in the current generation, for one query phase. */
    public PhaseHold acquire() {
        synchronized (lock) {
            current.holds++;
            openHolds++;
            return new PhaseHold(this, current);
        }
    }

    /**
     * Runs {@code action} once every hold open now, and every fork of one, has closed; at once, on the calling thread,
     * if none is open. Holds taken after this call do not delay it. The action runs outside the tracker's lock, on
     * the thread that closes the last hold, so it should only hand work to an executor.
     */
    public void whenDrained(Runnable action) {
        synchronized (lock) {
            if (current.holds > 0 || draining.isEmpty() == false) {
                current.waiting.add(action);
                draining.addLast(current);
                current = new Generation();
                return;
            }
        }
        action.run();
    }

    /** Gives back one hold of {@code generation}, and runs the actions that were waiting for the last of them. */
    void release(Generation generation) {
        List<Runnable> drained = null;
        synchronized (lock) {
            generation.holds--;
            openHolds--;
            while (draining.isEmpty() == false && draining.peekFirst().holds == 0) {
                Generation done = draining.pollFirst();
                if (drained == null) {
                    drained = new ArrayList<>();
                }
                drained.addAll(done.waiting);
            }
        }
        if (drained != null) {
            for (Runnable action : drained) {
                action.run();
            }
        }
    }

    /** @return the number of query phases holding this shard now */
    public int openHolds() {
        synchronized (lock) {
            return openHolds;
        }
    }
}

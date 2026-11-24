/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.storage.warmfullfile;

import org.opensearch.common.Nullable;
import org.opensearch.common.annotation.ExperimentalApi;
import org.opensearch.common.lease.Releasable;
import org.opensearch.common.lease.Releasables;

import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * One query phase's hold on its shard's {@link ShardUseTracker}: the phase's own count, plus one fork for every task
 * the phase hands to another thread. The hold is given back to the tracker when the phase has closed it and every
 * fork has closed too, so a task that outlives its query phase still keeps the shard's whole files mapped until it
 * returns.
 *
 * @opensearch.experimental
 */
@ExperimentalApi
public final class PhaseHold implements Releasable {

    private final ShardUseTracker tracker;
    private final ShardUseTracker.Generation generation;
    // The phase's own count and one per open fork; the hold is back with the tracker at zero.
    private final AtomicInteger counts = new AtomicInteger(1);
    private final AtomicBoolean closed = new AtomicBoolean();

    PhaseHold(ShardUseTracker tracker, ShardUseTracker.Generation generation) {
        this.tracker = tracker;
        this.generation = generation;
    }

    /** @return the tracker of the shard this hold is on */
    public ShardUseTracker tracker() {
        return tracker;
    }

    /**
     * Takes a fork of this hold for a task that runs on another thread. Never blocks.
     *
     * @return the fork, to close when the task returns; null once the hold and all its forks have closed
     */
    @Nullable
    public Releasable tryFork() {
        while (true) {
            int count = counts.get();
            if (count == 0) {
                return null;
            }
            if (counts.compareAndSet(count, count + 1)) {
                return Releasables.releaseOnce(this::release);
            }
        }
    }

    /** Closes the phase's own count. Closing twice is the same as closing once. */
    @Override
    public void close() {
        if (closed.compareAndSet(false, true)) {
            release();
        }
    }

    private void release() {
        if (counts.decrementAndGet() == 0) {
            tracker.release(generation);
        }
    }
}

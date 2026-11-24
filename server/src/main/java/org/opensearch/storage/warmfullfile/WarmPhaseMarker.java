/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.storage.warmfullfile;

import org.opensearch.common.Nullable;

import java.util.concurrent.atomic.AtomicReference;

/**
 * Says that the request a thread serves is inside a query phase of a warm shard. One per request, kept as a
 * transient of the {@code ThreadContext}: every copy of the request's context shares it, so the tasks a query forks
 * see its current state. A read is served from a whole file only while the marker is on for the file's shard.
 */
final class WarmPhaseMarker {

    private final AtomicReference<PhaseHold> hold = new AtomicReference<>();

    void switchOn(PhaseHold phaseHold) {
        hold.set(phaseHold);
    }

    /** Switches the marker off, if {@code phaseHold} is the hold it is on for. */
    void switchOff(PhaseHold phaseHold) {
        hold.compareAndSet(phaseHold, null);
    }

    /** @return the hold if the marker is on for {@code tracker}'s shard, else null */
    @Nullable
    PhaseHold holdFor(ShardUseTracker tracker) {
        PhaseHold current = hold.get();
        return current != null && current.tracker() == tracker ? current : null;
    }
}

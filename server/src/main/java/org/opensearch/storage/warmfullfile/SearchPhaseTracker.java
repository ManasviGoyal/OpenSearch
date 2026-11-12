/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.storage.warmfullfile;

import org.opensearch.common.annotation.ExperimentalApi;
import org.opensearch.index.shard.SearchOperationListener;
import org.opensearch.search.internal.SearchContext;

import java.util.ArrayDeque;
import java.util.Deque;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Tracks the search phases running on this node's warm shards, so a whole-file mapping can be released while its
 * reader stays open. A query phase, a fetch phase or one slice of a concurrent segment search is a phase; each runs on
 * one thread and is reported by the {@link SearchOperationListener} hooks on that thread. A whole-file view is cut
 * only by a thread inside a phase ({@link #inPhase()}), so once every phase that was running when a family was
 * demoted has ended, no view of its mapping can still be read, and the mapping can be unmapped.
 *
 * <p>Each phase is stamped with the generation current when it starts. {@link #whenQuiesced} takes a generation and
 * runs its action once no phase stamped at or before it is still running; phases that start later are not waited
 * for, because they were handed blocks, not views. The wait is therefore bounded by the longest phase in flight at
 * the demotion, on any shard, which is the length of one search.
 *
 * @opensearch.experimental
 */
@ExperimentalApi
public final class SearchPhaseTracker implements SearchOperationListener {

    private record Waiter(long generation, Runnable action) {
    }

    private final AtomicLong generation = new AtomicLong();
    // Every running phase, by its token, stamped with the generation it started under.
    private final Map<Object, Long> inFlight = new ConcurrentHashMap<>();
    private final List<Waiter> waiters = new CopyOnWriteArrayList<>();
    // The phases the current thread is inside, outermost first; a slice executed inline nests in its query phase.
    private final ThreadLocal<Deque<Object>> current = ThreadLocal.withInitial(ArrayDeque::new);

    @Override
    public void onPreQueryPhase(SearchContext searchContext) {
        enter();
    }

    @Override
    public void onQueryPhase(SearchContext searchContext, long tookInNanos) {
        exit();
    }

    @Override
    public void onFailedQueryPhase(SearchContext searchContext) {
        exit();
    }

    @Override
    public void onPreFetchPhase(SearchContext searchContext) {
        enter();
    }

    @Override
    public void onFetchPhase(SearchContext searchContext, long tookInNanos) {
        exit();
    }

    @Override
    public void onFailedFetchPhase(SearchContext searchContext) {
        exit();
    }

    @Override
    public void onPreSliceExecution(SearchContext searchContext) {
        enter();
    }

    @Override
    public void onSliceExecution(SearchContext searchContext) {
        exit();
    }

    @Override
    public void onFailedSliceExecution(SearchContext searchContext) {
        exit();
    }

    /** Records that the current thread has started a phase. */
    void enter() {
        Object token = new Object();
        inFlight.put(token, generation.get());
        current.get().addLast(token);
    }

    /** Records that the current thread has ended its innermost phase, and runs the actions that unblocks. */
    void exit() {
        Object token = current.get().pollLast();
        if (token == null) {
            return;
        }
        inFlight.remove(token);
        runQuiesced();
    }

    /** @return true while the current thread is inside a search phase, i.e. may be handed a whole-file view */
    public boolean inPhase() {
        return current.get().isEmpty() == false;
    }

    /**
     * Starts a new generation: phases that start from now on are stamped with it, and are not waited for by a
     * {@link #whenQuiesced} call for the generation returned.
     *
     * @return the generation the phases running now, and every earlier one, are stamped at or below
     */
    long nextGeneration() {
        return generation.getAndIncrement();
    }

    /**
     * Runs {@code action} once no phase stamped at or before {@code generation} is running: at once, on the calling
     * thread, if none is; otherwise on the search thread that ends the last of them.
     */
    void whenQuiesced(long generation, Runnable action) {
        waiters.add(new Waiter(generation, action));
        runQuiesced();
    }

    /** @return the number of phases running on this node */
    int inFlight() {
        return inFlight.size();
    }

    private void runQuiesced() {
        if (waiters.isEmpty()) {
            return;
        }
        long oldestRunning = Long.MAX_VALUE;
        for (long started : inFlight.values()) {
            oldestRunning = Math.min(oldestRunning, started);
        }
        for (Waiter waiter : waiters) {
            if (waiter.generation() < oldestRunning && waiters.remove(waiter)) {
                waiter.action().run();
            }
        }
    }
}

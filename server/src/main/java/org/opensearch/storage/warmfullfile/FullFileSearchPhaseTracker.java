/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.storage.warmfullfile;

import org.opensearch.common.annotation.ExperimentalApi;
import org.opensearch.common.util.concurrent.ThreadContext;
import org.opensearch.index.shard.SearchOperationListener;
import org.opensearch.search.internal.SearchContext;

import java.util.Deque;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedDeque;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Tracks the search phases running on this node's warm shards, so a whole-file mapping can be released while its
 * reader stays open. A query phase, a fetch phase or one slice of a concurrent segment search is a phase, reported by
 * the {@link SearchOperationListener} hooks. A whole-file view is cut only inside a phase ({@link #inPhase()}), so
 * once every phase that was running when a family was demoted has ended, no view of its mapping can still be read,
 * and the mapping can be unmapped.
 *
 * <p>The phases a request is inside are kept in a {@link Scope} stored as a transient of the {@link ThreadContext}.
 * The thread pools copy the context onto every task they run, so a task forked from a phase (a concurrent segment
 * search slice, or the per-leaf tasks k-NN submits through the searcher's task executor) shares its scope and is
 * inside the phase too. A thread whose context has no scope, such as a merge thread or a recovery thread, is outside
 * every phase and reads blocks.
 *
 * <p>Each phase is stamped with the generation current when it starts. {@link #whenQuiesced} takes a generation and
 * runs its action once no phase stamped at or before it is still running; phases that start later are not waited
 * for, because they were handed blocks, not views. The wait is therefore bounded by the longest phase in flight at
 * the demotion, on any shard, which is the length of one search.
 *
 * @opensearch.experimental
 */
@ExperimentalApi
public final class FullFileSearchPhaseTracker implements SearchOperationListener {

    /** The {@link ThreadContext} transient under which a request's {@link Scope} is kept. */
    static final String SCOPE_TRANSIENT = "warm_full_file.search_scope";

    /**
     * The phases one request is inside, shared by every thread the request's context was copied to. Entering pushes
     * a token and exiting pops the most recent one; with slices on several threads a pop can take another slice's
     * token, which is safe because tokens are interchangeable except for their generation stamp, and the most recent
     * token never carries an older stamp than the phase that is ending.
     */
    static final class Scope {
        private final Deque<Object> tokens = new ConcurrentLinkedDeque<>();
    }

    private record Waiter(long generation, Runnable action) {
    }

    private final ThreadContext threadContext;
    private final AtomicLong generation = new AtomicLong();
    // Every running phase, by its token, stamped with the generation it started under.
    private final Map<Object, Long> inFlight = new ConcurrentHashMap<>();
    private final List<Waiter> waiters = new CopyOnWriteArrayList<>();

    public FullFileSearchPhaseTracker(ThreadContext threadContext) {
        this.threadContext = threadContext;
    }

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

    /** Records that the current context has started a phase. */
    void enter() {
        Scope scope = threadContext.getTransient(SCOPE_TRANSIENT);
        if (scope == null) {
            scope = new Scope();
            threadContext.putTransient(SCOPE_TRANSIENT, scope);
        }
        Object token = new Object();
        inFlight.put(token, generation.get());
        scope.tokens.addLast(token);
    }

    /** Records that the current context has ended its most recent phase, and runs the actions that unblocks. */
    void exit() {
        Scope scope = threadContext.getTransient(SCOPE_TRANSIENT);
        Object token = scope == null ? null : scope.tokens.pollLast();
        if (token == null) {
            return;
        }
        inFlight.remove(token);
        runQuiesced();
    }

    /** @return true while the current context is inside a search phase, i.e. may be handed a whole-file view */
    public boolean inPhase() {
        Scope scope = threadContext.getTransient(SCOPE_TRANSIENT);
        return scope != null && scope.tokens.isEmpty() == false;
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

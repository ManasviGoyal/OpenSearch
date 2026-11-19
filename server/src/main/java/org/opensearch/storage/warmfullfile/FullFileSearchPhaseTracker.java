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
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedDeque;

/**
 * Tracks the search phases running on this node's warm shards as scopes, so a promoted whole file is referenced
 * exactly while a search reads it. A query phase, a fetch phase or one slice of a concurrent segment search is a
 * phase, reported by the {@link SearchOperationListener} hooks. The phases a request is inside form its
 * {@link Scope}, stored as a transient of the {@link ThreadContext}: the thread pools copy the context onto every
 * task they run, so a task forked from a phase (a concurrent segment search slice, or the per-leaf tasks k-NN
 * submits through the searcher's task executor) shares the scope and is inside the phase too. A thread whose context
 * has no scope, such as a merge thread or a recovery thread, is outside every phase and reads blocks.
 *
 * <p>A whole-file view is cut only through {@link #holdsView}: the first view of a family in a scope takes one
 * reference on the whole-file cache entry and one in-flight count on the family, and the scope's end gives both
 * back. Between searches a promoted file is therefore an unreferenced cache entry, evicted like a block, and a
 * family demoted while a scope reads it unmaps only once that scope ends.
 *
 * @opensearch.experimental
 */
@ExperimentalApi
public final class FullFileSearchPhaseTracker implements SearchOperationListener {

    /** The {@link ThreadContext} transient under which a request's {@link Scope} is kept. */
    static final String SCOPE_TRANSIENT = "warm_full_file.search_scope";

    /**
     * The phases one request is inside and the whole files it holds, shared by every thread the request's context was
     * copied to. Entering pushes a token and exiting pops one; the scope ends when no token is left. With slices on
     * several threads a pop can take another slice's token, which is harmless because tokens are interchangeable.
     */
    static final class Scope {
        private final Deque<Object> tokens = new ConcurrentLinkedDeque<>();
        // The families this scope holds a view of; false for one whose file the cache had already dropped.
        private final Map<FullFileState, Boolean> held = new ConcurrentHashMap<>();
    }

    private final ThreadContext threadContext;

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
        scope.tokens.addLast(new Object());
    }

    /** Records that the current context has ended a phase; the end of its last phase gives back every view it holds. */
    void exit() {
        Scope scope = threadContext.getTransient(SCOPE_TRANSIENT);
        if (scope == null || scope.tokens.pollLast() == null) {
            return;
        }
        if (scope.tokens.isEmpty()) {
            for (Map.Entry<FullFileState, Boolean> hold : scope.held.entrySet()) {
                if (hold.getValue()) {
                    hold.getKey().releaseView();
                }
            }
            scope.held.clear();
        }
    }

    /** @return true while the current context is inside a search phase */
    public boolean inPhase() {
        Scope scope = threadContext.getTransient(SCOPE_TRANSIENT);
        return scope != null && scope.tokens.isEmpty() == false;
    }

    /**
     * Whether the current scope holds a view of {@code family}, taking it on the first call for the family in the
     * scope. Concurrent first calls from threads sharing the scope wait for the one that takes it, so no thread cuts
     * a view before the hold exists.
     *
     * @return true if the caller may cut a view of the family's mapping; false outside a phase, when the family is
     *         not FULL, or when the cache had dropped the file at the first call in this scope
     */
    public boolean holdsView(FullFileState family) {
        Scope scope = threadContext.getTransient(SCOPE_TRANSIENT);
        if (scope == null || scope.tokens.isEmpty()) {
            return false;
        }
        return scope.held.computeIfAbsent(family, FullFileState::acquireView);
    }
}

/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.storage.warmfullfile;

import org.opensearch.common.annotation.ExperimentalApi;
import org.opensearch.common.lease.Releasable;
import org.opensearch.common.lease.Releasables;
import org.opensearch.common.util.concurrent.ThreadContext;
import org.opensearch.index.shard.SearchOperationListener;
import org.opensearch.search.internal.SearchContext;

import java.util.Deque;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedDeque;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * Tracks the query phases running on this node's warm shards as scopes, so a promoted whole file is referenced
 * exactly while a query reads it. The {@link SearchOperationListener} hooks of the query phase open and close a
 * scope; no other phase does, so a fetch, a merge or a recovery is outside every scope and reads blocks. The phases
 * a request is inside form its {@link Scope}, stored as a transient of the {@link ThreadContext}: the thread pools
 * copy the context onto every task they run, so a task forked from a query phase (a concurrent segment search slice,
 * or the per-leaf tasks k-NN submits through the searcher's task executor) shares the scope and is inside the phase
 * too.
 *
 * <p>A whole-file view is cut only through {@link #holdsView}: the first view of a family in a scope takes one
 * reference on the whole-file cache entry and one in-flight count on the family, and the scope's end gives both
 * back. Between queries a promoted file is therefore an unreferenced cache entry, evicted like a block, and a
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
    // The cleanup of every query phase that is open, by its search context. The phase's post hook runs it; the search
    // context runs it too when it closes, which covers a phase whose post hook never ran.
    private final Map<SearchContext, Releasable> openPhases = new ConcurrentHashMap<>();

    public FullFileSearchPhaseTracker(ThreadContext threadContext) {
        this.threadContext = threadContext;
    }

    @Override
    public void onPreQueryPhase(SearchContext searchContext) {
        final Scope scope = scope();
        final AtomicBoolean entered = new AtomicBoolean();
        final Releasable cleanup = Releasables.releaseOnce(() -> {
            openPhases.remove(searchContext);
            if (entered.get()) {
                exit(scope);
            }
        });
        openPhases.put(searchContext, cleanup);
        try {
            searchContext.addReleasable(cleanup);
            // Entered last, so that a failure above never leaves the scope open.
            scope.tokens.addLast(new Object());
            entered.set(true);
        } catch (RuntimeException e) {
            openPhases.remove(searchContext);
            throw e;
        }
    }

    @Override
    public void onQueryPhase(SearchContext searchContext, long tookInNanos) {
        closePhase(searchContext);
    }

    @Override
    public void onFailedQueryPhase(SearchContext searchContext) {
        closePhase(searchContext);
    }

    private void closePhase(SearchContext searchContext) {
        Releasable cleanup = openPhases.get(searchContext);
        if (cleanup != null) {
            cleanup.close();
        }
    }

    /** The scope of the request this thread serves, created on first use. */
    private Scope scope() {
        Scope scope = threadContext.getTransient(SCOPE_TRANSIENT);
        if (scope == null) {
            scope = new Scope();
            threadContext.putTransient(SCOPE_TRANSIENT, scope);
        }
        return scope;
    }

    /** Enters a phase on this thread's request. */
    void enter() {
        scope().tokens.addLast(new Object());
    }

    /** Leaves a phase of this thread's request; a no-op outside every phase. */
    void exit() {
        Scope scope = threadContext.getTransient(SCOPE_TRANSIENT);
        if (scope != null) {
            exit(scope);
        }
    }

    /** Leaves a phase of {@code scope}; the last one out gives back every whole file the scope holds. */
    private void exit(Scope scope) {
        if (scope.tokens.pollLast() == null) {
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

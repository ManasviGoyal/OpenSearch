/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.storage.warmfullfile;

import org.apache.lucene.store.AlreadyClosedException;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.FilterDirectory;
import org.opensearch.common.Nullable;
import org.opensearch.common.annotation.ExperimentalApi;
import org.opensearch.common.lease.Releasable;
import org.opensearch.common.lease.Releasables;
import org.opensearch.common.util.concurrent.ThreadContext;
import org.opensearch.index.shard.IndexShard;
import org.opensearch.index.shard.SearchOperationListener;
import org.opensearch.index.store.Store;
import org.opensearch.search.internal.SearchContext;
import org.opensearch.storage.directory.TieredDirectory;

import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executor;

/**
 * Brackets each query phase of a warm shard: the phase takes a {@link PhaseHold} on the shard's
 * {@link ShardUseTracker} and switches a {@link WarmPhaseMarker} on in the {@link ThreadContext}, and gives both back
 * when it ends. A read is served from a whole file only under the marker, and a whole file is unmapped only once the
 * holds that were open on its shard have closed. No other phase is bracketed, so a fetch, a merge or a recovery reads
 * blocks.
 *
 * <p>The thread pools copy the context onto every task they run, so the tasks a query forks (the slices of a
 * concurrent segment search, or the per-leaf tasks a k-NN query submits through the searcher's task executor) see the
 * marker too. The query thread may stop waiting for such a task, for example when it is interrupted, and end its phase
 * while the task still reads. So the listener also wraps the searcher's executor: each task takes a fork of the
 * query's hold and keeps it until it returns. A query phase whose searcher was not given that wrapper gets no
 * marker, and reads blocks.
 *
 * @opensearch.experimental
 */
@ExperimentalApi
public final class WarmQueryPhaseListener implements SearchOperationListener {

    /** The {@link ThreadContext} transient under which a request's {@link WarmPhaseMarker} is kept. */
    static final String MARKER = "warm_full_file.marker";
    /** The {@link ThreadContext} transient under which the shards whose searcher executor was wrapped are kept. */
    static final String WRAPPED_SHARDS = "warm_full_file.wrapped_shards";

    // The hold a wrapped task runs under, on the thread that runs it; a task it forks in turn forks the same hold.
    private static final ThreadLocal<PhaseHold> RUNNING_TASK_HOLD = new ThreadLocal<>();

    private final ThreadContext threadContext;
    // The cleanup of every query phase that is open, by its search context. The phase's post hook runs it; the search
    // context runs it too when it closes, which covers a phase whose post hook never ran.
    private final Map<SearchContext, Releasable> openPhases = new ConcurrentHashMap<>();

    public WarmQueryPhaseListener(ThreadContext threadContext) {
        this.threadContext = threadContext;
    }

    @Override
    public void onPreQueryPhase(SearchContext searchContext) {
        final IndexShard shard = searchContext.indexShard();
        final ShardUseTracker tracker = trackerFor(shard);
        // Without the executor wrapper nothing keeps a forked task's shard held, so the phase reads blocks.
        if (tracker == null || executorWrapped(shard) == false) {
            return;
        }
        final PhaseHold hold = tracker.acquire();
        try {
            final WarmPhaseMarker marker = marker();
            final Releasable cleanup = Releasables.releaseOnce(() -> {
                marker.switchOff(hold);
                openPhases.remove(searchContext);
                hold.close();
            });
            openPhases.put(searchContext, cleanup);
            searchContext.addReleasable(cleanup);
            // Switched on last, so that a failure above never leaves it on.
            marker.switchOn(hold);
        } catch (RuntimeException e) {
            openPhases.remove(searchContext);
            hold.close();
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

    /**
     * Wraps the executor of a warm shard's searcher so that every task it is handed takes a fork of the hold of the
     * query phase that submits it, and gives the fork back when the task returns. A task submitted outside a query
     * phase, or after its phase and all its forks have ended, runs as it is and reads blocks.
     */
    @Override
    public Executor wrapSearcherExecutor(IndexShard shard, Executor delegate) {
        final ShardUseTracker tracker = trackerFor(shard);
        if (tracker == null) {
            return delegate;
        }
        wrappedShards().add(shard);
        return task -> {
            PhaseHold hold = RUNNING_TASK_HOLD.get();
            if (hold == null || hold.tracker() != tracker) {
                hold = holdOf(threadContext, tracker);
            }
            final Releasable fork = hold == null ? null : hold.tryFork();
            if (fork == null) {
                delegate.execute(task);
                return;
            }
            final PhaseHold owner = hold;
            try {
                delegate.execute(() -> {
                    try (fork) {
                        runAsTaskOf(owner, task);
                    }
                });
            } catch (RuntimeException e) {
                // Rejected: the caller runs the task itself, under the query's own hold.
                fork.close();
                throw e;
            }
        };
    }

    private static void runAsTaskOf(PhaseHold owner, Runnable task) {
        final PhaseHold previous = RUNNING_TASK_HOLD.get();
        RUNNING_TASK_HOLD.set(owner);
        try {
            task.run();
        } finally {
            if (previous == null) {
                RUNNING_TASK_HOLD.remove();
            } else {
                RUNNING_TASK_HOLD.set(previous);
            }
        }
    }

    /**
     * The hold of the query phase the calling thread's request is inside, if that phase is on {@code tracker}'s shard.
     *
     * @return the hold, or null outside such a phase
     */
    @Nullable
    static PhaseHold holdOf(ThreadContext threadContext, ShardUseTracker tracker) {
        final WarmPhaseMarker marker = threadContext.getTransient(MARKER);
        return marker == null ? null : marker.holdFor(tracker);
    }

    /** The request's marker, created on first use. */
    private WarmPhaseMarker marker() {
        WarmPhaseMarker marker = threadContext.getTransient(MARKER);
        if (marker == null) {
            marker = new WarmPhaseMarker();
            threadContext.putTransient(MARKER, marker);
        }
        return marker;
    }

    /** The shards whose searcher executor this request had wrapped, created on first use. */
    private Set<IndexShard> wrappedShards() {
        Set<IndexShard> shards = threadContext.getTransient(WRAPPED_SHARDS);
        if (shards == null) {
            shards = ConcurrentHashMap.newKeySet();
            threadContext.putTransient(WRAPPED_SHARDS, shards);
        }
        return shards;
    }

    /** Whether the searcher of this request's search context on {@code shard} was given the wrapped executor. */
    private boolean executorWrapped(IndexShard shard) {
        final Set<IndexShard> shards = threadContext.getTransient(WRAPPED_SHARDS);
        return shards != null && shards.contains(shard);
    }

    /** The tracker of a warm shard, or null for a shard that is not warm or whose store is closed. */
    @Nullable
    private static ShardUseTracker trackerFor(IndexShard shard) {
        if (shard == null) {
            return null;
        }
        final Store store = shard.store();
        if (store == null) {
            return null;
        }
        try {
            Directory directory = store.directory();
            while (directory != null) {
                if (directory instanceof TieredDirectory tiered) {
                    return tiered.shardUseTracker();
                }
                directory = directory instanceof FilterDirectory filter ? filter.getDelegate() : null;
            }
            return null;
        } catch (AlreadyClosedException e) {
            return null;
        }
    }
}

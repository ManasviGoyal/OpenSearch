/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.storage.warmfullfile;

import org.apache.lucene.store.ByteBuffersDirectory;
import org.apache.lucene.store.Directory;
import org.opensearch.common.lease.Releasable;
import org.opensearch.common.settings.Settings;
import org.opensearch.common.util.concurrent.ThreadContext;
import org.opensearch.index.shard.IndexShard;
import org.opensearch.index.store.Store;
import org.opensearch.search.internal.SearchContext;
import org.opensearch.storage.directory.TieredDirectory;
import org.opensearch.test.OpenSearchTestCase;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Executor;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.atomic.AtomicInteger;

import org.mockito.ArgumentCaptor;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class WarmQueryPhaseListenerTests extends OpenSearchTestCase {

    private final ThreadContext threadContext = new ThreadContext(Settings.EMPTY);
    private final WarmQueryPhaseListener listener = new WarmQueryPhaseListener(threadContext);
    private final ShardUseTracker tracker = new ShardUseTracker();

    /** A shard whose store sits on a tiered directory with {@link #tracker}. */
    private IndexShard warmShard() {
        TieredDirectory tiered = mock(TieredDirectory.class);
        when(tiered.shardUseTracker()).thenReturn(tracker);
        return shardOn(tiered);
    }

    private static IndexShard shardOn(Directory directory) {
        Store store = mock(Store.class);
        when(store.directory()).thenReturn(directory);
        IndexShard shard = mock(IndexShard.class);
        when(shard.store()).thenReturn(store);
        return shard;
    }

    private static SearchContext contextOn(IndexShard shard) {
        SearchContext context = mock(SearchContext.class);
        when(context.indexShard()).thenReturn(shard);
        return context;
    }

    /** An executor that keeps the tasks it is handed until the test runs them. */
    private static final class DeferringExecutor implements Executor {
        final List<Runnable> tasks = new ArrayList<>();

        @Override
        public void execute(Runnable task) {
            tasks.add(task);
        }
    }

    public void testAQueryPhaseHoldsItsShardAndSwitchesTheMarkerOn() {
        IndexShard shard = warmShard();
        SearchContext context = contextOn(shard);
        listener.wrapSearcherExecutor(shard, Runnable::run);

        listener.onPreQueryPhase(context);
        assertEquals(1, tracker.openHolds());
        PhaseHold hold = WarmQueryPhaseListener.holdOf(threadContext, tracker);
        assertNotNull("the marker is on for the shard", hold);
        assertSame(tracker, hold.tracker());
        assertNull("and for no other shard", WarmQueryPhaseListener.holdOf(threadContext, new ShardUseTracker()));

        listener.onQueryPhase(context, 1L);
        assertEquals(0, tracker.openHolds());
        assertNull(WarmQueryPhaseListener.holdOf(threadContext, tracker));

        listener.onPreQueryPhase(context);
        assertEquals(1, tracker.openHolds());
        listener.onFailedQueryPhase(context);
        assertEquals(0, tracker.openHolds());
        assertNull(WarmQueryPhaseListener.holdOf(threadContext, tracker));
    }

    public void testNoOtherPhaseHoldsTheShard() {
        IndexShard shard = warmShard();
        SearchContext context = contextOn(shard);
        listener.wrapSearcherExecutor(shard, Runnable::run);

        listener.onPreFetchPhase(context);
        assertEquals(0, tracker.openHolds());
        assertNull(WarmQueryPhaseListener.holdOf(threadContext, tracker));
        listener.onFetchPhase(context, 1L);
        listener.onPreSliceExecution(context);
        assertEquals(0, tracker.openHolds());
        listener.onSliceExecution(context);
    }

    public void testWithoutTheExecutorWrapperAQueryPhaseGetsNoMarker() {
        // Nothing keeps a forked task's shard held if the searcher was not given the wrapped executor, so the phase
        // takes no hold and reads blocks.
        SearchContext context = contextOn(warmShard());

        listener.onPreQueryPhase(context);

        assertEquals(0, tracker.openHolds());
        assertNull(WarmQueryPhaseListener.holdOf(threadContext, tracker));
        verify(context, never()).addReleasable(any());
        listener.onQueryPhase(context, 1L);
    }

    public void testAShardThatIsNotWarmIsLeftAlone() {
        IndexShard shard = shardOn(new ByteBuffersDirectory());
        SearchContext context = contextOn(shard);
        Executor executor = Runnable::run;

        assertSame("its searcher's executor is not wrapped", executor, listener.wrapSearcherExecutor(shard, executor));
        listener.onPreQueryPhase(context);
        verify(context, never()).addReleasable(any());
        listener.onQueryPhase(context, 1L);
    }

    public void testAQueryPhaseWhosePostHookNeverRanIsClosedWithItsSearchContext() {
        IndexShard shard = warmShard();
        SearchContext context = contextOn(shard);
        listener.wrapSearcherExecutor(shard, Runnable::run);
        ArgumentCaptor<Releasable> registered = ArgumentCaptor.forClass(Releasable.class);

        listener.onPreQueryPhase(context);
        verify(context).addReleasable(registered.capture());
        assertEquals(1, tracker.openHolds());

        registered.getValue().close();
        assertEquals(0, tracker.openHolds());
        assertNull(WarmQueryPhaseListener.holdOf(threadContext, tracker));

        listener.onQueryPhase(context, 1L);
        registered.getValue().close();
        assertEquals("run again, the cleanup does nothing", 0, tracker.openHolds());
    }

    public void testAFailureRegisteringTheCleanupLeavesNoHoldAndNoMarker() {
        IndexShard shard = warmShard();
        SearchContext context = contextOn(shard);
        listener.wrapSearcherExecutor(shard, Runnable::run);
        doThrow(new IllegalStateException("simulated")).when(context).addReleasable(any());

        expectThrows(IllegalStateException.class, () -> listener.onPreQueryPhase(context));

        assertEquals(0, tracker.openHolds());
        assertNull(WarmQueryPhaseListener.holdOf(threadContext, tracker));
    }

    public void testAForkedTaskKeepsTheShardHeldAfterItsQueryPhaseHasEnded() {
        // The query thread stops waiting for a task it forked, and its phase ends. The task still runs, so the shard
        // stays held until the task returns: a release asked for meanwhile waits for it.
        IndexShard shard = warmShard();
        SearchContext context = contextOn(shard);
        DeferringExecutor pool = new DeferringExecutor();
        Executor wrapped = listener.wrapSearcherExecutor(shard, pool);
        AtomicInteger ran = new AtomicInteger();
        AtomicInteger released = new AtomicInteger();

        listener.onPreQueryPhase(context);
        wrapped.execute(ran::incrementAndGet);
        assertEquals("handed on, not run yet", 1, pool.tasks.size());
        listener.onQueryPhase(context, 1L);

        assertNull("the marker is off: new reads get blocks", WarmQueryPhaseListener.holdOf(threadContext, tracker));
        assertEquals("the task's fork still holds the shard", 1, tracker.openHolds());
        tracker.whenDrained(released::incrementAndGet);
        assertEquals(0, released.get());

        pool.tasks.get(0).run();

        assertEquals(1, ran.get());
        assertEquals(0, tracker.openHolds());
        assertEquals("released once the task has returned", 1, released.get());
    }

    public void testATaskForkedByATaskForksTheSameHold() {
        // A forked task fans out again after its query phase has ended: the inner task holds the shard as well.
        IndexShard shard = warmShard();
        SearchContext context = contextOn(shard);
        DeferringExecutor pool = new DeferringExecutor();
        Executor wrapped = listener.wrapSearcherExecutor(shard, pool);
        AtomicInteger innerRan = new AtomicInteger();

        listener.onPreQueryPhase(context);
        wrapped.execute(() -> wrapped.execute(innerRan::incrementAndGet));
        listener.onQueryPhase(context, 1L);

        pool.tasks.get(0).run();
        assertEquals("the inner task was handed on", 2, pool.tasks.size());
        assertEquals("and holds the shard after the outer task returned", 1, tracker.openHolds());

        pool.tasks.get(1).run();
        assertEquals(1, innerRan.get());
        assertEquals(0, tracker.openHolds());
    }

    public void testATaskSubmittedOutsideAQueryPhaseRunsAsItIs() {
        IndexShard shard = warmShard();
        DeferringExecutor pool = new DeferringExecutor();
        Executor wrapped = listener.wrapSearcherExecutor(shard, pool);
        Runnable task = () -> {};

        wrapped.execute(task);

        assertSame("not wrapped: there is no hold to fork", task, pool.tasks.get(0));
        assertEquals(0, tracker.openHolds());
    }

    public void testARejectedTaskGivesItsForkBack() {
        // The pool rejects the task, and Lucene then runs it on the calling thread under the query's own hold. The
        // fork taken for it must not stay open.
        IndexShard shard = warmShard();
        SearchContext context = contextOn(shard);
        Executor rejecting = task -> { throw new RejectedExecutionException("simulated"); };
        Executor wrapped = listener.wrapSearcherExecutor(shard, rejecting);

        listener.onPreQueryPhase(context);
        expectThrows(RejectedExecutionException.class, () -> wrapped.execute(() -> {}));
        assertEquals("only the query's own hold", 1, tracker.openHolds());
        listener.onQueryPhase(context, 1L);

        assertEquals(0, tracker.openHolds());
    }
}

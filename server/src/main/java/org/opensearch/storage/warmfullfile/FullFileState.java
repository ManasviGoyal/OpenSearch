/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */
package org.opensearch.storage.warmfullfile;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.message.ParameterizedMessage;
import org.apache.lucene.store.FSDirectory;
import org.apache.lucene.store.IndexInput;
import org.opensearch.common.annotation.ExperimentalApi;
import org.opensearch.common.util.CancellableThreads;
import org.opensearch.index.store.RemoteSegmentStoreDirectory;
import org.opensearch.index.store.remote.filecache.CachedIndexInput;
import org.opensearch.index.store.remote.filecache.FileCache;
import org.opensearch.index.store.remote.utils.TransferManager;

import java.nio.file.Path;
import java.util.concurrent.Executor;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.BooleanSupplier;

/**
 * Per-file (family) whole-file promotion state, shared by every handle of the file: the original opened by
 * {@code TieredDirectory.openInput} and every clone and slice derived from it. One instance per
 * {@code PromotableCachedSwitchableIndexInput}, so a file is downloaded and mapped at most once however many readers
 * open it.
 *
 * <p>A promoted whole file is an ordinary file cache entry ({@link PromotedWholeFileEntry}) that nothing holds a
 * reference on, so the cache evicts it under pressure like a block, also while a query reads it. When the cache drops
 * the entry, for any reason, {@link #onEntryRemoved} demotes the family to blocks and owes the unmap. "In use" is
 * counted per shard, not per file: the unmap waits for every query phase that was open on the family's shard when the
 * family left FULL ({@link ShardUseTracker#whenDrained}), so a query that holds a view never has the mapping closed
 * under it. A read outside a query phase gets blocks.
 *
 * <p>Every transition is a named method of this class, taken under one lock: {@link #onAccess}, {@link #abandonPromotion},
 * {@link #completePromotion}, and the three ways out of FULL, {@link #onEntryRemoved}, {@link #demote} and
 * {@link #closeFamily}. The mapping is published and the family moves to {@link State#FULL} in one step, so a reader
 * that sees FULL sees the mapping. The three ways out share one path: the family stops handing out views, the release
 * is owed, and it runs once the shard's open query phases have ended, exactly once per FULL period, on the cleanup
 * executor and never on the cache listener thread. The release drops the cache entry, closes the mapping, deletes the file, and only then
 * gives back the bytes the cache still counted for it and the budget, so neither says the disk is free before it is.
 * Nothing takes a family back to FULL but a new promotion, which starts only after the release has run, so a file is
 * never mapped twice.
 *
 * <p>Lock order: a file cache segment's lock, then this family's lock, then the shard tracker's lock. The cache calls
 * {@link #onEntryRemoved} under its segment lock, so no cache call is made while this family's lock is held, and the
 * tracker calls out to nothing while it holds its own.
 *
 * @opensearch.experimental
 */
@ExperimentalApi
public final class FullFileState {

    private static final Logger logger = LogManager.getLogger(FullFileState.class);

    /**
     * Promotion lifecycle state.
     *
     * @opensearch.experimental
     */
    @ExperimentalApi
    public enum State {
        /** Served from 8 MiB blocks (default). */
        BLOCK,
        /** A whole-file download is in flight on a background thread. */
        PROMOTING,
        /** The whole file is mapped locally and served from {@link #localMaster}. */
        FULL,
        /** The family has closed; no further promotion. */
        CLOSED
    }

    private final AtomicReference<State> state = new AtomicReference<>(State.BLOCK);
    // True once the family's file is served from the remote store and its local copy is gone: set at construction for
    // a family created from remote, otherwise by TieredDirectory.afterSyncToRemote after switchToRemote has returned.
    // The trigger reads this, not an individual input's flag, so a promotion can never overlap the local-copy removal.
    private volatile boolean remoteMode;
    // The checksum the remote store recorded for the file at upload (the Lucene footer value as a decimal string), set
    // together with remoteMode; null when the upload metadata was not available.
    private volatile String expectedChecksum;
    private final AtomicReference<IndexInput> localMaster = new AtomicReference<>();
    // The cache entry of the current FULL period, which owns the mapping; the cache's removal of it demotes the family.
    private volatile PromotedWholeFileEntry entry;
    // The handle a download of this family runs under; created by the first promotion that needs it, cancelled at
    // closeFamily, and left as CANCELLED afterwards so a promotion racing the close starts nothing.
    private final AtomicReference<CancellableThreads> downloadCancellation = new AtomicReference<>();
    private static final CancellableThreads CANCELLED = new CancellableThreads();
    static {
        CANCELLED.cancel("family closed");
    }
    private volatile long retryAt;
    // The cache's evictions of this family's whole file that followed one another closely, and when the wait set by
    // the last one ends; guarded by releaseLock.
    private int evictionsInARow;
    private long backoffEndsMillis;
    // Set when a whole file assembled partly from this family's local blocks failed its checksum: the next download
    // reads every part from the remote store, since one of those blocks is corrupt and nothing says which.
    private volatile boolean localBlocksSuspect;
    // The query phases open on the family's shard; the unmap waits for the ones open when the family left FULL.
    private final ShardUseTracker tracker;
    // Set once this FULL period has asked the tracker to run its release; re-armed by completePromotion.
    private final AtomicBoolean drainRequested = new AtomicBoolean(false);
    // When the cache entry's position was last refreshed for a view (monotonic millis).
    private final AtomicLong lastTouchMillis = new AtomicLong(Long.MIN_VALUE);

    // Teardown wiring, set by completePromotion together with the mapping.
    private volatile Executor cleanupExecutor;
    private volatile BooleanSupplier nodeStopping;
    private volatile Runnable budgetRelease;
    private volatile Runnable onAwaitingRelease;
    private volatile Runnable onCleanupDone;
    private volatile Runnable onEvictedByCache;
    // Set when the family left FULL (closed, demoted or evicted by the cache) with a mapping still published: the
    // deferred unmap is owed exactly in that case.
    private volatile boolean mappingReleaseOwed;
    // Set when the release could not delete the file; it is then run again by retryRelease().
    private volatile boolean releaseRetryNeeded;
    // Orders the transitions out of FULL (close, demote, eviction), the scheduling of the release and the start of a
    // new FULL period against each other; never held across cache calls.
    private final Object releaseLock = new Object();
    // One-shot per FULL period: re-armed by completePromotion.
    private final AtomicBoolean cleanupScheduled = new AtomicBoolean(false);
    private volatile boolean cleanupDone;

    // Everything promote() needs to build the download request and open the family's own mapping, all available from
    // the PromotableCachedSwitchableIndexInput constructor.
    private final TransferManager transferManager;
    private final FSDirectory localDirectory;
    private final FileCache fileCache;
    private final String fileName;
    private final long fileLength;
    private final Path fullFilePath;

    /**
     * @param transferManager used to download the whole file in parts
     * @param localDirectory  the shard's local FS directory (download target and mmap source)
     * @param fileCache       the node file cache
     * @param fileName        the plain file name (no suffix)
     * @param fileLength      the whole-file length in bytes
     * @param fullFilePath    the FileCache key the whole file occupies ({@code <dir>/<name>})
     */
    public FullFileState(
        TransferManager transferManager,
        FSDirectory localDirectory,
        FileCache fileCache,
        String fileName,
        long fileLength,
        Path fullFilePath
    ) {
        this(transferManager, localDirectory, fileCache, fileName, fileLength, fullFilePath, new ShardUseTracker());
    }

    /**
     * As the constructor above, for a family of a shard.
     *
     * @param tracker the tracker of the shard the file belongs to
     */
    public FullFileState(
        TransferManager transferManager,
        FSDirectory localDirectory,
        FileCache fileCache,
        String fileName,
        long fileLength,
        Path fullFilePath,
        ShardUseTracker tracker
    ) {
        this.tracker = tracker;
        this.transferManager = transferManager;
        this.localDirectory = localDirectory;
        this.fileCache = fileCache;
        this.fileName = fileName;
        this.fileLength = fileLength;
        this.fullFilePath = fullFilePath;
    }

    /**
     * Marks the family as served from the remote store, which allows whole-file promotion. Set at construction for a
     * family created from the remote store, otherwise by the directory after the switch to remote has completed for the
     * original and every clone and the local copy has been removed; a promotion therefore never overlaps that removal.
     * An input's own switched flag flips earlier, while the switch is still in progress, so it is not used for this.
     * Remote mode is final.
     *
     * @param expectedChecksum the checksum the remote store holds for the file, which a local whole-file copy must
     *                         match before it is served; null when unknown, in which case no local copy is reused
     */
    public void markRemoteMode(String expectedChecksum) {
        this.expectedChecksum = expectedChecksum;
        remoteMode = true;
    }

    /** @return true once the family is in remote (block) mode and may be promoted */
    public boolean isRemoteMode() {
        return remoteMode;
    }

    /** @return the remote store's checksum for the file (a decimal string), or null when it is not known */
    String expectedChecksum() {
        return expectedChecksum;
    }

    /**
     * The checksum the remote store recorded for {@code fileName} at upload.
     *
     * @param remoteDirectory the shard's remote segment store
     * @param fileName        the plain file name
     * @return the checksum as a decimal string, or null when the file has no upload metadata
     */
    public static String uploadedChecksum(RemoteSegmentStoreDirectory remoteDirectory, String fileName) {
        RemoteSegmentStoreDirectory.UploadedSegmentMetadata metadata = remoteDirectory.getSegmentsUploadedToRemoteStore().get(fileName);
        return metadata == null ? null : metadata.getChecksum();
    }

    /** @return the current lifecycle state */
    public State state() {
        return state.get();
    }

    /**
     * The trigger: a clone or slice of a handle finds the family in BLOCK. Moves it to PROMOTING when the retry time has
     * passed and no previous mapping is still waiting for its release (a promotion in that window would map the file a
     * second time). The state and the retry time are read without the lock first, so the lock is taken only when a
     * promotion is about to start.
     *
     * @param nowMillis the current monotonic time
     * @return true if this call moved the family to PROMOTING; the caller then promotes it or abandons the promotion
     */
    boolean onAccess(long nowMillis) {
        if (state.get() != State.BLOCK || nowMillis < retryAt) {
            return false;
        }
        synchronized (releaseLock) {
            if (state.get() != State.BLOCK || nowMillis < retryAt || isMappingReleasePending()) {
                return false;
            }
            state.set(State.PROMOTING);
            return true;
        }
    }

    /**
     * Ends a promotion that did not reach FULL: PROMOTING goes back to BLOCK and no promotion starts before
     * {@code retryAt}. The retry time is set even when the family is no longer PROMOTING (it closed meanwhile), so a
     * trigger racing this call cannot start a new promotion in between.
     *
     * @param retryAt the earliest monotonic time a new promotion may be attempted
     * @return true if the family went back to BLOCK; false if it was in any other state
     */
    boolean abandonPromotion(long retryAt) {
        synchronized (releaseLock) {
            this.retryAt = retryAt;
            return state.compareAndSet(State.PROMOTING, State.BLOCK);
        }
    }

    /** Records that a whole file assembled from this family's local blocks failed its checksum. */
    void markLocalBlocksSuspect() {
        localBlocksSuspect = true;
    }

    /** @return true once a whole file assembled from this family's local blocks failed its checksum */
    boolean localBlocksSuspect() {
        return localBlocksSuspect;
    }

    /** @return the local whole-file mapping, or null before promotion publishes it */
    public IndexInput localMaster() {
        return localMaster.get();
    }

    /** @return the cache entry of the current FULL period, or null outside one */
    PromotedWholeFileEntry entry() {
        return entry;
    }

    /** How {@link #completePromotion} ended. */
    enum Completion {
        /** The mapping is published and the family is FULL. */
        FULL,
        /** Another completion handler for this family published its mapping first; that handler owns the state. */
        PUBLISHED_BY_OTHER,
        /** The family left PROMOTING before the publish (it closed, or its promotion was abandoned); nothing was installed. */
        NOT_PROMOTING
    }

    /**
     * Publishes the mapping of a PROMOTING family and moves it to FULL in one step, with the teardown that later gives
     * the mapping and the budget back. A new FULL period starts here, so the one-shot release flags of the previous
     * period are re-armed; that period's release has run by now, because {@link #onAccess} does not start a promotion
     * while a release is pending and a closed family is never promoted.
     *
     * @param entry             the cache entry that owns the family's whole-file mapping; the caller puts it in the
     *                          cache right after
     * @param cleanupExecutor   pool the unmap runs on (never the FileCache listener thread)
     * @param nodeStopping      whether the node is stopping; a release that runs then leaves the file where it is
     * @param budgetRelease     returns this file's reserved per-segment budget bytes, once the file is deleted
     * @param onAwaitingRelease called if the family leaves FULL while scopes still hold views
     * @param onCleanupDone     called once the deferred unmap has run
     * @param onEvictedByCache  called when the cache drops the entry of a FULL family
     * @return {@link Completion#FULL}, or why nothing was installed
     */
    Completion completePromotion(
        PromotedWholeFileEntry entry,
        Executor cleanupExecutor,
        BooleanSupplier nodeStopping,
        Runnable budgetRelease,
        Runnable onAwaitingRelease,
        Runnable onCleanupDone,
        Runnable onEvictedByCache
    ) {
        synchronized (releaseLock) {
            if (localMaster.get() != null) {
                return Completion.PUBLISHED_BY_OTHER;
            }
            if (state.get() != State.PROMOTING) {
                return Completion.NOT_PROMOTING;
            }
            this.cleanupExecutor = cleanupExecutor;
            this.nodeStopping = nodeStopping;
            this.budgetRelease = budgetRelease;
            this.onAwaitingRelease = onAwaitingRelease;
            this.onCleanupDone = onCleanupDone;
            this.onEvictedByCache = onEvictedByCache;
            mappingReleaseOwed = false;
            releaseRetryNeeded = false;
            drainRequested.set(false);
            cleanupScheduled.set(false);
            cleanupDone = false;
            this.entry = entry;
            localMaster.set(entry.mapping());
            state.set(State.FULL);
            return Completion.FULL;
        }
    }

    /**
     * Counts an eviction of the whole file by the cache and puts off the next promotion: by {@code firstDelayMillis}
     * after the first eviction, twice as long after each one that follows, and never longer than
     * {@code maxDelayMillis}. An eviction that comes {@code quietMillis} or more after the previous wait ended starts
     * the count again: the file was free to be promoted for that long and kept its place. The time is measured from
     * the end of the wait, not from the eviction, or a long wait would by itself look like a quiet spell. A retry time
     * already further away is kept.
     *
     * @param nowMillis the current monotonic time
     * @return the delay applied, in millis
     */
    long backOffAfterEviction(long nowMillis, long firstDelayMillis, long maxDelayMillis, long quietMillis) {
        synchronized (releaseLock) {
            if (evictionsInARow > 0 && nowMillis - backoffEndsMillis >= quietMillis) {
                evictionsInARow = 0;
            }
            evictionsInARow++;
            long delay = firstDelayMillis <= 0 ? 0 : Math.min(maxDelayMillis, firstDelayMillis << Math.min(evictionsInARow - 1, 30));
            backoffEndsMillis = nowMillis + delay;
            retryAt = Math.max(retryAt, nowMillis + delay);
            return delay;
        }
    }

    /** @return the earliest time (monotonic millis) a new promotion may be attempted */
    long retryAt() {
        return retryAt;
    }

    /** @return the tracker of the shard the file belongs to */
    public ShardUseTracker tracker() {
        return tracker;
    }

    /**
     * Refreshes the position of the whole file's cache entry, at most once a second, for a view that is about to be
     * cut. Nothing holds a reference on the entry, so this is what keeps a file in use from looking cold to the cache;
     * a file read less than once a second ages like any other entry.
     *
     * @param nowMillis the current monotonic time
     */
    public void touch(long nowMillis) {
        long last = lastTouchMillis.get();
        if (nowMillis - last < 1000 && last != Long.MIN_VALUE) {
            return;
        }
        if (lastTouchMillis.compareAndSet(last, nowMillis) && fileCache.get(fullFilePath) != null) {
            fileCache.decRef(fullFilePath);
        }
    }

    /**
     * The handle a download of this family runs under, created on first use. Once the family has closed this is an
     * already cancelled handle, so a download started under it fails before its first remote read.
     *
     * @return the family's download cancellation handle
     */
    CancellableThreads downloadCancellation() {
        return downloadCancellation.updateAndGet(current -> current != null ? current : new CancellableThreads());
    }

    /**
     * Called by {@link PromotedWholeFileEntry#close()} when the cache drops the entry, by eviction, prune, clear or an
     * explicit remove, inside the FileCache removal listener under a segment lock, so nothing here touches the cache.
     * A FULL family leaves FULL for BLOCK. The budget stays held: the file still occupies the disk until the release
     * has closed the mapping and deleted it. An entry of an earlier period changes nothing.
     *
     * @param removed the entry the cache dropped
     */
    void onEntryRemoved(PromotedWholeFileEntry removed) {
        boolean wasFull;
        synchronized (releaseLock) {
            if (removed != entry) {
                return;
            }
            wasFull = leaveFull(State.BLOCK);
        }
        if (wasFull) {
            Runnable evicted = onEvictedByCache;
            if (evicted != null) {
                evicted.run();
            }
        }
        scheduleRelease();
    }

    /**
     * The family-close routine. Called from {@code PromotableCachedSwitchableIndexInput.close()}, which runs
     * inside the FileCache removal listener under a segment lock, so this must not touch the FileCache inline.
     *
     * <p>Marks the state CLOSED and cancels a download in flight. A FULL family leaves FULL, and its whole file is
     * released like a demoted or evicted one. If it was PROMOTING, the cancelled download fails and the manager's
     * completion handler unwinds; if BLOCK with nothing owed, there is nothing to release.
     */
    public void closeFamily() {
        synchronized (releaseLock) {
            leaveFull(State.CLOSED);
        }
        CancellableThreads download = downloadCancellation.getAndSet(CANCELLED);
        if (download != null) {
            download.cancel("family closed"); // a download in flight fails within a part; its completion handler unwinds
        }
        scheduleRelease();
    }

    /**
     * Takes a FULL family back to BLOCK because its extension is no longer configured. Until the release has run the
     * family cannot be promoted again (see {@link #isMappingReleasePending()}).
     *
     * @return true if the family was FULL and is now BLOCK; false if it was in any other state
     */
    boolean demote() {
        boolean wasFull;
        synchronized (releaseLock) {
            wasFull = leaveFull(State.BLOCK);
        }
        scheduleRelease();
        return wasFull;
    }

    /**
     * The one way out of FULL, for the cache dropping the entry, a demotion and a close: the family moves to
     * {@code next} and, if it was FULL, owes the release of its mapping. No new view is handed out from here on; views
     * already cut keep reading the mapping until the release. Called under the lock, and followed by
     * {@link #scheduleRelease()} outside it.
     *
     * @param next {@link State#BLOCK}, taken only from FULL, or {@link State#CLOSED}, taken from any state
     * @return true if the family was FULL
     */
    private boolean leaveFull(State next) {
        assert Thread.holdsLock(releaseLock);
        // The release is owed before the new state becomes visible: a trigger that sees BLOCK must also see the pending
        // release, or it would start a promotion that maps the file a second time and then finds this mapping.
        boolean owedBefore = mappingReleaseOwed;
        mappingReleaseOwed = true;
        boolean wasFull = next == State.CLOSED ? state.getAndSet(State.CLOSED) == State.FULL : state.compareAndSet(State.FULL, State.BLOCK);
        if (wasFull == false) {
            mappingReleaseOwed = owedBefore;
        }
        return wasFull;
    }

    /**
     * Asks the shard's tracker to run the release a family owes once the query phases open now have ended, and
     * reports the family if any is open. Once per FULL period; a no-op for a family that owes nothing.
     */
    private void scheduleRelease() {
        if (mappingReleaseOwed == false || cleanupDone) {
            return;
        }
        if (drainRequested.compareAndSet(false, true) == false) {
            return;
        }
        if (tracker.openHolds() > 0) {
            Runnable report = onAwaitingRelease;
            if (report != null) {
                report.run();
            }
        }
        tracker.whenDrained(this::maybeScheduleCleanup);
    }

    /**
     * @return true while a family that left FULL still holds its previous mapping, i.e. until the deferred release has
     *         run; a promotion in that window would map the file a second time
     */
    boolean isMappingReleasePending() {
        return mappingReleaseOwed && cleanupDone == false && state.get() != State.CLOSED;
    }

    /**
     * Hands the cleanup to the executor, exactly once per FULL period. Called by the shard's tracker once the query
     * phases that were open when the family left FULL have ended, and by {@link #retryRelease()}.
     */
    private void maybeScheduleCleanup() {
        synchronized (releaseLock) {
            if (mappingReleaseOwed == false) {
                return;
            }
            Executor executor = cleanupExecutor;
            if (executor == null || cleanupScheduled.compareAndSet(false, true) == false) {
                return;
            }
            try {
                executor.execute(this::runCleanup);
            } catch (RuntimeException e) {
                // Nothing was queued. Never run inline: this may be the FileCache listener thread. The release is run
                // again by retryRelease().
                cleanupScheduled.set(false);
                releaseRetryNeeded = true;
                logger.warn("could not schedule whole-file cleanup for {}; it is tried again later", fullFilePath, e);
            }
        }
    }

    /**
     * The release. In order: the cache entry is dropped, if the cache still holds it; the mapping is closed; the file
     * is deleted; the bytes the cache kept counting and the budget are given back. A failed delete keeps both and the
     * release is run again by {@link #retryRelease()}. Once the node is stopping only the mapping is closed: the entry
     * and the file stay where they are, and the next start validates the file or deletes it.
     */
    private void runCleanup() {
        final boolean stopping = isStopping();
        releaseRetryNeeded = false;
        final PromotedWholeFileEntry current = entry;
        if (stopping == false) {
            try {
                // By value: a newer entry at the key belongs to a later family of the file and is left alone. Dropping
                // the entry moves the file aside and runs onEntryRemoved, which finds nothing more to do.
                CachedIndexInput other = fileCache.compute(fullFilePath, (key, existing) -> existing == current ? null : existing);
                if (other != null) {
                    fileCache.decRef(fullFilePath); // compute took a reference on the entry it kept
                }
            } catch (RuntimeException e) {
                logger.warn(() -> new ParameterizedMessage("failed to remove whole file {} from the cache", fullFilePath), e);
            }
        }
        localMaster.set(null);
        try {
            current.closeMapping(); // munmap: safe now, no scope holds a view
        } catch (Exception e) {
            logger.warn(() -> new ParameterizedMessage("failed to close whole-file mapping for {}", fullFilePath), e);
        }
        if (stopping || isStopping()) {
            return;
        }
        if (current.deleteFile() == false) {
            // The file still occupies the disk, so the cache's bytes and the budget stay held.
            releaseRetryNeeded = true;
            cleanupScheduled.set(false);
            Runnable report = onAwaitingRelease;
            if (report != null) {
                report.run();
            }
            return;
        }
        if (current.owedToCache()) {
            fileCache.releaseOwed(fullFilePath, current.length());
        }
        Runnable release = budgetRelease;
        if (release != null) {
            release.run();
        }
        cleanupDone = true;
        Runnable done = onCleanupDone;
        if (done != null) {
            done.run();
        }
    }

    /** Runs the release again if its last run could not delete the file. */
    void retryRelease() {
        if (releaseRetryNeeded) {
            maybeScheduleCleanup();
        }
    }

    /** Whether the whole file is to be left on disk: the node is stopping, or its cache was closed as for a restart. */
    private boolean isStopping() {
        return nodeStopping.getAsBoolean()
            || fileCache.isClosedForRestart()
            || (cleanupExecutor instanceof ExecutorService pool && pool.isShutdown());
    }

    /** @return true once the deferred unmap has run */
    boolean cleanupDone() {
        return cleanupDone;
    }

    // Accessors for the fields promote() needs; package-private because only WarmFullFileManager consumes them.

    /** @return the transfer manager used to download the whole file */
    TransferManager transferManager() {
        return transferManager;
    }

    /** @return the shard's local FS directory */
    FSDirectory localDirectory() {
        return localDirectory;
    }

    /** @return the plain file name */
    String fileName() {
        return fileName;
    }

    /** @return the whole-file length in bytes */
    long fileLength() {
        return fileLength;
    }

    /** @return the FileCache key the whole file occupies */
    Path fullFilePath() {
        return fullFilePath;
    }
}

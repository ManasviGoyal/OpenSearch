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
import org.apache.lucene.store.FSDirectory;
import org.apache.lucene.store.IndexInput;
import org.opensearch.common.annotation.ExperimentalApi;
import org.opensearch.common.util.CancellableThreads;
import org.opensearch.index.store.RemoteSegmentStoreDirectory;
import org.opensearch.index.store.remote.filecache.FileCache;
import org.opensearch.index.store.remote.utils.TransferManager;

import java.nio.file.Path;
import java.util.concurrent.Executor;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

/**
 * Per-file (family) whole-file promotion state, shared by every handle of the file: the original opened by
 * {@code TieredDirectory.openInput} and every clone and slice derived from it. One instance per
 * {@code PromotableCachedSwitchableIndexInput}, so a file is downloaded and mapped at most once however many readers
 * open it.
 *
 * <p>A promoted whole file is an ordinary file cache entry ({@link PromotedWholeFileEntry}): the family holds no
 * reference on it while FULL. A search scope takes one reference and one in-flight count on its first view of the
 * file ({@link #acquireView}) and gives both back when it ends ({@link #releaseView}), so between searches the file is
 * unreferenced and the cache evicts it under pressure like a block. When the cache drops the entry, for any reason,
 * {@link #onEntryRemoved} demotes the family to blocks and owes the unmap, which runs once the in-flight count is
 * zero; a scope that holds a view never has the mapping closed under it. A thread outside a scope reads blocks.
 *
 * <p>Every transition is a named method of this class, taken under one lock: {@link #onAccess}, {@link #abandonPromotion},
 * {@link #completePromotion}, {@link #demote}, {@link #restore}, {@link #onEntryRemoved}, {@link #closeFamily} and the
 * release the last {@link #releaseView} schedules. The mapping is published and the family moves to {@link State#FULL}
 * in one step, so a reader that sees FULL sees the mapping. The unmap and the release of the budget run exactly once
 * per FULL period, on the cleanup executor, never on the cache listener thread.
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
    // Set when a whole file assembled partly from this family's local blocks failed its checksum: the next download
    // reads every part from the remote store, since one of those blocks is corrupt and nothing says which.
    private volatile boolean localBlocksSuspect;
    // Search scopes holding a view of the mapping; the unmap waits for zero.
    private final AtomicInteger inFlightScopes = new AtomicInteger(0);

    // Teardown wiring, set by completePromotion together with the mapping.
    private volatile Executor cleanupExecutor;
    private volatile Runnable budgetRelease;
    private volatile Runnable onAwaitingRelease;
    private volatile Runnable onCleanupDone;
    private volatile Runnable onEvictedByCache;
    // Set when the family left FULL (closed, demoted or evicted by the cache) with a mapping still published: the
    // deferred unmap is owed exactly in that case.
    private volatile boolean mappingReleaseOwed;
    // Set once the cache has dropped this period's entry; the family then neither removes the key nor restores.
    private volatile boolean entryRemoved;
    // Orders the transitions out of FULL (close, demote, eviction), restore, the scheduling of the release and the start
    // of a new FULL period against each other; never held across cache calls.
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
     * @param budgetRelease     returns this file's reserved per-segment budget bytes; may run more than once, and
     *                          gives the bytes back the first time: when the cache drops the file, or at the unmap
     * @param onAwaitingRelease called if the family leaves FULL while scopes still hold views
     * @param onCleanupDone     called once the deferred unmap has run
     * @param onEvictedByCache  called when the cache drops the entry of a FULL family
     * @return {@link Completion#FULL}, or why nothing was installed
     */
    Completion completePromotion(
        PromotedWholeFileEntry entry,
        Executor cleanupExecutor,
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
            this.budgetRelease = budgetRelease;
            this.onAwaitingRelease = onAwaitingRelease;
            this.onCleanupDone = onCleanupDone;
            this.onEvictedByCache = onEvictedByCache;
            mappingReleaseOwed = false;
            entryRemoved = false;
            cleanupScheduled.set(false);
            cleanupDone = false;
            this.entry = entry;
            localMaster.set(entry.mapping());
            state.set(State.FULL);
            return Completion.FULL;
        }
    }

    /** @return the earliest time (monotonic millis) a new promotion may be attempted */
    long retryAt() {
        return retryAt;
    }

    /**
     * Takes a search scope's hold on the whole file: one reference on the cache entry, so the cache does not evict
     * it while the scope reads, and one in-flight count, so a demotion does not unmap under the scope. The reference
     * is taken before the state is checked, so a family seen FULL under it cannot lose its entry to eviction in
     * between; an explicit removal can still demote it, and then waits for this scope.
     *
     * @return true if the scope now holds the file and may cut views of {@link #localMaster()}; false when the family
     *         is not FULL or the cache has dropped the file, in which case the scope reads blocks
     */
    boolean acquireView() {
        if (state.get() != State.FULL) {
            return false;
        }
        if (fileCache.get(fullFilePath) == null) {
            return false;
        }
        synchronized (releaseLock) {
            if (state.get() == State.FULL && mappingReleaseOwed == false) {
                inFlightScopes.incrementAndGet();
                return true;
            }
        }
        fileCache.decRef(fullFilePath);
        return false;
    }

    /** Gives a scope's hold back; the last one out of a family that owes its release schedules the unmap. */
    void releaseView() {
        fileCache.decRef(fullFilePath);
        synchronized (releaseLock) {
            if (inFlightScopes.decrementAndGet() == 0) {
                maybeScheduleCleanup();
            }
        }
    }

    /** @return the number of search scopes holding a view of the mapping */
    int inFlightScopes() {
        return inFlightScopes.get();
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
     * A FULL family is demoted: new reads go to blocks, and the mapping is released once no scope holds a view. The
     * budget goes back now, since the file no longer occupies its segment. An entry of an earlier period, or of a
     * family already demoted or closed, changes nothing beyond that.
     *
     * @param removed the entry the cache dropped
     */
    void onEntryRemoved(PromotedWholeFileEntry removed) {
        boolean wasFull;
        Runnable budget;
        synchronized (releaseLock) {
            if (removed != entry) {
                return;
            }
            entryRemoved = true;
            wasFull = state.compareAndSet(State.FULL, State.BLOCK);
            if (wasFull) {
                mappingReleaseOwed = true;
            }
            budget = budgetRelease;
        }
        if (budget != null) {
            budget.run();
        }
        if (wasFull) {
            Runnable evicted = onEvictedByCache;
            if (evicted != null) {
                evicted.run();
            }
            if (inFlightScopes.get() > 0) {
                Runnable report = onAwaitingRelease;
                if (report != null) {
                    report.run();
                }
            }
        }
        maybeScheduleCleanup();
    }

    /**
     * The family-close routine. Called from {@code PromotableCachedSwitchableIndexInput.close()}, which runs
     * inside the FileCache removal listener under a segment lock, so this must not touch the FileCache inline.
     *
     * <p>Marks the state CLOSED and cancels a download in flight. If it was FULL, or demoted with its release still
     * owed: schedules the unmap and the budget release for when no scope holds a view (immediately on the cleanup
     * executor if none does), and reports the family if scopes still do. The whole-file entry is left to the cache,
     * unreferenced and evictable. If it was PROMOTING, the cancelled download fails and the manager's completion
     * handler unwinds; if BLOCK with nothing owed, there is nothing to release.
     */
    public void closeFamily() {
        State prev;
        boolean owesRelease;
        synchronized (releaseLock) {
            // Held with restore(): a restore that has just put the family back to FULL must not clear the release this
            // close owes, or the mapping would never be given back.
            prev = state.getAndSet(State.CLOSED);
            if (prev == State.FULL) {
                mappingReleaseOwed = true;
            }
            // A demoted family still holds its mapping until its scopes end, like a FULL one.
            owesRelease = mappingReleaseOwed && cleanupDone == false;
        }
        CancellableThreads download = downloadCancellation.getAndSet(CANCELLED);
        if (download != null) {
            download.cancel("family closed"); // a download in flight fails within a part; its completion handler unwinds
        }
        if (owesRelease == false) {
            return;
        }
        if (inFlightScopes.get() > 0) {
            Runnable report = onAwaitingRelease;
            if (report != null) {
                report.run();
            }
        }
        maybeScheduleCleanup();
    }

    /**
     * Takes a FULL family back to BLOCK: no new whole-file view is handed out from now on, and once no scope holds a
     * view the mapping is closed, the whole file is removed from the cache and the budget is released, through the
     * same deferred cleanup a close uses. Views already cut keep reading the mapping until then. Until the release has
     * run the family cannot be promoted again (see {@link #isMappingReleasePending()}).
     *
     * @return true if the family was FULL and is now BLOCK; false if it was in any other state
     */
    boolean demote() {
        synchronized (releaseLock) {
            // The release is owed before BLOCK becomes visible: a trigger that sees BLOCK must also see the pending
            // release, or it would start a promotion that maps the file a second time and then finds this mapping.
            boolean owedBefore = mappingReleaseOwed;
            mappingReleaseOwed = true;
            if (state.compareAndSet(State.FULL, State.BLOCK) == false) {
                mappingReleaseOwed = owedBefore;
                return false;
            }
            maybeScheduleCleanup();
            return true;
        }
    }

    /**
     * Takes a demoted family back to FULL while it still holds its mapping and the cache still holds its file: the
     * extension is in the setting again and nothing has been released, so views are served from the existing mapping
     * from the next clone on, with no download and no second mapping. Refused once the deferred release has been
     * scheduled, once the cache has dropped the file, or if the family is not a demoted one; such a family is promoted
     * again the ordinary way after its release.
     *
     * @return true if the family is FULL again
     */
    boolean restore() {
        synchronized (releaseLock) {
            if (mappingReleaseOwed == false || entryRemoved || cleanupScheduled.get() || localMaster.get() == null) {
                return false;
            }
            if (state.compareAndSet(State.BLOCK, State.FULL) == false) {
                return false;
            }
            mappingReleaseOwed = false;
            return true;
        }
    }

    /**
     * @return true while a demoted family still holds its previous mapping, i.e. between {@link #demote()} and the
     *         deferred release or a {@link #restore()}; a promotion in that window would map the file a second time
     */
    boolean isMappingReleasePending() {
        return mappingReleaseOwed && cleanupDone == false && state.get() != State.CLOSED;
    }

    /**
     * Schedules the cleanup exactly once per FULL period, and only when the family left FULL with a mapping and no
     * scope holds a view. {@link #closeFamily()}, {@link #demote()}, {@link #onEntryRemoved} and {@link #releaseView()}
     * call this after their own write, so whichever runs second sees both conditions true.
     */
    private void maybeScheduleCleanup() {
        // Held with closeFamily(), demote() and restore(): a restore must not slip in between this check and the
        // scheduling, or the release would run under a family that is FULL again.
        synchronized (releaseLock) {
            if (mappingReleaseOwed == false || inFlightScopes.get() != 0) {
                return;
            }
            Executor executor = cleanupExecutor;
            if (executor == null || cleanupScheduled.compareAndSet(false, true) == false) {
                return;
            }
            try {
                executor.execute(this::runCleanup);
            } catch (RuntimeException e) {
                // Nothing was queued, so a later releaseView(), demote() or closeFamily() may schedule it again. Never
                // run inline: this may be the FileCache listener thread.
                cleanupScheduled.set(false);
                logger.warn("could not schedule whole-file cleanup for {}; will retry on the next scope end or close", fullFilePath, e);
            }
        }
    }

    private void runCleanup() {
        localMaster.set(null);
        PromotedWholeFileEntry current = entry;
        if (current != null) {
            try {
                current.closeMapping(); // munmap: safe now, no scope holds a view
            } catch (Exception e) {
                logger.warn("failed to close whole-file mapping for {}", fullFilePath, e);
            }
        }
        if (entryRemoved == false && state.get() != State.CLOSED) {
            // Demoted, not closed: the file is no longer served, so drop it from the cache now rather than leave it
            // to eviction. The key is still this family's: a new family for the file exists only after this one
            // closes. A closing family leaves its entry to the cache, unreferenced and evictable.
            try {
                fileCache.remove(fullFilePath); // runs onEntryRemoved, which finds nothing more to do
            } catch (RuntimeException e) {
                logger.warn("failed to remove whole file {} from the cache", fullFilePath, e);
            }
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

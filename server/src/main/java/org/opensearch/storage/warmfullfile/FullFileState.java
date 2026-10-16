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
import org.opensearch.index.store.remote.filecache.FileCache;
import org.opensearch.index.store.remote.utils.TransferManager;

import java.io.IOException;
import java.nio.file.Path;
import java.util.concurrent.Executor;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

/**
 * Per-file-family state for warm-tier whole-file promotion.
 *
 * <p>One instance is owned by each {@code CachedSwitchableIndexInput} (the family's cache entry). It tracks the
 * promotion lifecycle and holds what the node-level manager needs to download the whole file and later unmap it:
 *
 * <pre>
 *   BLOCK  --(first eligible clone/slice triggers promotion)--&gt;  PROMOTING
 *   PROMOTING --(whole-file download completes, mapping opened)--&gt;  FULL
 *   PROMOTING --(decline or failure)--&gt;  BLOCK  (retry after retryAt)
 *   any --(family closes)--&gt;  CLOSED
 * </pre>
 *
 * The mapping is published before the CAS to {@link State#FULL}. When a FULL family closes, the budget is released at
 * once and the unmap plus the release of the family's FileCache reference run exactly once on the cleanup executor,
 * after the last handle closes; whichever of "family closed" and "last handle closed" happens second schedules it, so
 * no thread waits and nothing re-enters the FileCache from the removal listener.
 *
 * @opensearch.experimental
 */
@ExperimentalApi
public final class FullFileState {

    private static final Logger logger = LogManager.getLogger(FullFileState.class);

    /** Promotion lifecycle state. */
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
    private final AtomicReference<IndexInput> localMaster = new AtomicReference<>();
    private volatile long retryAt;
    private final AtomicInteger handleCount = new AtomicInteger(0);

    // Teardown wiring, attached by the manager before the CAS to FULL.
    private volatile Executor cleanupExecutor;
    private volatile Runnable budgetRelease;
    private volatile Runnable onClosedWithLiveHandles;
    // Set only when the family closed while FULL: the deferred cleanup is owed exactly in that case.
    private volatile boolean closedWhileFull;
    private final AtomicBoolean cleanupScheduled = new AtomicBoolean(false);
    private volatile boolean cleanupDone;

    // Everything promote() needs to build the download request and open the family's own mapping, all available from
    // the CachedSwitchableIndexInput constructor.
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
     * @param fileLength       the whole-file length in bytes
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
     */
    public void markRemoteMode() {
        remoteMode = true;
    }

    /** @return true once the family is in remote (block) mode and may be promoted */
    public boolean isRemoteMode() {
        return remoteMode;
    }

    /** @return the current lifecycle state */
    public State state() {
        return state.get();
    }

    /**
     * Atomically moves from {@code expect} to {@code next}.
     * @return true if the transition was applied
     */
    public boolean casState(State expect, State next) {
        return state.compareAndSet(expect, next);
    }

    /** @return the local whole-file mapping, or null before promotion publishes it */
    public IndexInput localMaster() {
        return localMaster.get();
    }

    /**
     * Atomically publishes or clears the mapping. The manager publishes with {@code (null, master)} so that only one
     * completion handler can install a mapping for the family, and unwinds with {@code (master, null)} so that a handler
     * can only clear the mapping it installed itself.
     *
     * @return true if the field held {@code expected} and now holds {@code update}
     */
    public boolean compareAndSetLocalMaster(IndexInput expected, IndexInput update) {
        return localMaster.compareAndSet(expected, update);
    }

    /** @return the earliest time (monotonic millis) a new promotion may be attempted */
    public long retryAt() {
        return retryAt;
    }

    /**
     * Sets the next retry time after a decline or failure.
     * @param retryAt monotonic millis
     */
    public void setRetryAt(long retryAt) {
        this.retryAt = retryAt;
    }

    /**
     * Records a new live handle (called when a wrapper handle is opened).
     * @return the new handle count
     */
    public int incHandles() {
        return handleCount.incrementAndGet();
    }

    /**
     * Releases a live handle (called when a wrapper handle closes). If this was the last handle of a family that
     * already closed while FULL, schedules the deferred cleanup.
     * @return the new handle count
     */
    public int decHandles() {
        int remaining = handleCount.decrementAndGet();
        if (remaining == 0) {
            maybeScheduleCleanup();
        }
        return remaining;
    }

    /** @return the current live handle count */
    public int handleCount() {
        return handleCount.get();
    }

    /**
     * Attaches the teardown for a FULL family. Called by the manager before it publishes FULL.
     *
     * @param cleanupExecutor          pool the unmap + decRef run on (never the FileCache listener thread)
     * @param budgetRelease            returns this file's reserved per-segment budget bytes
     * @param onClosedWithLiveHandles  called if the family closes while handles are still open (leak detection)
     */
    void attachCleanup(Executor cleanupExecutor, Runnable budgetRelease, Runnable onClosedWithLiveHandles) {
        this.cleanupExecutor = cleanupExecutor;
        this.budgetRelease = budgetRelease;
        this.onClosedWithLiveHandles = onClosedWithLiveHandles;
    }

    /**
     * The family-close routine. Called from {@code CachedSwitchableIndexInput.close()}, which runs
     * inside the FileCache removal listener under a segment lock, so this must not touch the FileCache inline.
     *
     * <p>Marks the state CLOSED. If it was FULL: releases the budget now, and schedules the unmap + decRef for when the
     * handle count reaches zero (immediately on the cleanup executor if it already is). If it was PROMOTING, the
     * manager's completion handler sees the lost CAS and unwinds; if BLOCK, there is nothing to release.
     */
    public void closeFamily() {
        State prev = state.getAndSet(State.CLOSED);
        if (prev != State.FULL) {
            return;
        }
        Runnable release = budgetRelease;
        if (release != null) {
            release.run();
        }
        closedWhileFull = true;
        if (handleCount.get() > 0) {
            Runnable report = onClosedWithLiveHandles;
            if (report != null) {
                report.run();
            }
        }
        maybeScheduleCleanup();
    }

    /**
     * Schedules the cleanup exactly once, and only when the family closed while FULL and no handle remains. Both
     * {@link #closeFamily()} and {@link #decHandles()} call this after their own write, so whichever runs second sees
     * both conditions true.
     */
    private void maybeScheduleCleanup() {
        if (closedWhileFull == false || handleCount.get() != 0) {
            return;
        }
        Executor executor = cleanupExecutor;
        if (executor == null || cleanupScheduled.compareAndSet(false, true) == false) {
            return;
        }
        try {
            executor.execute(this::runCleanup);
        } catch (RuntimeException e) {
            // Only expected on node shutdown. Never run inline: this may be the FileCache listener thread.
            logger.warn("could not schedule whole-file cleanup for {}; mapping kept until shutdown", fullFilePath, e);
        }
    }

    private void runCleanup() {
        IndexInput master = localMaster.getAndSet(null);
        if (master != null) {
            try {
                master.close(); // munmap: safe now, no handle (and so no view derived from one) remains
            } catch (IOException | RuntimeException e) {
                logger.warn("failed to close whole-file mapping for {}", fullFilePath, e);
            }
        }
        try {
            fileCache.decRef(fullFilePath); // release the family's reference; the whole file becomes evictable
        } catch (RuntimeException e) {
            logger.warn("failed to release whole-file cache reference for {}", fullFilePath, e);
        }
        cleanupDone = true;
    }

    /** @return true once the deferred unmap + decRef has run */
    public boolean cleanupDone() {
        return cleanupDone;
    }

    // Accessors for the fields promote() needs. These are package-private on purpose: they expose internal types
    // (TransferManager, directories, FileCache) that are not public API, and only WarmFullFileManager (same package)
    // consumes them. Keeping them non-public keeps those internal types off this class's public API surface.

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
    public long fileLength() {
        return fileLength;
    }

    /** @return the FileCache key the whole file occupies */
    Path fullFilePath() {
        return fullFilePath;
    }
}

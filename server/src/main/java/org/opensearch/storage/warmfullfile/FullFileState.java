/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */
package org.opensearch.storage.warmfullfile;

import org.apache.lucene.store.FSDirectory;
import org.apache.lucene.store.IndexInput;
import org.opensearch.common.annotation.ExperimentalApi;
import org.opensearch.index.store.RemoteSegmentStoreDirectory;
import org.opensearch.index.store.remote.filecache.FileCache;
import org.opensearch.index.store.remote.utils.TransferManager;

import java.nio.file.Path;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

/**
 * Per-file-family state for warm-tier whole-file promotion.
 *
 * <p>One instance is owned by each {@code CachedSwitchableIndexInput} (the family's cache entry). It tracks the
 * promotion lifecycle and holds everything the node-level manager needs to download the whole file and later unmap
 * it safely:
 *
 * <pre>
 *   BLOCK  --(first eligible clone/slice triggers promotion)--&gt;  PROMOTING
 *   PROMOTING --(whole-file download completes, mapping opened)--&gt;  FULL
 *   PROMOTING --(decline or failure)--&gt;  BLOCK  (retry after retryAt)
 *   any --(family closes)--&gt;  CLOSED
 * </pre>
 *
 * The {@code localMaster} mapping is published (volatile) before the CAS to {@link State#FULL}, and is unmapped only
 * after the last handle closes (see {@link #incHandles()}/{@link #decHandles()}); that deferral is enforced by the
 * owning cache entry's close path, not here.
 *
 * @opensearch.experimental
 */
@ExperimentalApi
public final class FullFileState {

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
    private volatile IndexInput localMaster;
    private volatile long retryAt;
    private final AtomicInteger handleCount = new AtomicInteger(0);

    // Everything promote() needs to build the download request and open the family's own mapping.
    // All five are available from the CachedSwitchableIndexInput constructor.
    private final TransferManager transferManager;
    private final FSDirectory localDirectory;
    private final RemoteSegmentStoreDirectory remoteDirectory;
    private final FileCache fileCache;
    private final String fileName;
    private final long fileLength;
    private final Path fullFilePath;

    /**
     * @param transferManager used to download the whole file in parts
     * @param localDirectory  the shard's local FS directory (download target and mmap source)
     * @param remoteDirectory the remote segment store directory (source of truth)
     * @param fileCache       the node file cache
     * @param fileName        the plain file name (no suffix)
     * @param fileLength       the whole-file length in bytes
     * @param fullFilePath    the FileCache key the whole file occupies ({@code <dir>/<name>})
     */
    public FullFileState(
        TransferManager transferManager,
        FSDirectory localDirectory,
        RemoteSegmentStoreDirectory remoteDirectory,
        FileCache fileCache,
        String fileName,
        long fileLength,
        Path fullFilePath
    ) {
        this.transferManager = transferManager;
        this.localDirectory = localDirectory;
        this.remoteDirectory = remoteDirectory;
        this.fileCache = fileCache;
        this.fileName = fileName;
        this.fileLength = fileLength;
        this.fullFilePath = fullFilePath;
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

    /**
     * Atomically marks the family CLOSED and returns the previous state (so the caller can run the FULL-teardown path
     * exactly once).
     * @return the state prior to closing
     */
    public State getAndSetClosed() {
        return state.getAndSet(State.CLOSED);
    }

    /** @return the local whole-file mapping, or null before promotion publishes it */
    public IndexInput localMaster() {
        return localMaster;
    }

    /**
     * Publishes the local whole-file mapping. Must be set before the CAS to {@link State#FULL}.
     * @param localMaster the family's own memory mapping
     */
    public void setLocalMaster(IndexInput localMaster) {
        this.localMaster = localMaster;
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
     * Releases a live handle (called when a wrapper handle closes).
     * @return the new handle count
     */
    public int decHandles() {
        return handleCount.decrementAndGet();
    }

    /** @return the current live handle count */
    public int handleCount() {
        return handleCount.get();
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

    /** @return the remote segment store directory */
    RemoteSegmentStoreDirectory remoteDirectory() {
        return remoteDirectory;
    }

    /** @return the node file cache */
    FileCache fileCache() {
        return fileCache;
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

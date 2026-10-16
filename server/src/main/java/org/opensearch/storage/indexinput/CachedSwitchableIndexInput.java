/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.storage.indexinput;

import org.apache.lucene.store.AlreadyClosedException;
import org.apache.lucene.store.FSDirectory;
import org.apache.lucene.store.IndexInput;
import org.opensearch.index.store.RemoteSegmentStoreDirectory;
import org.opensearch.index.store.remote.filecache.CachedIndexInput;
import org.opensearch.index.store.remote.filecache.FileCache;
import org.opensearch.index.store.remote.utils.TransferManager;
import org.opensearch.storage.prefetch.TieredStoragePrefetchSettings;
import org.opensearch.storage.warmfullfile.FullFileState;
import org.opensearch.threadpool.ThreadPool;

import java.io.IOException;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Supplier;

import static org.opensearch.storage.utils.DirectoryUtils.getFilePath;
import static org.opensearch.storage.utils.DirectoryUtils.getFilePathSwitchable;

/**
 * A {@link CachedIndexInput} implementation that wraps a {@link SwitchableIndexInput} for use with the file cache.
 */
public class CachedSwitchableIndexInput implements CachedIndexInput {

    private final SwitchableIndexInput switchableIndexInput;
    private final FullFileState fullFileState;
    private final AtomicBoolean isClosed;

    public CachedSwitchableIndexInput(
        FileCache fileCache,
        String fileName,
        FSDirectory localDirectory,
        RemoteSegmentStoreDirectory remoteDirectory,
        TransferManager transferManager,
        boolean cacheFromRemote,
        ThreadPool threadPool,
        Supplier<TieredStoragePrefetchSettings> tieredStoragePrefetchSettingsSupplier
    ) throws IOException {
        isClosed = new AtomicBoolean(false);
        String resourceDescription = "SwitchableIndexInput (path=" + getFilePath(localDirectory, fileName) + ")";
        switchableIndexInput = new SwitchableIndexInput(
            resourceDescription,
            fileName,
            getFilePath(localDirectory, fileName),
            getFilePathSwitchable(localDirectory, fileName),
            fileCache,
            localDirectory,
            remoteDirectory,
            transferManager,
            cacheFromRemote,
            threadPool,
            tieredStoragePrefetchSettingsSupplier
        );
        // The whole file (when promoted) occupies the <dir>/<name> key; its length comes from the switchable input
        // (CachedSwitchableIndexInput.length() returns 0 and must not be used here).
        this.fullFileState = new FullFileState(
            transferManager,
            localDirectory,
            fileCache,
            fileName,
            switchableIndexInput.fileLength(),
            getFilePath(localDirectory, fileName)
        );
        if (cacheFromRemote) {
            // Created for a file that is already in the remote store: promotable from the start. A family created for a
            // just-written local file is marked remote by TieredDirectory.afterSyncToRemote once the switch completes.
            this.fullFileState.markRemoteMode();
        }
    }

    /**
     * Returns the whole-file promotion state for this family. Safe to call on a closed entry (plain field read; does
     * not throw, unlike {@link #getIndexInput()}).
     * @return the family's {@link FullFileState}
     */
    @Override
    public FullFileState fullFileState() {
        return fullFileState;
    }

    @Override
    public IndexInput getIndexInput() {
        if (isClosed.get()) throw new AlreadyClosedException("Index input is already closed");
        return switchableIndexInput;
    }

    @Override
    public long length() {
        return 0;
    }

    @Override
    public boolean isClosed() {
        return isClosed.get();
    }

    @Override
    public void close() throws Exception {
        if (!isClosed.getAndSet(true)) {
            // Runs inside the FileCache removal listener under a segment lock. closeFamily() never touches the FileCache
            // inline: it releases the budget and defers the unmap + decRef to a pool thread, once the last handle closes.
            fullFileState.closeFamily();
            switchableIndexInput.close();
        }
    }
}

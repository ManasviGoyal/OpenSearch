/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.storage.indexinput;

import org.apache.lucene.store.FSDirectory;
import org.opensearch.index.store.RemoteSegmentStoreDirectory;
import org.opensearch.index.store.remote.filecache.FileCache;
import org.opensearch.index.store.remote.utils.TransferManager;
import org.opensearch.storage.prefetch.TieredStoragePrefetchSettings;
import org.opensearch.storage.warmfullfile.FullFileState;
import org.opensearch.threadpool.ThreadPool;

import java.io.IOException;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Supplier;

import static org.opensearch.storage.utils.DirectoryUtils.getFilePath;

/**
 * A {@link CachedSwitchableIndexInput} whose file may be promoted to a whole local file. It owns the family's
 * {@link FullFileState} and closes the family when the entry leaves the file cache.
 */
public class PromotableCachedSwitchableIndexInput extends CachedSwitchableIndexInput {

    private final FullFileState fullFileState;
    private final AtomicBoolean familyClosed = new AtomicBoolean(false);

    public PromotableCachedSwitchableIndexInput(
        FileCache fileCache,
        String fileName,
        FSDirectory localDirectory,
        RemoteSegmentStoreDirectory remoteDirectory,
        TransferManager transferManager,
        boolean cacheFromRemote,
        ThreadPool threadPool,
        Supplier<TieredStoragePrefetchSettings> tieredStoragePrefetchSettingsSupplier
    ) throws IOException {
        super(
            fileCache,
            fileName,
            localDirectory,
            remoteDirectory,
            transferManager,
            cacheFromRemote,
            threadPool,
            tieredStoragePrefetchSettingsSupplier
        );
        // The whole file (when promoted) occupies the <dir>/<name> key; its length comes from the switchable input
        // (CachedSwitchableIndexInput.length() returns 0 and must not be used here).
        SwitchableIndexInput switchableIndexInput = (SwitchableIndexInput) getIndexInput();
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
            this.fullFileState.markRemoteMode(FullFileState.uploadedChecksum(remoteDirectory, fileName));
        }
    }

    /**
     * Returns the whole-file promotion state for this family. Safe to call on a closed entry (plain field read; does
     * not throw, unlike {@link #getIndexInput()}).
     * @return the family's {@link FullFileState}
     */
    public FullFileState fullFileState() {
        return fullFileState;
    }

    @Override
    public void close() throws Exception {
        if (familyClosed.compareAndSet(false, true)) {
            // Runs inside the FileCache removal listener under a segment lock. closeFamily() never touches the FileCache
            // inline: it defers the unmap, the decRef and the budget release to a pool thread, once the last handle closes.
            fullFileState.closeFamily();
        }
        super.close();
    }
}

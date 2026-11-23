/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.index.store.remote.filecache;

import org.apache.lucene.store.IndexInput;
import org.opensearch.common.annotation.ExperimentalApi;
import org.opensearch.common.annotation.PublicApi;

import java.io.IOException;
import java.nio.file.Path;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executor;

/**
 * Interface for an entry in the {@link FileCache} that can return an
 * {@link IndexInput}. Exactly how the IndexInput is created is determined by
 * the implementations.
 *
 * @opensearch.api
 */
@PublicApi(since = "2.7.0")
public interface CachedIndexInput extends AutoCloseable {
    /**
     * Gets the {@link IndexInput} this cache entry represents.
     * @return The IndexInput
     * @throws IOException if any I/O error occurs
     */
    IndexInput getIndexInput() throws IOException;

    /**
     *  Trigger and get the completable future of index input
     *  @return CompletableFuture of IndexInput
     */
    @ExperimentalApi
    default CompletableFuture<IndexInput> asyncLoadIndexInput(Executor executor) {
        return null;
    }

    /**
     * @return length in bytes
     */
    long length();

    /**
     * @return true if the entry is closed, false otherwise
     */
    boolean isClosed();

    /**
     * Whether the owner of this entry deletes its file itself, some time after the cache has dropped the entry. The
     * cache does not delete the file of such an entry: it renames the file out of the way, tells the entry with
     * {@link #onFileRetired}, and keeps counting the entry's {@link #length()} in its usage until the owner reports
     * the file deleted with {@link FileCache#releaseOwed}.
     *
     * @return true if the file outlives the entry and its owner deletes it; false if the cache deletes it with the entry
     */
    @ExperimentalApi
    default boolean isFileReleasedByOwner() {
        return false;
    }

    /**
     * Called by the cache when it drops an entry whose {@link #isFileReleasedByOwner()} is true, before it closes the
     * entry and under the cache's lock, so this makes no cache call. The entry's bytes are owed to the cache from here
     * until {@link FileCache#releaseOwed}.
     *
     * @param retired where the entry's file is now, for the owner to delete; null if there was no file to move
     */
    @ExperimentalApi
    default void onFileRetired(Path retired) {}
}

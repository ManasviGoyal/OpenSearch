/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.index.store.remote.filecache;

import org.opensearch.common.UUIDs;
import org.opensearch.common.cache.RemovalReason;
import org.opensearch.index.store.remote.utils.FileTypeUtils;
import org.opensearch.index.store.remote.utils.cache.SegmentedCache;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.NoSuchFileException;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;

import static org.opensearch.ExceptionsHelper.catchAsRuntimeException;

/**
 * File Cache (FC) is introduced to solve the problem that the local disk cannot hold
 * the entire dataset on remote store. It maintains a node level view of index files with priorities,
 * caching only those index files needed by queries. The file with the lowest priority
 * (Least Recently Used) in the FC is replaced first.
 *
 * <p>The two main interfaces of FC are put and get. When a new file index input is added
 * to the file cache, the file will be added at cache head, which means it has the highest
 * priority.
 * <p> The get function does not add file to cache, but it promotes the priority
 * of a given file (since it makes it the most recently used).
 *
 * <p>Once file cache reaches its capacity, it starts evictions. Eviction removes the file
 * items from cache tail and triggers a callback to clean up the file from disk. The
 * cleanup process also includes closing file’s descriptor.
 *
 * @opensearch.internal
 */
public class FileCacheFactory {

    public static FileCache createConcurrentLRUFileCache(long capacity) {
        return new FileCache(createDefaultBuilder().capacity(capacity).build());
    }

    public static FileCache createConcurrentLRUFileCache(long capacity, int concurrencyLevel) {
        return new FileCache(createDefaultBuilder().capacity(capacity).concurrencyLevel(concurrencyLevel).build());
    }

    private static SegmentedCache.Builder<Path, CachedIndexInput> createDefaultBuilder() {
        return SegmentedCache.<Path, CachedIndexInput>builder()
            // use length in bytes as the weight of the file item
            .weigher(CachedIndexInput::length)
            // a file its owner deletes later still occupies the disk after its entry is gone
            .owedWeigher(value -> value.isFileReleasedByOwner() ? value.length() : 0)
            .listener((removalNotification) -> {
                RemovalReason removalReason = removalNotification.getRemovalReason();
                CachedIndexInput value = removalNotification.getValue();
                Path key = removalNotification.getKey();
                if (removalReason != RemovalReason.REPLACED) {
                    final boolean fileReleasedByOwner = value.isFileReleasedByOwner();
                    if (fileReleasedByOwner && removalReason != RemovalReason.RESTARTED) {
                        // Before the close: the owner may start its release as soon as the entry is closed.
                        value.onFileRetired(retire(key));
                    }
                    catchAsRuntimeException(value::close);
                    // On RESTARTED removal, we close the IndexInput but preserve the files on disk as this scenario only occurs during
                    // tests
                    if (removalReason != RemovalReason.RESTARTED && fileReleasedByOwner == false) {
                        catchAsRuntimeException(() -> Files.deleteIfExists(key));
                    }
                }
            });
    }

    /**
     * Renames a file out of the way, so that its own name is free while its owner still has to delete it.
     *
     * @return the file's new path, or null if there was no file; a file that cannot be renamed is deleted in place
     */
    private static Path retire(Path key) {
        final Path retired = key.resolveSibling(key.getFileName() + "." + UUIDs.randomBase64UUID() + FileTypeUtils.RETIRED_FILE_SUFFIX);
        try {
            Files.move(key, retired, StandardCopyOption.ATOMIC_MOVE);
            return retired;
        } catch (NoSuchFileException e) {
            return null;
        } catch (IOException e) {
            catchAsRuntimeException(() -> Files.deleteIfExists(key));
            return null;
        }
    }

}

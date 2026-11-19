/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.storage.warmfullfile;

import org.apache.lucene.store.IndexInput;
import org.opensearch.index.store.remote.filecache.CachedFullFileIndexInput;
import org.opensearch.index.store.remote.filecache.FileCache;

import java.nio.file.Path;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * The file cache entry of a promoted whole file: an ordinary full-file entry, referenced only while a search scope
 * reads it and evicted by the cache like any other when it is not. The cache's removal listener closes an entry it
 * drops, by eviction, prune, clear or an explicit remove; this entry's {@link #close()} tells its family instead of
 * unmapping, because a scope may still be reading the mapping, and the family unmaps once no scope does, through
 * {@link #closeMapping()}. The cache deletes the file right after; the mapping stays valid until it is closed.
 */
final class PromotedWholeFileEntry extends CachedFullFileIndexInput {

    private final FullFileState family;
    private final IndexInput mapping;
    private final long length;
    private final AtomicBoolean mappingClosed = new AtomicBoolean(false);

    /**
     * @param fileCache the node file cache
     * @param path      the whole file's cache key
     * @param mapping   the family's whole-file mapping, which this entry owns and weighs
     * @param family    the family served from the mapping
     */
    PromotedWholeFileEntry(FileCache fileCache, Path path, IndexInput mapping, FullFileState family) {
        super(fileCache, path, mapping);
        this.family = family;
        this.mapping = mapping;
        this.length = mapping.length();
    }

    /** The file's length, which the cache weighs the entry by; answered after the mapping is closed too. */
    @Override
    public long length() {
        return length;
    }

    /** @return the whole-file mapping views are cut from */
    IndexInput mapping() {
        return mapping;
    }

    /**
     * Called by the file cache when it drops this entry, under its segment lock: the family is demoted and unmaps
     * once no scope reads the mapping. Never touches the cache inline.
     */
    @Override
    public void close() {
        family.onEntryRemoved(this);
    }

    /** Closes the mapping. Called by the family's deferred release, once no scope can read it. */
    void closeMapping() throws Exception {
        if (mappingClosed.compareAndSet(false, true)) {
            super.close();
        }
    }

    @Override
    public boolean isClosed() {
        return mappingClosed.get();
    }
}

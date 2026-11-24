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
import org.apache.lucene.store.IndexInput;
import org.opensearch.index.store.remote.filecache.CachedFullFileIndexInput;
import org.opensearch.index.store.remote.filecache.FileCache;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * The file cache entry of a promoted whole file: an ordinary full-file entry, referenced only while a search scope
 * reads it and evicted by the cache like any other when it is not. The cache's removal listener closes an entry it
 * drops, by eviction, prune, clear or an explicit remove; this entry's {@link #close()} tells its family instead of
 * unmapping, because a scope may still be reading the mapping, and the family unmaps once no scope does, through
 * {@link #closeMapping()}. The cache does not delete the file: it moves it aside, so the name is free at once, and
 * keeps counting its bytes until the family has deleted it with {@link #deleteFile()} and released them.
 */
final class PromotedWholeFileEntry extends CachedFullFileIndexInput {

    private static final Logger logger = LogManager.getLogger(PromotedWholeFileEntry.class);

    private final FullFileState family;
    private final IndexInput mapping;
    private final long length;
    private final AtomicBoolean mappingClosed = new AtomicBoolean(false);
    // Where the cache moved the file when it dropped this entry; null while the entry is in the cache, and when there
    // was no file to move.
    private volatile Path retired;
    // Set when the cache dropped this entry and so counts its bytes; cleared by whoever gives them back.
    private final AtomicBoolean owedToCache = new AtomicBoolean(false);

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

    /** The family deletes the file itself, after it has closed the mapping; the cache moves it aside and waits. */
    @Override
    public boolean isFileReleasedByOwner() {
        return true;
    }

    @Override
    public void onFileRetired(Path retired) {
        this.retired = retired;
        this.owedToCache.set(true);
    }

    /**
     * Called by the file cache when it drops this entry, under its segment lock: the family is demoted and unmaps
     * once no scope reads the mapping. Never touches the cache inline.
     */
    @Override
    public void close() {
        family.onEntryRemoved(this);
    }

    /** @return true if the cache dropped this entry and still counts its bytes */
    boolean owedToCache() {
        return owedToCache.get();
    }

    /**
     * Claims the bytes the cache still counts for this entry, for the caller to give back.
     *
     * @return true, once, if the cache counts them; the caller then releases them
     */
    boolean takeOwed() {
        return owedToCache.compareAndSet(true, false);
    }

    /**
     * Deletes the file the cache moved aside. Called by the family's release after {@link #closeMapping()}.
     *
     * @return true if no such file is left; false if the delete failed and has to be tried again
     */
    boolean deleteFile() {
        final Path file = retired;
        if (file == null) {
            return true;
        }
        try {
            Files.deleteIfExists(file);
            retired = null;
            return true;
        } catch (IOException e) {
            logger.warn(() -> new ParameterizedMessage("failed to delete released whole file {}", file), e);
            return false;
        }
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

/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.storage.indexinput;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.lucene.store.AlreadyClosedException;
import org.apache.lucene.store.IndexInput;
import org.opensearch.common.annotation.ExperimentalApi;
import org.opensearch.storage.warmfullfile.FullFileState;
import org.opensearch.storage.warmfullfile.WarmFullFileManager;
import org.opensearch.threadpool.ThreadPool;

import java.io.IOException;

/**
 * A {@link SwitchableIndexInputWrapper} whose file may be promoted to a whole local file.
 *
 * <p>Only handles that passed the open-time eligibility rules are wrapped by this class. When the family is
 * {@link FullFileState.State#FULL}, {@link #clone()} and {@link #slice(String, long, long)} return a view of the
 * family's local whole-file mapping instead of a block input. Otherwise, the clone/slice asks the manager for a
 * promotion ({@link WarmFullFileManager#requestPromotion}) and a block input is returned. An input that already exists
 * is never switched.
 */
@ExperimentalApi
public class PromotableSwitchableIndexInputWrapper extends SwitchableIndexInputWrapper {

    private final String fullCloneDescription;
    private final FullFileState fullFileState;
    private final WarmFullFileManager manager;
    private static final Logger logger = LogManager.getLogger(PromotableSwitchableIndexInputWrapper.class);
    // Whether this thread belongs to a pool that runs queries. Pool threads are named like
    // opensearch[node][index_searcher][T#3], so a pool such as search_throttled does not match. Any other thread reads
    // blocks, whatever its context carries.
    private static final ThreadLocal<Boolean> QUERY_THREAD = ThreadLocal.withInitial(() -> {
        final String name = Thread.currentThread().getName();
        return name.contains("[" + ThreadPool.Names.SEARCH + "]") || name.contains("[" + ThreadPool.Names.INDEX_SEARCHER + "]");
    });

    /**
     * @param resourceDescription a human-readable description
     * @param in                  the underlying switchable input
     * @param fullFileState       the family's whole-file promotion state
     * @param manager             the node-level promotion manager (may be null when the feature is off)
     */
    public PromotableSwitchableIndexInputWrapper(
        String resourceDescription,
        SwitchableIndexInput in,
        FullFileState fullFileState,
        WarmFullFileManager manager
    ) {
        super(resourceDescription, in);
        this.fullCloneDescription = "Full clone " + resourceDescription;
        this.fullFileState = fullFileState;
        this.manager = manager;
    }

    @Override
    public IndexInput clone() {
        IndexInput view = viewOrNull("clone", master -> {
            IndexInput v = master.slice(fullCloneDescription, switchableIndexInput.offset(), switchableIndexInput.fileLength());
            v.seek(switchableIndexInput.getFilePointer()); // Lucene's clone() keeps the position
            return v;
        });
        if (view != null) {
            return view;
        }
        requestPromotion();
        return new PromotableSwitchableIndexInputWrapper(
            "Cloned " + resourceDescription,
            switchableIndexInput.clone(),
            fullFileState,
            manager
        );
    }

    @Override
    public IndexInput slice(String sliceDescription, long offset, long length) {
        IndexInput view = viewOrNull("slice", master -> {
            // The master spans the whole file, so its own bounds check is not enough: keep the slice inside this
            // handle's window, as the block path (and Lucene's slice contract) would.
            checkSliceWithinHandle(sliceDescription, offset, length);
            // A slice starts at 0, as Lucene requires; cut it at this instance's absolute offset.
            return master.slice(sliceDescription, switchableIndexInput.offset() + offset, length);
        });
        if (view != null) {
            return view;
        }
        requestPromotion();
        return new PromotableSwitchableIndexInputWrapper(
            "Sliced " + resourceDescription,
            switchableIndexInput.slice(sliceDescription, offset, length),
            fullFileState,
            manager
        );
    }

    /** Cuts one view of a published whole-file mapping. */
    @FunctionalInterface
    private interface ViewCutter {
        IndexInput cut(IndexInput master) throws IOException;
    }

    /**
     * A view of the family's whole-file mapping, or null when the handle must keep reading blocks: this thread is not
     * one of a query pool, its request is outside a query phase of the file's shard, the family is not FULL, the
     * mapping is not published, or the mapping was closed under this read. Blocks are always valid, so every fallback is silent
     * except an unexpected I/O error, which is logged at DEBUG.
     */
    private IndexInput viewOrNull(String what, ViewCutter cutter) {
        // Only a read inside a query phase of this file's shard, on a thread of a query pool, is served from the whole
        // file: the phase holds the shard, so the mapping is not closed under this view.
        if (QUERY_THREAD.get() == false || manager == null || manager.holdFor(fullFileState) == null) {
            return null;
        }
        IndexInput master = fullFileState.localMaster(); // read the volatile once
        if (fullFileState.state() != FullFileState.State.FULL || master == null) {
            return null;
        }
        fullFileState.touch(manager.relativeTimeInMillis());
        try {
            return cutter.cut(master);
        } catch (AlreadyClosedException e) {
            return null;
        } catch (IOException e) {
            logger.debug("failed to cut whole-file view for {}, using blocks: {}", what, resourceDescription, e);
            return null;
        }
    }

    private void checkSliceWithinHandle(String sliceDescription, long offset, long length) {
        if (offset < 0 || length < 0 || offset + length > switchableIndexInput.fileLength()) {
            throw new IllegalArgumentException(
                "slice() "
                    + sliceDescription
                    + " out of bounds: offset="
                    + offset
                    + ",length="
                    + length
                    + ",fileLength="
                    + switchableIndexInput.fileLength()
                    + ": "
                    + this
            );
        }
    }

    /**
     * The manager decides whether this access starts a promotion; without a manager the feature is off. A read on a
     * thread that is not one of a query pool asks for nothing.
     */
    private void requestPromotion() {
        if (manager != null && QUERY_THREAD.get()) {
            manager.requestPromotion(fullFileState);
        }
    }
}

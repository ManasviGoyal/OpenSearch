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
import org.opensearch.storage.warmfullfile.FullFileState;
import org.opensearch.storage.warmfullfile.WarmFullFileManager;

import java.io.IOException;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * A {@link SwitchableIndexInputWrapper} whose file may be promoted to a whole local file.
 *
 * <p>Only handles that passed the open-time eligibility rules are wrapped by this class. When the family is
 * {@link FullFileState.State#FULL}, {@link #clone()} and {@link #slice(String, long, long)} return a view of the
 * family's local whole-file mapping instead of a block input. Otherwise, the clone/slice asks the manager for a
 * promotion ({@link WarmFullFileManager#requestPromotion}) and a block input is returned. An input that already exists
 * is never switched.
 */
public class PromotableSwitchableIndexInputWrapper extends SwitchableIndexInputWrapper {

    private final String fullCloneDescription;
    private final FullFileState fullFileState;
    private final WarmFullFileManager manager;
    // Non-null only for handles opened by openInput: true while this handle holds its one count on the family state.
    private final AtomicBoolean handleCounted;
    private static final Logger logger = LogManager.getLogger(PromotableSwitchableIndexInputWrapper.class);

    /**
     * Constructor for a handle opened by {@code TieredDirectory.openInput}. The handle holds one count on the family
     * state until it is closed, so the family mapping is not unmapped while it may still be in use.
     *
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
        this(resourceDescription, in, fullFileState, manager, true);
    }

    private PromotableSwitchableIndexInputWrapper(
        String resourceDescription,
        SwitchableIndexInput in,
        FullFileState fullFileState,
        WarmFullFileManager manager,
        boolean countsHandle
    ) {
        super(resourceDescription, in);
        this.fullCloneDescription = "Full clone " + resourceDescription;
        this.fullFileState = fullFileState;
        this.manager = manager;
        if (countsHandle) {
            fullFileState.incHandles();
            // Released only on close(), deliberately NOT by the Cleaner: a whole-file view does not reference this
            // wrapper, so the wrapper can become unreachable while a view derived from it is still in use; a Cleaner
            // release could then let the deferred close unmap under that live view (SIGSEGV).
            this.handleCounted = new AtomicBoolean(true);
        } else {
            this.handleCounted = null;
        }
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
        // Derived wrappers share the family state but do not hold a handle count.
        return new PromotableSwitchableIndexInputWrapper(
            "Cloned " + resourceDescription,
            switchableIndexInput.clone(),
            fullFileState,
            manager,
            false
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
            manager,
            false
        );
    }

    /** Cuts one view of a published whole-file mapping. */
    @FunctionalInterface
    private interface ViewCutter {
        IndexInput cut(IndexInput master) throws IOException;
    }

    /**
     * A view of the family's whole-file mapping, or null when the handle must keep reading blocks: the family is not
     * FULL, the mapping is not published, or the mapping was closed under this read. Blocks are always valid, so every
     * fallback is silent except an unexpected I/O error, which is logged at DEBUG.
     */
    private IndexInput viewOrNull(String what, ViewCutter cutter) {
        IndexInput master = fullFileState.localMaster(); // read the volatile once
        if (fullFileState.state() != FullFileState.State.FULL || master == null) {
            return null;
        }
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

    /** The manager decides whether this access starts a promotion; without a manager the feature is off. */
    private void requestPromotion() {
        if (manager != null) {
            manager.requestPromotion(fullFileState);
        }
    }

    @Override
    public void close() throws IOException {
        try {
            super.close();
        } finally {
            if (handleCounted != null && handleCounted.compareAndSet(true, false)) {
                fullFileState.decHandles();
            }
        }
    }
}

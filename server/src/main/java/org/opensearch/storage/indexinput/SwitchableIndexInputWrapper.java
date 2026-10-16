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
import org.apache.lucene.store.FilterIndexInput;
import org.apache.lucene.store.IndexInput;
import org.apache.lucene.store.RandomAccessInput;
import org.opensearch.common.util.concurrent.OpenSearchExecutors;
import org.opensearch.storage.warmfullfile.FullFileState;
import org.opensearch.storage.warmfullfile.WarmFullFileManager;

import java.io.IOException;
import java.lang.ref.Cleaner;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * Wrapper around {@link SwitchableIndexInput} that uses a {@link java.lang.ref.Cleaner} to ensure resources are released.
 *
 * <p>Whole-file promotion: when the family is {@link FullFileState.State#FULL}, {@link #clone()} and
 * {@link #slice(String, long, long)} return a view of the family's local whole-file mapping instead of a block input.
 * Otherwise, the first clone/slice of an eligible handle triggers a background promotion and a block input is
 * returned. An input that already exists is never switched.
 */
public class SwitchableIndexInputWrapper extends FilterIndexInput implements RandomAccessInput {

    private final String resourceDescription;
    private final String fullCloneDescription;
    private final SwitchableIndexInput switchableIndexInput;
    private final FullFileState fullFileState;
    private final boolean eligible;
    private final WarmFullFileManager manager;
    // Non-null only for handles opened by openInput: true while this handle holds its one count on the family state.
    private final AtomicBoolean handleCounted;
    // Lucene's ConcurrentMergeScheduler names its threads so; OpenSearch keeps the fragment inside its own prefix.
    private static final String MERGE_THREAD_NAME_FRAGMENT = "Lucene Merge Thread";
    private static final Logger logger = LogManager.getLogger(SwitchableIndexInputWrapper.class);
    private static final Cleaner CLEANER = Cleaner.create(OpenSearchExecutors.daemonThreadFactory("index-input-cleaner"));

    /**
     * Constructor for a handle opened by {@code TieredDirectory.openInput}. The handle holds one count on the family
     * state until it is closed, so the family mapping is not unmapped while it may still be in use.
     *
     * @param resourceDescription a human-readable description
     * @param in                  the underlying switchable input
     * @param fullFileState       the family's whole-file promotion state (may be null when the feature is off)
     * @param eligible            whether this handle may trigger whole-file promotion (decided once in openInput)
     * @param manager             the node-level promotion manager (may be null when the feature is off)
     */
    public SwitchableIndexInputWrapper(
        String resourceDescription,
        SwitchableIndexInput in,
        FullFileState fullFileState,
        boolean eligible,
        WarmFullFileManager manager
    ) {
        this(resourceDescription, in, fullFileState, eligible, manager, true);
    }

    private SwitchableIndexInputWrapper(
        String resourceDescription,
        SwitchableIndexInput in,
        FullFileState fullFileState,
        boolean eligible,
        WarmFullFileManager manager,
        boolean countsHandle
    ) {
        super(resourceDescription, in);
        this.resourceDescription = resourceDescription;
        this.fullCloneDescription = "Full clone " + resourceDescription;
        this.switchableIndexInput = in;
        this.fullFileState = fullFileState;
        this.eligible = eligible;
        this.manager = manager;
        CLEANER.register(this, switchableIndexInput);
        if (countsHandle && fullFileState != null) {
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
        FullFileState st = fullFileState;
        if (st != null) {
            IndexInput view = viewOrNull(st, "clone", master -> {
                IndexInput v = master.slice(fullCloneDescription, switchableIndexInput.offset(), switchableIndexInput.fileLength());
                v.seek(switchableIndexInput.getFilePointer()); // Lucene's clone() keeps the position
                return v;
            });
            if (view != null) {
                return view;
            }
            maybeTrigger(st);
        }
        // Derived wrappers inherit eligibility and state but do not hold a handle count.
        return new SwitchableIndexInputWrapper("Cloned " + resourceDescription, switchableIndexInput.clone(), st, eligible, manager, false);
    }

    @Override
    public IndexInput slice(String sliceDescription, long offset, long length) {
        FullFileState st = fullFileState;
        if (st != null) {
            IndexInput view = viewOrNull(st, "slice", master -> {
                // The master spans the whole file, so its own bounds check is not enough: keep the slice inside this
                // handle's window, as the block path (and Lucene's slice contract) would.
                checkSliceWithinHandle(sliceDescription, offset, length);
                // A slice starts at 0, as Lucene requires; cut it at this instance's absolute offset.
                return master.slice(sliceDescription, switchableIndexInput.offset() + offset, length);
            });
            if (view != null) {
                return view;
            }
            maybeTrigger(st);
        }
        return new SwitchableIndexInputWrapper(
            "Sliced " + resourceDescription,
            switchableIndexInput.slice(sliceDescription, offset, length),
            st,
            eligible,
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
    private IndexInput viewOrNull(FullFileState st, String what, ViewCutter cutter) {
        IndexInput master = st.localMaster(); // read the volatile once
        if (st.state() != FullFileState.State.FULL || master == null) {
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

    /**
     * Requests promotion at most once per BLOCK period: only an eligible handle whose file is in remote (block) mode,
     * only in BLOCK, only after retryAt, never on a Lucene merge thread, and only for the thread that wins the CAS. The
     * trigger does a CAS and an executor submit and nothing else; promote() never runs on the search thread.
     */
    private void maybeTrigger(FullFileState st) {
        if (eligible == false || manager == null) {
            return;
        }
        // Checked at trigger time, not at open: a handle opened before the upload becomes promotable once the family
        // is in remote mode (see FullFileState.markRemoteMode for when that is).
        if (st.isRemoteMode() == false) {
            return;
        }
        if (st.state() != FullFileState.State.BLOCK || manager.relativeTimeInMillis() < st.retryAt()) {
            return;
        }
        // A merge reads a source segment once and then deletes it, so promoting for it only spends a slot, budget and
        // a download on a file with minutes to live. Merges usually read through the searcher's already-open reader,
        // whose handles passed the open-time context check, so the merge is recognised here by its thread instead.
        if (Thread.currentThread().getName().contains(MERGE_THREAD_NAME_FRAGMENT)) {
            return;
        }
        if (st.casState(FullFileState.State.BLOCK, FullFileState.State.PROMOTING)) {
            // Once per promotion attempt, not per clone or slice. The thread name shows what started it.
            logger.debug("whole-file promotion requested for {} on thread [{}]", resourceDescription, Thread.currentThread().getName());
            try {
                manager.executor().execute(() -> manager.promote(st));
            } catch (RuntimeException e) {
                // e.g. executor rejected: return to BLOCK so a later search can retry.
                st.casState(FullFileState.State.PROMOTING, FullFileState.State.BLOCK);
                logger.debug("whole-file promotion submit rejected for {}", resourceDescription, e);
            }
        }
    }

    @Override
    public byte readByte(long pos) throws IOException {
        return switchableIndexInput.readByte(pos);
    }

    @Override
    public short readShort(long pos) throws IOException {
        return switchableIndexInput.readShort(pos);
    }

    @Override
    public int readInt(long pos) throws IOException {
        return switchableIndexInput.readInt(pos);
    }

    @Override
    public long readLong(long pos) throws IOException {
        return switchableIndexInput.readLong(pos);
    }

    @Override
    public void prefetch(long offset, long length) throws IOException {
        this.switchableIndexInput.prefetch(offset, length);
    }

    @Override
    public void close() throws IOException {
        try {
            switchableIndexInput.close();
        } finally {
            if (handleCounted != null && handleCounted.compareAndSet(true, false)) {
                fullFileState.decHandles();
            }
        }
    }

    @Override
    public byte readByte() throws IOException {
        return switchableIndexInput.readByte();
    }

    @Override
    public short readShort() throws IOException {
        return switchableIndexInput.readShort();
    }

    @Override
    public int readInt() throws IOException {
        return switchableIndexInput.readInt();
    }

    @Override
    public long readLong() throws IOException {
        return switchableIndexInput.readLong();
    }

    @Override
    public int readVInt() throws IOException {
        return switchableIndexInput.readVInt();
    }

    @Override
    public long readVLong() throws IOException {
        return switchableIndexInput.readVLong();
    }

}

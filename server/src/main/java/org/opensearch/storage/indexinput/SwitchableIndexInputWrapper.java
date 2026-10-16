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
 * Otherwise, the first clone/slice of an eligible handle triggers a background promotion and the block input is
 * returned as today. Inputs that already exist are never switched.
 */
public class SwitchableIndexInputWrapper extends FilterIndexInput implements RandomAccessInput {

    private final String resourceDescription;
    private final SwitchableIndexInput switchableIndexInput;
    private final FullFileState fullFileState;
    private final boolean eligible;
    private final WarmFullFileManager manager;
    // Non-null only for handles opened by openInput: each such handle holds one count on the family state.
    private final HandleRelease handleRelease;
    private static final Logger logger = LogManager.getLogger(SwitchableIndexInputWrapper.class);
    private static final Cleaner CLEANER = Cleaner.create(OpenSearchExecutors.daemonThreadFactory("index-input-cleaner"));

    /**
     * Backward-compatible constructor; the handle is not eligible for whole-file promotion and carries no family state.
     */
    public SwitchableIndexInputWrapper(String resourceDescription, SwitchableIndexInput in) {
        this(resourceDescription, in, null, false, null, false);
    }

    /**
     * Constructor for a handle opened by {@code TieredDirectory.openInput}. The handle holds one count on the family
     * state until it is closed (or cleaned), so the family mapping is not unmapped while it may still be in use.
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
        this.switchableIndexInput = in;
        this.fullFileState = fullFileState;
        this.eligible = eligible;
        this.manager = manager;
        CLEANER.register(this, switchableIndexInput);
        if (countsHandle && fullFileState != null) {
            fullFileState.incHandles();
            // Released only on close(), deliberately NOT by the Cleaner: a whole-file
            // view does not reference this wrapper, so the wrapper can become unreachable while a view derived from it
            // is still in use; a Cleaner release could then let the deferred close unmap under that live view (SIGSEGV).
            this.handleRelease = new HandleRelease(fullFileState);
        } else {
            this.handleRelease = null;
        }
    }

    @Override
    public IndexInput clone() {
        FullFileState st = fullFileState;
        if (st != null) {
            IndexInput master = st.localMaster(); // read the volatile once
            if (st.state() == FullFileState.State.FULL && master != null) {
                try {
                    IndexInput view = master.slice(
                        "Full clone " + resourceDescription,
                        switchableIndexInput.offset(),
                        switchableIndexInput.fileLength()
                    );
                    view.seek(switchableIndexInput.getFilePointer()); // Lucene's clone() keeps the position
                    return view;
                } catch (AlreadyClosedException e) {
                    // A close raced this read; blocks are always valid, so fall through.
                } catch (IOException e) {
                    logger.debug("failed to cut whole-file view for clone, using blocks: {}", resourceDescription, e);
                }
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
            IndexInput master = st.localMaster(); // read the volatile once
            if (st.state() == FullFileState.State.FULL && master != null) {
                // The master spans the whole file, so its own bounds check is not enough: keep the slice inside this
                // handle's window, as the block path (and Lucene's slice contract) would.
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
                try {
                    // A slice starts at 0, as Lucene requires; cut it at this instance's absolute offset.
                    return master.slice(sliceDescription, switchableIndexInput.offset() + offset, length);
                } catch (AlreadyClosedException e) {
                    // A close raced this read; fall through to blocks.
                } catch (IOException e) {
                    logger.debug("failed to cut whole-file view for slice, using blocks: {}", resourceDescription, e);
                }
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

    /**
     * Requests promotion at most once per BLOCK period: only an eligible handle whose file is in remote (block) mode,
     * only in BLOCK, only after retryAt, and only for the thread that wins the CAS. The trigger does a CAS and an
     * executor submit and nothing else; promote() never runs on the search thread.
     */
    private void maybeTrigger(FullFileState st) {
        if (eligible == false || manager == null) {
            return;
        }
        // Remote mode is checked here rather than when the handle was opened: a handle opened at refresh time, before
        // the upload, becomes promotable once the family has switched to remote. The family flag is set only after the
        // switch has completed for the original and all clones and the local copy has been removed (an individual
        // input's own flag flips earlier, while the switch is still in progress), so a promotion can never overlap the
        // removal of the local copy. Remote mode is final.
        if (st.isRemoteMode() == false) {
            return;
        }
        if (st.state() != FullFileState.State.BLOCK || manager.relativeTimeInMillis() < st.retryAt()) {
            return;
        }
        // A merge reads a source segment once and then deletes it, so promoting for it only spends a slot, budget and
        // a download on a file with minutes to live. Merges usually read through the searcher's already-open reader,
        // whose handles passed the open-time context check, so the merge is recognised here by its thread instead:
        // Lucene names merge threads "Lucene Merge Thread #N" and OpenSearch keeps that fragment in its prefixed name.
        if (Thread.currentThread().getName().contains("Lucene Merge Thread")) {
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
            if (handleRelease != null) {
                handleRelease.run();
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

    /** Releases one handle count exactly once, however many times {@link #close()} is called. */
    private static final class HandleRelease implements Runnable {
        private final FullFileState state;
        private final AtomicBoolean released = new AtomicBoolean(false);

        HandleRelease(FullFileState state) {
            this.state = state;
        }

        @Override
        public void run() {
            if (released.compareAndSet(false, true)) {
                state.decHandles();
            }
        }
    }
}

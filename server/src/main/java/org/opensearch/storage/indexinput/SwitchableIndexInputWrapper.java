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
import org.apache.lucene.store.FilterIndexInput;
import org.apache.lucene.store.IndexInput;
import org.apache.lucene.store.RandomAccessInput;
import org.opensearch.common.util.concurrent.OpenSearchExecutors;
import org.opensearch.storage.warmfullfile.FullFileState;

import java.io.IOException;
import java.lang.ref.Cleaner;

/**
 * Wrapper around {@link SwitchableIndexInput} that uses a {@link java.lang.ref.Cleaner} to ensure resources are released.
 */
public class SwitchableIndexInputWrapper extends FilterIndexInput implements RandomAccessInput {

    private final String resourceDescription;
    private final SwitchableIndexInput switchableIndexInput;
    private final FullFileState fullFileState;
    private final boolean eligible;
    private static final Logger logger = LogManager.getLogger(SwitchableIndexInputWrapper.class);
    private static final Cleaner CLEANER = Cleaner.create(OpenSearchExecutors.daemonThreadFactory("index-input-cleaner"));

    /**
     * Backward-compatible constructor; the handle is not eligible for whole-file promotion and carries no family state.
     */
    public SwitchableIndexInputWrapper(String resourceDescription, SwitchableIndexInput in) {
        this(resourceDescription, in, null, false);
    }

    /**
     * @param resourceDescription a human-readable description
     * @param in                  the underlying switchable input
     * @param fullFileState       the family's whole-file promotion state (may be null when the feature is off)
     * @param eligible            whether this handle may trigger whole-file promotion (decided once in openInput)
     */
    public SwitchableIndexInputWrapper(String resourceDescription, SwitchableIndexInput in, FullFileState fullFileState, boolean eligible) {
        super(resourceDescription, in);
        this.resourceDescription = resourceDescription;
        this.switchableIndexInput = in;
        this.fullFileState = fullFileState;
        this.eligible = eligible;
        CLEANER.register(this, switchableIndexInput);
    }

    @Override
    public IndexInput clone() {
        // Wrapper clones inherit the eligibility flag and family state (serve path is unchanged until step 5).
        return new SwitchableIndexInputWrapper("Cloned " + resourceDescription, switchableIndexInput.clone(), fullFileState, eligible);
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
    public IndexInput slice(String sliceDescription, long offset, long length) {
        // Wrapper slices inherit the eligibility flag and family state (serve path is unchanged until step 5).
        return new SwitchableIndexInputWrapper(
            "Sliced " + resourceDescription,
            switchableIndexInput.slice(sliceDescription, offset, length),
            fullFileState,
            eligible
        );
    }

    @Override
    public void close() throws IOException {
        switchableIndexInput.close();
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

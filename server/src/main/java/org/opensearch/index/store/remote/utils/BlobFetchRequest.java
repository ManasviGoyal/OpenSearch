/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.index.store.remote.utils;

import org.apache.lucene.store.Directory;
import org.apache.lucene.store.FSDirectory;

import java.nio.file.Path;
import java.util.List;

/**
 * The specification to fetch specific block from blob store
 *
 * @opensearch.internal
 */
public class BlobFetchRequest {

    private final Path filePath;

    private final Directory directory;

    private final String fileName;

    private final List<BlobPart> blobParts;

    private final long blobLength;

    private final boolean atomicWrite;

    private final long localBlockSize;

    private final PartListener partListener;

    /** Told of every part written to the target, and where its bytes came from. */
    @FunctionalInterface
    public interface PartListener {
        void onPart(long bytes, boolean fromLocalBlock);
    }

    private BlobFetchRequest(Builder builder) {
        this.fileName = builder.fileName;
        this.filePath = builder.directory.getDirectory().resolve(fileName);
        this.directory = builder.directory;
        this.blobParts = builder.blobParts;
        this.blobLength = builder.blobParts.stream().mapToLong(o -> o.getLength()).sum();
        this.atomicWrite = builder.atomicWrite;
        this.localBlockSize = builder.localBlockSize;
        this.partListener = builder.partListener;
    }

    public Path getFilePath() {
        return filePath;
    }

    public Directory getDirectory() {
        return directory;
    }

    public String getFileName() {
        return fileName;
    }

    public List<BlobPart> blobParts() {
        return blobParts;
    }

    public long getBlobLength() {
        return blobLength;
    }

    /**
     * Whether the download is written to {@link #getTempFilePath()}, fsynced and then renamed to {@link #getFilePath()},
     * so that a file at the final path is always complete. Off by default: the file is written in place.
     */
    public boolean isAtomicWrite() {
        return atomicWrite;
    }

    /**
     * The path an atomic-write download is written to before the rename: next to the final path, with
     * {@link FileTypeUtils#PARTIAL_DOWNLOAD_SUFFIX}.
     */
    public Path getTempFilePath() {
        return filePath.resolveSibling(fileName + FileTypeUtils.PARTIAL_DOWNLOAD_SUFFIX);
    }

    /**
     * The size of the blocks of this file the file cache may hold, when a part that is exactly one such block may be
     * copied from it instead of read from the remote store; 0 when every part is read from the remote store.
     */
    public long getLocalBlockSize() {
        return localBlockSize;
    }

    /** Reports a part written to the target: its length and whether it was copied from a local block or read remotely. */
    public void onPartWritten(long bytes, boolean fromLocalBlock) {
        if (partListener != null) {
            partListener.onPart(bytes, fromLocalBlock);
        }
    }

    public static Builder builder() {
        return new Builder();
    }

    @Override
    public String toString() {
        return "BlobFetchRequest{"
            + "blobParts="
            + blobParts
            + '\''
            + ", filePath="
            + filePath
            + ", directory="
            + directory
            + ", fileName='"
            + fileName
            + '\''
            + ", atomicWrite="
            + atomicWrite
            + ", localBlockSize="
            + localBlockSize
            + '}';
    }

    /**
     * BlobPart represents a single chunk of a file
     */
    public static class BlobPart {
        private String blobName;
        private long position;
        private long length;

        public BlobPart(String blobName, long position, long length) {
            this.blobName = blobName;
            if (length <= 0) {
                throw new IllegalArgumentException("Length for blob part fetch request needs to be non-negative");
            }
            this.length = length;
            this.position = position;
        }

        public String getBlobName() {
            return blobName;
        }

        public long getPosition() {
            return position;
        }

        public long getLength() {
            return length;
        }
    }

    /**
     * Builder for BlobFetchRequest
     */
    public static final class Builder {
        private List<BlobPart> blobParts;
        private FSDirectory directory;
        private String fileName;
        private boolean atomicWrite;
        private long localBlockSize;
        private PartListener partListener;

        private Builder() {}

        public Builder directory(FSDirectory directory) {
            this.directory = directory;
            return this;
        }

        public Builder fileName(String fileName) {
            this.fileName = fileName;
            return this;
        }

        public Builder blobParts(List<BlobPart> blobParts) {
            this.blobParts = blobParts;
            return this;
        }

        /** See {@link BlobFetchRequest#isAtomicWrite()}. */
        public Builder atomicWrite(boolean atomicWrite) {
            this.atomicWrite = atomicWrite;
            return this;
        }

        /**
         * Lets a part that is exactly one block of {@code blockSize} bytes be copied from the block the file cache
         * holds for it, if any, instead of read from the remote store. Correct only when the parts are block-sized
         * and block-aligned, as the whole-file promotion builds them.
         *
         * @param blockSize the block size the file cache uses for this file
         */
        public Builder reuseLocalBlocks(long blockSize) {
            this.localBlockSize = blockSize;
            return this;
        }

        /** Told of every part the download writes, with its length and whether it came from a local block. */
        public Builder partListener(PartListener partListener) {
            this.partListener = partListener;
            return this;
        }

        public BlobFetchRequest build() {
            return new BlobFetchRequest(this);
        }
    }
}

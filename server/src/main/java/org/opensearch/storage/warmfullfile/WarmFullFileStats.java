/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */
package org.opensearch.storage.warmfullfile;

import org.opensearch.common.annotation.ExperimentalApi;

import java.util.Collections;
import java.util.EnumMap;
import java.util.Map;

/**
 * A point-in-time snapshot of a node's whole-file promotion counters, taken from {@link WarmFullFileManager#stats()}.
 * Cumulative counters never decrease for the life of the node; {@link #reservedBytes()},
 * {@link #reservedBytesPerSegment()} and {@link #activeDownloads()} describe the current moment.
 *
 * @opensearch.experimental
 */
@ExperimentalApi
public final class WarmFullFileStats {

    /**
     * Why a promotion did not reach FULL. Each decline or failure is counted under exactly one reason.
     *
     * @opensearch.experimental
     */
    @ExperimentalApi
    public enum DeclineReason {
        /** Every download slot was in use. */
        NO_SLOT,
        /** The file did not fit the per-segment whole-file budget. */
        BUDGET_EXCEEDED,
        /** The file is not below the local directory's mmap chunk size. */
        TOO_LARGE,
        /** The local directory serves the file with an input that is not Lucene's memory-mapped input. */
        NOT_MMAP,
        /** A remote read failed. */
        DOWNLOAD_FAILED,
        /** The downloaded file does not have the expected length. */
        LENGTH_MISMATCH,
        /** The whole-file cache entry was evicted before the family reached FULL. */
        ENTRY_EVICTED,
        /** The family closed while the download or the publish was in progress. */
        FAMILY_CLOSED,
        /** An unexpected exception while starting the download or publishing the mapping. */
        ERROR
    }

    private final long requested;
    private final long started;
    private final long promoted;
    private final long downloadedBytes;
    private final long promotedBytes;
    private final long downloadMillis;
    private final Map<DeclineReason, Long> declined;
    private final long[] reservedBytesPerSegment;
    private final int activeDownloads;

    WarmFullFileStats(
        long requested,
        long started,
        long promoted,
        long downloadedBytes,
        long promotedBytes,
        long downloadMillis,
        Map<DeclineReason, Long> declined,
        long[] reservedBytesPerSegment,
        int activeDownloads
    ) {
        this.requested = requested;
        this.started = started;
        this.promoted = promoted;
        this.downloadedBytes = downloadedBytes;
        this.promotedBytes = promotedBytes;
        this.downloadMillis = downloadMillis;
        this.declined = Collections.unmodifiableMap(new EnumMap<>(declined));
        this.reservedBytesPerSegment = reservedBytesPerSegment.clone();
        this.activeDownloads = activeDownloads;
    }

    /** @return the number of promotion attempts handed to the manager, including those that found the family already closed */
    public long requested() {
        return requested;
    }

    /** @return the number of downloads begun after admission (a free slot and budget) */
    public long started() {
        return started;
    }

    /** @return the number of families that reached FULL */
    public long promoted() {
        return promoted;
    }

    /**
     * @return the number of started downloads that did not reach FULL: declined after the download for not being
     *         memory-mapped, a failed read, a wrong length, an evicted entry, a closed family, or an error
     */
    public long failed() {
        return count(DeclineReason.NOT_MMAP) + count(DeclineReason.DOWNLOAD_FAILED) + count(DeclineReason.LENGTH_MISMATCH) + count(
            DeclineReason.ENTRY_EVICTED
        ) + count(DeclineReason.FAMILY_CLOSED) + count(DeclineReason.ERROR);
    }

    private long count(DeclineReason reason) {
        Long value = declined.get(reason);
        return value == null ? 0L : value;
    }

    /** @return the total length of every completed download, whether or not the file then reached FULL */
    public long downloadedBytes() {
        return downloadedBytes;
    }

    /** @return the total length of the files that reached FULL */
    public long promotedBytes() {
        return promotedBytes;
    }

    /** @return the summed wall-clock time, in milliseconds, from the start of a download to FULL, over promoted files */
    public long downloadMillis() {
        return downloadMillis;
    }

    /** @return the number of promotions that did not reach FULL, by reason; every reason is present, possibly with zero */
    public Map<DeclineReason, Long> declined() {
        return declined;
    }

    /** @return the number of promotions that did not reach FULL, all reasons together */
    public long totalDeclined() {
        long total = 0;
        for (long count : declined.values()) {
            total += count;
        }
        return total;
    }

    /** @return whole-file bytes currently charged against the per-segment budgets (downloading or FULL), node-wide */
    public long reservedBytes() {
        long total = 0;
        for (long bytes : reservedBytesPerSegment) {
            total += bytes;
        }
        return total;
    }

    /** @return whole-file bytes currently charged against each cache segment's budget, indexed by segment */
    public long[] reservedBytesPerSegment() {
        return reservedBytesPerSegment.clone();
    }

    /** @return downloads currently holding a slot */
    public int activeDownloads() {
        return activeDownloads;
    }

    @Override
    public String toString() {
        return "WarmFullFileStats{requested="
            + requested
            + ", started="
            + started
            + ", promoted="
            + promoted
            + ", failed="
            + failed()
            + ", downloadedBytes="
            + downloadedBytes
            + ", promotedBytes="
            + promotedBytes
            + ", downloadMillis="
            + downloadMillis
            + ", declined="
            + declined
            + ", reservedBytes="
            + reservedBytes()
            + ", activeDownloads="
            + activeDownloads
            + '}';
    }
}

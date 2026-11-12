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
import org.opensearch.common.lease.Releasable;

import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLongArray;

/**
 * The per-cache-segment whole-file budget: the bytes of whole files each segment of the file cache may hold.
 * {@link #reserve} admits a file only while its segment stays within the cap, and returns the reservation as a
 * {@link Releasable} that gives the bytes back once, however often it is closed. A segment's count never goes below
 * zero, so a release that does not match a reservation cannot disable the cap.
 */
final class SegmentBudget {

    private static final Logger logger = LogManager.getLogger(SegmentBudget.class);

    private final AtomicLongArray reserved;
    private final long capPerSegment;

    /**
     * @param segments      the file cache's segment count
     * @param capPerSegment the most whole-file bytes one segment may hold
     */
    SegmentBudget(int segments, long capPerSegment) {
        this.reserved = new AtomicLongArray(segments);
        this.capPerSegment = capPerSegment;
    }

    /**
     * Reserves {@code bytes} in {@code segment} if the segment stays at or below its cap.
     *
     * @return the reservation, released by closing it, or null when the bytes would take the segment over its cap
     */
    Releasable reserve(int segment, long bytes) {
        while (true) {
            long current = reserved.get(segment);
            if (current + bytes > capPerSegment) {
                return null;
            }
            if (reserved.compareAndSet(segment, current, current + bytes)) {
                return new Reservation(segment, bytes);
            }
        }
    }

    /** @return the most whole-file bytes one segment may hold */
    long capPerSegment() {
        return capPerSegment;
    }

    /** @return the number of segments */
    int segments() {
        return reserved.length();
    }

    /** @return the bytes currently reserved in each segment, by segment index */
    long[] reservedPerSegment() {
        long[] snapshot = new long[reserved.length()];
        for (int i = 0; i < snapshot.length; i++) {
            snapshot[i] = reserved.get(i);
        }
        return snapshot;
    }

    /** @return the bytes currently reserved across all segments */
    long totalReserved() {
        long total = 0;
        for (int i = 0; i < reserved.length(); i++) {
            total += reserved.get(i);
        }
        return total;
    }

    private void release(int segment, long bytes) {
        while (true) {
            long current = reserved.get(segment);
            long next = Math.max(0, current - bytes);
            if (reserved.compareAndSet(segment, current, next)) {
                assert current - bytes >= 0 : "segment " + segment + " releases " + bytes + " whole-file bytes but holds " + current;
                if (current - bytes < 0) {
                    logger.warn(
                        "whole-file budget of cache segment {} released {} bytes but held {}; kept at zero",
                        segment,
                        bytes,
                        current
                    );
                }
                return;
            }
        }
    }

    /** One reservation; closing it gives the bytes back exactly once. */
    private final class Reservation implements Releasable {
        private final int segment;
        private final long bytes;
        private final AtomicBoolean released = new AtomicBoolean(false);

        Reservation(int segment, long bytes) {
            this.segment = segment;
            this.bytes = bytes;
        }

        @Override
        public void close() {
            if (released.compareAndSet(false, true)) {
                release(segment, bytes);
            }
        }
    }
}

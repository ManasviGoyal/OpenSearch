/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.storage.warmfullfile;

import org.opensearch.common.lease.Releasable;
import org.opensearch.test.OpenSearchTestCase;

public class SegmentBudgetTests extends OpenSearchTestCase {

    public void testAdmitsUpToTheCapAndRefusesBeyondIt() {
        SegmentBudget budget = new SegmentBudget(2, 100);
        assertEquals(100, budget.capPerSegment());
        assertEquals(2, budget.segments());

        Releasable first = budget.reserve(0, 60);
        assertNotNull(first);
        assertNull("40 left, 50 asked", budget.reserve(0, 50));
        Releasable second = budget.reserve(0, 40);
        assertNotNull("exactly the cap is admitted", second);
        assertNull("nothing left", budget.reserve(0, 1));
        assertArrayEquals(new long[] { 100, 0 }, budget.reservedPerSegment());
        assertEquals(100, budget.totalReserved());

        first.close();
        assertArrayEquals(new long[] { 40, 0 }, budget.reservedPerSegment());
        assertNotNull("room again after a release", budget.reserve(0, 60));
        second.close();
    }

    public void testSegmentsAreIndependent() {
        SegmentBudget budget = new SegmentBudget(3, 10);
        assertNotNull(budget.reserve(0, 10));
        assertNull("segment 0 is full", budget.reserve(0, 1));
        assertNotNull("segment 1 is untouched", budget.reserve(1, 10));
        assertNotNull(budget.reserve(2, 5));
        assertArrayEquals(new long[] { 10, 10, 5 }, budget.reservedPerSegment());
        assertEquals(25, budget.totalReserved());
    }

    public void testAReservationIsReleasedOnce() {
        // Closing the same reservation again must not give the bytes back a second time: a double release would let
        // the segment admit more than its cap.
        SegmentBudget budget = new SegmentBudget(1, 100);
        Releasable kept = budget.reserve(0, 70);
        Releasable released = budget.reserve(0, 30);
        assertEquals(100, budget.totalReserved());

        released.close();
        released.close();
        released.close();

        assertEquals("only the released reservation's bytes came back", 70, budget.totalReserved());
        assertNull("the kept reservation still blocks a second file", budget.reserve(0, 31));
        assertNotNull(budget.reserve(0, 30));
        kept.close();
    }
}

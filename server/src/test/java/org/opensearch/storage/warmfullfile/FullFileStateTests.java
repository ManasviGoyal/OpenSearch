/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */
package org.opensearch.storage.warmfullfile;

import org.opensearch.test.OpenSearchTestCase;

import java.nio.file.Path;

public class FullFileStateTests extends OpenSearchTestCase {

    private FullFileState newState() {
        // The reference-type fields are only consumed by the manager (step 4); null is fine for state-machine tests.
        return new FullFileState(null, null, null, null, "_5.faiss", 1_700_000_000L, Path.of("/cache/_5.faiss"));
    }

    public void testStartsInBlock() {
        FullFileState st = newState();
        assertEquals(FullFileState.State.BLOCK, st.state());
        assertNull(st.localMaster());
        assertEquals(0, st.handleCount());
        assertEquals(0L, st.retryAt());
    }

    public void testCasTransitions() {
        FullFileState st = newState();
        assertTrue(st.casState(FullFileState.State.BLOCK, FullFileState.State.PROMOTING));
        assertEquals(FullFileState.State.PROMOTING, st.state());
        // a CAS from the wrong expected value must fail and not change the state
        assertFalse(st.casState(FullFileState.State.BLOCK, FullFileState.State.FULL));
        assertEquals(FullFileState.State.PROMOTING, st.state());
        assertTrue(st.casState(FullFileState.State.PROMOTING, FullFileState.State.FULL));
        assertEquals(FullFileState.State.FULL, st.state());
    }

    public void testGetAndSetClosedReturnsPrevious() {
        FullFileState st = newState();
        st.casState(FullFileState.State.BLOCK, FullFileState.State.PROMOTING);
        st.casState(FullFileState.State.PROMOTING, FullFileState.State.FULL);
        assertEquals(FullFileState.State.FULL, st.getAndSetClosed());
        assertEquals(FullFileState.State.CLOSED, st.state());
    }

    public void testHandleCount() {
        FullFileState st = newState();
        assertEquals(1, st.incHandles());
        assertEquals(2, st.incHandles());
        assertEquals(1, st.decHandles());
        assertEquals(0, st.decHandles());
        assertEquals(0, st.handleCount());
    }

    public void testRetryAt() {
        FullFileState st = newState();
        st.setRetryAt(12345L);
        assertEquals(12345L, st.retryAt());
    }

    public void testFileLengthAccessor() {
        FullFileState st = newState();
        assertEquals(1_700_000_000L, st.fileLength());
    }
}

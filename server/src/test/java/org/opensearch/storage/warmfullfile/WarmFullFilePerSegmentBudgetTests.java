/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */
package org.opensearch.storage.warmfullfile;

import org.apache.lucene.store.FSDirectory;
import org.apache.lucene.store.IndexInput;
import org.opensearch.index.store.remote.filecache.CachedIndexInput;
import org.opensearch.index.store.remote.filecache.FileCache;
import org.opensearch.index.store.remote.filecache.FileCacheFactory;
import org.opensearch.index.store.remote.utils.BlobFetchRequest;
import org.opensearch.index.store.remote.utils.TransferManager;
import org.opensearch.test.OpenSearchTestCase;
import org.opensearch.threadpool.TestThreadPool;
import org.opensearch.threadpool.ThreadPool;
import org.junit.After;
import org.junit.Before;

import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

/**
 * The per-segment whole-file budget against a real multi-segment {@link FileCache}: whole files are admitted up to half
 * of the segment they hash to, and block fetches into the same segment keep working beside them.
 */
public class WarmFullFilePerSegmentBudgetTests extends OpenSearchTestCase {

    private static final int SEGMENTS = 4;
    private static final long CAPACITY = 80_000;
    private static final long PART_SIZE = 1024;
    private static final long FILE_LENGTH = 4096;

    private ThreadPool threadPool;
    private FSDirectory localDirectory;
    private Path dir;
    private FileCache fileCache;
    private WarmFullFileManagerTests.PositionDependentStreamReader reader;
    private TransferManager transferManager;
    private AtomicLong clock;
    private WarmFullFileManager manager;
    private long perSegmentCapacity;
    private long cap;
    private int target;

    @Before
    public void setUpHarness() throws Exception {
        threadPool = new TestThreadPool("WarmFullFilePerSegmentBudgetTests");
        dir = createTempDir();
        localDirectory = FSDirectory.open(dir);
        // concurrencyLevel 4 -> 4 segments of 20,000 bytes; the manager budgets half of each (10,000 bytes).
        fileCache = FileCacheFactory.createConcurrentLRUFileCache(CAPACITY, SEGMENTS);
        assertEquals(CAPACITY, fileCache.capacity());
        assertEquals(SEGMENTS, fileCache.segmentCount());
        reader = new WarmFullFileManagerTests.PositionDependentStreamReader();
        transferManager = new TransferManager(reader, fileCache, threadPool);
        clock = new AtomicLong(10_000);
        manager = new WarmFullFileManager(fileCache, Runnable::run, clock::get, 8, 0.5, PART_SIZE);
        manager.onExtensionsChanged(Set.of("faiss")); // as the settings listener tells it in production
        perSegmentCapacity = CAPACITY / SEGMENTS;
        cap = perSegmentCapacity / 2;
        target = fileCache.segmentIndexFor(dir.resolve("_t0.faiss"));
    }

    @After
    public void tearDownHarness() throws Exception {
        localDirectory.close();
        threadPool.shutdownNow();
    }

    /** Whole-file names (as cache keys) that hash to the target segment. */
    private List<String> wholeFileNamesInTargetSegment(int count) {
        List<String> names = new ArrayList<>();
        for (int i = 0; names.size() < count; i++) {
            String name = "_t" + i + ".faiss";
            if (fileCache.segmentIndexFor(dir.resolve(name)) == target) {
                names.add(name);
            }
        }
        return names;
    }

    /** A file name whose first block key ({@code name + "_block_0"}) hashes to the target segment. */
    private String blockFileNameInTargetSegment() {
        for (int i = 0;; i++) {
            String name = "_b" + i + ".faiss";
            if (fileCache.segmentIndexFor(dir.resolve(name + "_block_0")) == target) {
                return name;
            }
        }
    }

    private FullFileState newState(String name) {
        return new FullFileState(transferManager, localDirectory, fileCache, name, FILE_LENGTH, dir.resolve(name));
    }

    private void awaitState(FullFileState st, FullFileState.State state) throws Exception {
        assertBusy(() -> assertEquals(state, st.state()), 10, TimeUnit.SECONDS);
    }

    /** Promotes whole files into the target segment until the next one would exceed the budget; returns the admitted ones. */
    private List<FullFileState> fillTargetSegmentToBudget() throws Exception {
        int admitted = (int) (cap / FILE_LENGTH);
        assertTrue("the budget must hold at least one file", admitted >= 1);
        List<String> names = wholeFileNamesInTargetSegment(admitted + 1);
        List<FullFileState> full = new ArrayList<>();
        for (int i = 0; i < admitted; i++) {
            FullFileState st = newState(names.get(i));
            assertTrue(st.casState(FullFileState.State.BLOCK, FullFileState.State.PROMOTING));
            manager.promote(st);
            awaitState(st, FullFileState.State.FULL);
            full.add(st);
        }
        assertEquals("one budget counter per cache segment", SEGMENTS, manager.stats().reservedBytesPerSegment().length);
        assertEquals(admitted * FILE_LENGTH, manager.stats().reservedBytesPerSegment()[target]);
        assertEquals("one read per part per file", admitted * (FILE_LENGTH / PART_SIZE), reader.readCount());

        // The next file in the same segment would push the segment past half its capacity: declined.
        int readsBefore = reader.readCount();
        FullFileState declined = newState(names.get(admitted));
        assertTrue(declined.casState(FullFileState.State.BLOCK, FullFileState.State.PROMOTING));
        manager.promote(declined);
        assertEquals(FullFileState.State.BLOCK, declined.state());
        assertEquals(clock.get() + WarmFullFileManager.RETRY_DELAY_MILLIS, declined.retryAt());
        assertNull("nothing published for the declined file", declined.localMaster());
        assertEquals("no read for the declined file", readsBefore, reader.readCount());
        assertNull("no cache entry for the declined file", fileCache.get(declined.fullFilePath()));
        return full;
    }

    public void testAdmissionStopsAtHalfTheSegment() throws Exception {
        List<FullFileState> full = fillTargetSegmentToBudget();

        long wholeFileBytes = manager.stats().reservedBytesPerSegment()[target];
        assertTrue("whole-file bytes " + wholeFileBytes + " must stay within the budget " + cap, wholeFileBytes <= cap);
        assertTrue("budget is strictly below the segment capacity", cap < perSegmentCapacity);
        assertEquals(full.size() * FILE_LENGTH, wholeFileBytes);
        // Other segments are untouched.
        for (int s = 0; s < SEGMENTS; s++) {
            if (s != target) {
                assertEquals("segment " + s, 0L, manager.stats().reservedBytesPerSegment()[s]);
            }
        }
        for (FullFileState st : full) {
            assertNotNull(fileCache.get(st.fullFilePath()));
            fileCache.decRef(st.fullFilePath());
            assertEquals(Integer.valueOf(1), fileCache.getRef(st.fullFilePath()));
        }
    }

    public void testBlockFetchIntoFullBudgetSegmentStillSucceeds() throws Exception {
        List<FullFileState> full = fillTargetSegmentToBudget();
        long usageBefore = fileCache.usage();

        // A block of ANOTHER file whose block key hashes to the same segment: fetched normally, nothing evicted.
        String blockFile = blockFileNameInTargetSegment();
        String blockKeyName = blockFile + "_block_0";
        assertEquals(target, fileCache.segmentIndexFor(dir.resolve(blockKeyName)));
        BlobFetchRequest blockRequest = BlobFetchRequest.builder()
            .directory(localDirectory)
            .fileName(blockKeyName)
            .blobParts(List.of(new BlobFetchRequest.BlobPart(blockFile, 0, PART_SIZE)))
            .build();
        int readsBefore = reader.readCount();
        try (IndexInput block = transferManager.fetchBlob(blockRequest)) {
            assertEquals(PART_SIZE, block.length());
            byte[] actual = new byte[(int) PART_SIZE];
            block.readBytes(actual, 0, (int) PART_SIZE);
            byte[] expected = new byte[(int) PART_SIZE];
            for (int i = 0; i < expected.length; i++) {
                expected[i] = (byte) (i & 0xff);
            }
            assertArrayEquals("block bytes come from the reader at offset 0", expected, actual);
        }
        assertEquals("the block was read once", readsBefore + 1, reader.readCount());

        assertTrue("usage within capacity", fileCache.usage() <= fileCache.capacity());
        assertEquals("the block was added beside the whole files", usageBefore + PART_SIZE, fileCache.usage());
        CachedIndexInput blockEntry = fileCache.get(dir.resolve(blockKeyName));
        assertNotNull("block entry present", blockEntry);
        fileCache.decRef(dir.resolve(blockKeyName));
        for (FullFileState st : full) {
            assertEquals("whole file still FULL", FullFileState.State.FULL, st.state());
            assertNotNull("whole file still cached: " + st.fullFilePath(), fileCache.get(st.fullFilePath()));
            fileCache.decRef(st.fullFilePath());
        }
        assertEquals(
            "whole-file budget unchanged by the block fetch",
            full.size() * FILE_LENGTH,
            manager.stats().reservedBytesPerSegment()[target]
        );
    }
}

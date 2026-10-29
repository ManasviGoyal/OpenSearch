/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.storage;

import org.apache.lucene.store.AlreadyClosedException;
import org.opensearch.action.NoShardAvailableActionException;
import org.opensearch.action.admin.cluster.health.ClusterHealthResponse;
import org.opensearch.action.admin.indices.close.CloseIndexResponse;
import org.opensearch.action.admin.indices.open.OpenIndexResponse;
import org.opensearch.action.search.SearchPhaseExecutionException;
import org.opensearch.action.search.SearchResponse;
import org.opensearch.action.search.ShardSearchFailure;
import org.opensearch.cluster.node.DiscoveryNode;
import org.opensearch.cluster.routing.allocation.command.MoveAllocationCommand;
import org.opensearch.common.Priority;
import org.opensearch.common.settings.Settings;
import org.opensearch.common.unit.TimeValue;
import org.opensearch.index.IndexNotFoundException;
import org.opensearch.index.IndexSettings;
import org.opensearch.index.query.QueryBuilders;
import org.opensearch.index.shard.IllegalIndexShardStateException;
import org.opensearch.index.shard.ShardNotFoundException;
import org.opensearch.index.store.remote.filecache.FileCache;
import org.opensearch.indices.IndexClosedException;
import org.opensearch.indices.ShardLimitValidator;
import org.opensearch.search.SearchContextMissingException;
import org.opensearch.search.SearchHit;
import org.opensearch.search.sort.SortOrder;
import org.opensearch.storage.warmfullfile.FullFileState;
import org.opensearch.storage.warmfullfile.WarmFullFileStats;
import org.opensearch.test.InternalTestCluster;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;

import static org.opensearch.test.hamcrest.OpenSearchAssertions.assertAcked;
import static org.opensearch.test.hamcrest.OpenSearchAssertions.assertHitCount;
import static org.opensearch.test.hamcrest.OpenSearchAssertions.assertNoFailures;
import static org.hamcrest.Matchers.greaterThan;
import static org.hamcrest.Matchers.greaterThanOrEqualTo;
import static org.hamcrest.Matchers.lessThanOrEqualTo;

/**
 * Integration tests for warm-tier whole-file promotion of {@code .fdt} (stored fields) files served by mmap.
 */
public class WarmFullFileIT extends WarmFullFileBaseIT {

    private static final String INDEX_NAME = "warm-full-file-idx";
    private static final String SECOND_INDEX_NAME = "warm-full-file-idx-2";

    // The cluster holds two shards, far below the default limit of 1000 per node. Updating this setting is only a way
    // to publish a cluster state change while a download is held, so any value other than the default works.
    private static final int RAISED_MAX_SHARDS_PER_NODE = 1500;

    /** Indexes {@link #NUM_DOCS} large documents, force-merges to one segment and waits for block mode. */
    private void populateAndMergeToBlocks(String index, WarmShard warmShard) throws Exception {
        indexDocs(index, 0, NUM_DOCS);
        mergeToBlocks(index, warmShard);
    }

    private SearchResponse searchWithSource(String index) {
        SearchResponse response = client().prepareSearch(index).setQuery(QueryBuilders.matchAllQuery()).setSize(10).get();
        assertHitCount(response, NUM_DOCS);
        return response;
    }

    /** Searches (each search clones the stored-fields input) until the file is promoted. */
    private void searchUntilPromoted(String index, WarmShard warmShard, String fdt) throws Exception {
        assertBusy(() -> {
            searchWithSource(index);
            assertTrue("whole file not promoted yet, family state " + stateOf(warmShard, fdt), isPromoted(warmShard, fdt));
        }, DOWNLOAD_TIMEOUT.seconds(), TimeUnit.SECONDS);
    }

    /**
     * A fingerprint of a few queries: hit count, then id, score and the hash and length of the _source of every hit. The
     * queries are deterministic for an unchanged single-segment index.
     */
    private List<String> queryFingerprint(String index) {
        List<String> fingerprint = new ArrayList<>();
        fingerprint.add(
            describe(
                client().prepareSearch(index)
                    .setQuery(QueryBuilders.matchAllQuery())
                    .addSort("_doc", SortOrder.ASC)
                    .setTrackScores(true)
                    .setSize(50)
                    .get()
            )
        );
        fingerprint.add(describe(client().prepareSearch(index).setQuery(QueryBuilders.termQuery("tag", "t3")).setSize(50).get()));
        fingerprint.add(describe(client().prepareSearch(index).setQuery(QueryBuilders.termQuery("seq", 17)).setSize(5).get()));
        return fingerprint;
    }

    private static String describe(SearchResponse response) {
        assertEquals(0, response.getFailedShards());
        StringBuilder sb = new StringBuilder().append(response.getHits().getTotalHits().value()).append(':');
        for (SearchHit hit : response.getHits().getHits()) {
            String source = hit.getSourceAsString();
            assertNotNull("hit without _source: " + hit.getId(), source);
            sb.append(hit.getId())
                .append('|')
                .append(Float.floatToIntBits(hit.getScore()))
                .append('|')
                .append(source.length())
                .append('|')
                .append(source.hashCode())
                .append(',');
        }
        return sb.toString();
    }

    /**
     * Creates the index, fills it, waits for block mode, enables promotion of .fdt files and re-opens the index so the
     * reader's handles are opened from blocks. Returns the warm shard after the re-open.
     */
    private WarmShard prepareEligibleIndex(String index, String warmNode) throws Exception {
        createWarmIndex(index);
        populateAndMergeToBlocks(index, warmShard(index, warmNode));
        setFullFileExtensions(FDT_EXTENSION);
        reopenIndex(index);
        WarmShard warmShard = warmShard(index, warmNode);
        waitForBlockMode(warmShard);
        return warmShard;
    }

    /** A download hook that signals {@code entered} and then blocks until {@code release} is counted down. */
    private static Runnable holdDownload(CountDownLatch entered, CountDownLatch release) {
        return () -> {
            entered.countDown();
            try {
                release.await(DOWNLOAD_TIMEOUT.seconds(), TimeUnit.SECONDS);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
        };
    }

    public void testEnablingExtensionsAfterOpenPromotesWithoutReopen() throws Exception {
        // The reader's handles are opened while the setting is empty. Adding the extension afterwards is enough: the
        // next search promotes, with no index close/open.
        String warmNode = startCluster();
        createWarmIndex(INDEX_NAME);
        populateAndMergeToBlocks(INDEX_NAME, warmShard(INDEX_NAME, warmNode));
        reopenIndex(INDEX_NAME);
        WarmShard warmShard = warmShard(INDEX_NAME, warmNode);
        waitForBlockMode(warmShard);
        String fdt = eligibleFdt(warmShard);
        for (int i = 0; i < 5; i++) {
            searchWithSource(INDEX_NAME);
            assertEquals("nothing promotes while the setting is empty", FullFileState.State.BLOCK, stateOf(warmShard, fdt));
        }

        setFullFileExtensions(FDT_EXTENSION);
        searchUntilPromoted(INDEX_NAME, warmShard, fdt);
        assertEquals(FullFileState.State.FULL, stateOf(warmShard, fdt));

        deleteIndex(INDEX_NAME);
    }

    public void testCloseAndOpenPromotesAgainFromTheWholeFileLeftInTheCache() throws Exception {
        // After a close and open on the same node the whole file is still on disk and in the cache: the next search
        // promotes the new family from it without fetching any bytes, and returns the same results.
        String warmNode = startCluster();
        WarmShard warmShard = prepareEligibleIndex(INDEX_NAME, warmNode);
        String fdt = eligibleFdt(warmShard);
        searchUntilPromoted(INDEX_NAME, warmShard, fdt);
        List<String> fingerprint = queryFingerprint(INDEX_NAME);
        long downloadedBefore = warmShard.manager.stats().downloadedBytes();
        Path wholeFilePath = warmShard.directory.getFilePath(fdt);

        reopenIndex(INDEX_NAME);
        assertTrue("the whole file survives the close", Files.exists(wholeFilePath));
        WarmShard reopened = warmShard(INDEX_NAME, warmNode);
        searchUntilPromoted(INDEX_NAME, reopened, fdt);

        WarmFullFileStats stats = reopened.manager.stats();
        assertEquals(2L, stats.promoted());
        assertEquals(
            "the second promotion is handed the finished download; the bytes are counted once more but not fetched",
            downloadedBefore * 2,
            stats.downloadedBytes()
        );
        assertEquals(fingerprint, queryFingerprint(INDEX_NAME));

        deleteIndex(INDEX_NAME);
    }

    public void testNoPromotionOnOpenRefreshOrForceMerge() throws Exception {
        String warmNode = startCluster();
        setFullFileExtensions(FDT_EXTENSION);
        createWarmIndex(INDEX_NAME);
        WarmShard warmShard = warmShard(INDEX_NAME, warmNode);
        populateAndMergeToBlocks(INDEX_NAME, warmShard);
        String fdt = eligibleFdt(warmShard);
        Path wholeFilePath = warmShard.directory.getFilePath(fdt);

        // Re-open so that the reader handles are opened from blocks, then refresh and force-merge again without searching.
        reopenIndex(INDEX_NAME);
        warmShard = warmShard(INDEX_NAME, warmNode);
        waitForBlockMode(warmShard);
        for (int i = 0; i < 3; i++) {
            refresh(INDEX_NAME);
            assertNoFailures(client().admin().indices().prepareForceMerge(INDEX_NAME).setMaxNumSegments(1).get());
            flushAndRefresh(INDEX_NAME);
            assertTrue(wholeSegmentFiles(localFiles(warmShard)).isEmpty());
            assertFalse(hasWholeFileCacheEntry(warmShard.fileCache, wholeFilePath));
            assertEquals(FullFileState.State.BLOCK, stateOf(warmShard, fdt));
        }

        deleteIndex(INDEX_NAME);
    }

    public void testRealMergeDoesNotPromoteSourceSegments() throws Exception {
        String warmNode = startCluster();
        setFullFileExtensions(FDT_EXTENSION);
        // Automatic refreshes are off so that each flushed batch becomes exactly one segment.
        Settings settings = Settings.builder()
            .put(warmIndexSettings())
            .put(IndexSettings.INDEX_REFRESH_INTERVAL_SETTING.getKey(), -1)
            .build();
        createWarmIndex(INDEX_NAME, settings);
        WarmShard warmShard = warmShard(INDEX_NAME, warmNode);

        // Two segments without a force merge, each with a stored-fields file above one block.
        int half = NUM_DOCS / 2;
        indexDocs(INDEX_NAME, 0, half);
        flushAndRefresh(INDEX_NAME);
        indexDocs(INDEX_NAME, half, NUM_DOCS);
        flushAndRefresh(INDEX_NAME);
        waitForBlockMode(warmShard);
        List<String> sourceFdts = fdts(warmShard.shard);
        assertEquals("expected two segments before the merge: " + sourceFdts, 2, sourceFdts.size());
        for (String fdt : sourceFdts) {
            assertThat(fdt + " must exceed one block", warmShard.directory.fileLength(fdt), greaterThan(ONE_BLOCK_BYTES));
        }

        // Re-open so that the reader handles of both segments are opened from blocks before the merge reads them.
        reopenIndex(INDEX_NAME);
        warmShard = warmShard(INDEX_NAME, warmNode);
        waitForBlockMode(warmShard);
        assertEquals(sourceFdts, fdts(warmShard.shard));
        for (String fdt : sourceFdts) {
            assertEquals(FullFileState.State.BLOCK, stateOf(warmShard, fdt));
        }
        WarmFullFileStats before = warmShard.manager.stats();

        assertNoFailures(client().admin().indices().prepareForceMerge(INDEX_NAME).setMaxNumSegments(1).get());
        flushAndRefresh(INDEX_NAME);
        List<String> mergedFdts = fdts(warmShard.shard);
        assertEquals("expected one segment after the merge: " + mergedFdts, 1, mergedFdts.size());
        assertFalse("merge kept a source segment: " + mergedFdts, sourceFdts.contains(mergedFdts.get(0)));
        waitForBlockMode(warmShard);

        // The merge read both source segments through the open reader but asked for no promotion.
        WarmFullFileStats after = warmShard.manager.stats();
        assertEquals("the merge requested a promotion: " + after, before.requested(), after.requested());
        assertEquals(before.started(), after.started());
        assertEquals(before.promoted(), after.promoted());
        for (String fdt : sourceFdts) {
            assertFalse(
                fdt + " has a whole-file cache entry",
                hasWholeFileCacheEntry(warmShard.fileCache, warmShard.directory.getFilePath(fdt))
            );
        }
        String mergedFdt = eligibleFdt(warmShard);
        assertFalse(hasWholeFileCacheEntry(warmShard.fileCache, warmShard.directory.getFilePath(mergedFdt)));
        assertFalse(isPromoted(warmShard, mergedFdt));
        assertTrue(wholeSegmentFiles(localFiles(warmShard)).isEmpty());

        // A search promotes the merged file, so the index was eligible all along.
        searchUntilPromoted(INDEX_NAME, warmShard, mergedFdt);
        assertThat(warmShard.manager.stats().promoted(), greaterThan(before.promoted()));

        deleteIndex(INDEX_NAME);
    }

    public void testPromotionAfterSearches() throws Exception {
        String warmNode = startCluster();
        createWarmIndex(INDEX_NAME);
        populateAndMergeToBlocks(INDEX_NAME, warmShard(INDEX_NAME, warmNode));

        // Baseline with the feature off (no extensions configured).
        List<String> featureOff = queryFingerprint(INDEX_NAME);

        setFullFileExtensions(FDT_EXTENSION);
        reopenIndex(INDEX_NAME);
        WarmShard warmShard = warmShard(INDEX_NAME, warmNode);
        waitForBlockMode(warmShard);
        String fdt = eligibleFdt(warmShard);
        assertFalse(isPromoted(warmShard, fdt));
        assertEquals(FullFileState.State.BLOCK, stateOf(warmShard, fdt));

        List<String> beforePromotion = queryFingerprint(INDEX_NAME);
        assertEquals(featureOff, beforePromotion);

        searchUntilPromoted(INDEX_NAME, warmShard, fdt);

        List<String> afterPromotion = queryFingerprint(INDEX_NAME);
        assertEquals(beforePromotion, afterPromotion);
        assertEquals(FullFileState.State.FULL, stateOf(warmShard, fdt));

        deleteIndex(INDEX_NAME);
    }

    public void testPromotionOnFirstSearchAfterUploadWithoutReopen() throws Exception {
        // The searcher's handles were opened at refresh time, before the upload, and the index is never closed or
        // reopened: the first searches after the segment reaches block mode must still promote the .fdt.
        String warmNode = startCluster();
        setFullFileExtensions(FDT_EXTENSION);
        createWarmIndex(INDEX_NAME);
        WarmShard warmShard = warmShard(INDEX_NAME, warmNode);
        populateAndMergeToBlocks(INDEX_NAME, warmShard);
        String fdt = eligibleFdt(warmShard);
        assertFalse(isPromoted(warmShard, fdt));

        List<String> beforePromotion = queryFingerprint(INDEX_NAME);
        searchUntilPromoted(INDEX_NAME, warmShard, fdt);

        assertEquals(FullFileState.State.FULL, stateOf(warmShard, fdt));
        assertEquals(beforePromotion, queryFingerprint(INDEX_NAME));
        deleteIndex(INDEX_NAME);
    }

    public void testIneligibleFilesStayOnBlocks() throws Exception {
        String warmNode = startCluster();
        WarmShard warmShard = prepareEligibleIndex(INDEX_NAME, warmNode);
        String fdt = eligibleFdt(warmShard);
        Set<String> otherFiles = segmentFiles(warmShard.shard).stream()
            .filter(f -> f.endsWith(FDT_SUFFIX) == false)
            .collect(Collectors.toSet());
        assertFalse(otherFiles.isEmpty());

        searchUntilPromoted(INDEX_NAME, warmShard, fdt);
        for (int i = 0; i < 5; i++) {
            queryFingerprint(INDEX_NAME);
        }

        List<String> wholeLocalFiles = wholeSegmentFiles(localFiles(warmShard));
        assertEquals("only the stored-fields file may be a whole local file: " + wholeLocalFiles, List.of(fdt), wholeLocalFiles);
        for (String other : otherFiles) {
            Path otherWholeFilePath = warmShard.directory.getFilePath(other);
            assertFalse(other + " has a whole-file cache entry", hasWholeFileCacheEntry(warmShard.fileCache, otherWholeFilePath));
            FullFileState.State state = stateOf(warmShard, other);
            assertTrue(other + " is in state " + state, state == null || state == FullFileState.State.BLOCK);
        }

        deleteIndex(INDEX_NAME);
    }

    public void testClearingExtensionsStopsNewPromotions() throws Exception {
        String warmNode = startCluster();
        WarmShard first = prepareEligibleIndex(INDEX_NAME, warmNode);
        String firstFdt = eligibleFdt(first);
        searchUntilPromoted(INDEX_NAME, first, firstFdt);
        List<String> firstFingerprint = queryFingerprint(INDEX_NAME);

        clearFullFileExtensions();

        createWarmIndex(SECOND_INDEX_NAME);
        populateAndMergeToBlocks(SECOND_INDEX_NAME, warmShard(SECOND_INDEX_NAME, warmNode));
        reopenIndex(SECOND_INDEX_NAME);
        WarmShard second = warmShard(SECOND_INDEX_NAME, warmNode);
        waitForBlockMode(second);
        String secondFdt = eligibleFdt(second);
        for (int i = 0; i < 20; i++) {
            searchWithSource(SECOND_INDEX_NAME);
            assertFalse("second index promoted with the feature off", isPromoted(second, secondFdt));
            assertEquals(FullFileState.State.BLOCK, stateOf(second, secondFdt));
        }
        assertTrue(wholeSegmentFiles(localFiles(second)).isEmpty());

        // The already promoted family was demoted by the settings change: it serves blocks again with the same
        // results, and its mapping, whole file and budget are released once the reader's handles close (on reopen).
        assertEquals(FullFileState.State.BLOCK, stateOf(first, firstFdt));
        assertEquals(1L, first.manager.stats().demoted());
        assertEquals(firstFingerprint, queryFingerprint(INDEX_NAME));

        // Listing the extension again while the reader is still open restores the family from the mapping it kept:
        // FULL at once, no download, same results. Then clear it once more before the reopen below.
        long startedBefore = first.manager.stats().started();
        setFullFileExtensions(FDT_EXTENSION);
        assertBusy(() -> assertEquals(FullFileState.State.FULL, stateOf(first, firstFdt)), WAIT_TIMEOUT.seconds(), TimeUnit.SECONDS);
        assertEquals(1L, first.manager.stats().restored());
        assertEquals("no download for the restore", startedBefore, first.manager.stats().started());
        assertEquals(firstFingerprint, queryFingerprint(INDEX_NAME));
        clearFullFileExtensions();
        assertBusy(() -> assertEquals(FullFileState.State.BLOCK, stateOf(first, firstFdt)), WAIT_TIMEOUT.seconds(), TimeUnit.SECONDS);
        assertEquals(2L, first.manager.stats().demoted());
        reopenIndex(INDEX_NAME);
        WarmShard reopened = warmShard(INDEX_NAME, warmNode);
        assertBusy(() -> {
            assertTrue("whole file released after the reopen: " + localFiles(reopened), wholeSegmentFiles(localFiles(reopened)).isEmpty());
            assertEquals("budget released", 0L, reopened.manager.stats().reservedBytes());
        }, DOWNLOAD_TIMEOUT.seconds(), TimeUnit.SECONDS);
        assertEquals(firstFingerprint, queryFingerprint(INDEX_NAME));

        deleteIndex(INDEX_NAME);
        deleteIndex(SECOND_INDEX_NAME);
    }

    public void testWritesAndMergeAfterPromotionCloseTheOldFamilyAndPromoteTheMergedSegment() throws Exception {
        String warmNode = startCluster();
        WarmShard warmShard = prepareEligibleIndex(INDEX_NAME, warmNode);
        String promotedFdt = eligibleFdt(warmShard);
        searchUntilPromoted(INDEX_NAME, warmShard, promotedFdt);
        FullFileState promotedFamily = requireFamilyState(warmShard, promotedFdt);
        Path promotedWholeFile = warmShard.directory.getFilePath(promotedFdt);
        long promotedBefore = warmShard.manager.stats().promoted();
        assertThat(warmShard.manager.stats().reservedBytes(), greaterThan(0L));

        // Writes and a flush while the segment is served whole: a second segment appears and the promoted one keeps serving.
        indexDocs(INDEX_NAME, NUM_DOCS, 2 * NUM_DOCS);
        flushAndRefresh(INDEX_NAME);
        assertHitCount(client().prepareSearch(INDEX_NAME).setQuery(QueryBuilders.matchAllQuery()).setSize(0).get(), 2 * NUM_DOCS);
        assertEquals(FullFileState.State.FULL, promotedFamily.state());
        assertThat(fdts(warmShard.shard).size(), greaterThan(1));

        // A force merge removes the promoted segment. Its family closes and gives the budget back at once; the mapping,
        // the cache reference and the whole file go once the old reader's handles close.
        flushAndRefresh(INDEX_NAME);
        assertNoFailures(client().admin().indices().prepareForceMerge(INDEX_NAME).setMaxNumSegments(1).get());
        flushAndRefresh(INDEX_NAME);
        List<String> mergedFdts = fdts(warmShard.shard);
        assertEquals("expected one segment after the merge: " + mergedFdts, 1, mergedFdts.size());
        String mergedFdt = mergedFdts.get(0);
        assertNotEquals(promotedFdt, mergedFdt);
        assertBusy(() -> {
            FullFileState mergedFamily = familyState(warmShard, mergedFdt);
            assertNotNull("no family for the merged segment yet", mergedFamily);
            assertTrue("merged segment not uploaded yet", mergedFamily.isRemoteMode());
        }, BLOCK_MODE_TIMEOUT.seconds(), TimeUnit.SECONDS);
        assertBusy(() -> {
            assertEquals(FullFileState.State.CLOSED, promotedFamily.state());
            assertTrue("mapping of the merged-away segment not released", promotedFamily.cleanupDone());
            assertEquals("budget of the merged-away segment not released", 0L, warmShard.manager.stats().reservedBytes());
            assertFalse("whole file of the merged-away segment still local", localFiles(warmShard).contains(promotedFdt));
            assertFalse(
                "whole-file cache entry of the merged-away segment left behind",
                hasWholeFileCacheEntry(warmShard.fileCache, promotedWholeFile)
            );
        }, DOWNLOAD_TIMEOUT.seconds(), TimeUnit.SECONDS);

        // The merged segment is on blocks and promotes on the next searches, with every document.
        assertAdmissibleLength(mergedFdt, warmShard.directory.fileLength(mergedFdt), perSegmentBudget(warmShard));
        assertFalse(isPromoted(warmShard, mergedFdt));
        assertBusy(() -> {
            assertHitCount(client().prepareSearch(INDEX_NAME).setQuery(QueryBuilders.matchAllQuery()).setSize(10).get(), 2 * NUM_DOCS);
            assertTrue("merged segment not promoted yet, family state " + stateOf(warmShard, mergedFdt), isPromoted(warmShard, mergedFdt));
        }, DOWNLOAD_TIMEOUT.seconds(), TimeUnit.SECONDS);
        assertEquals(promotedBefore + 1, warmShard.manager.stats().promoted());
        assertEquals(FullFileState.State.FULL, stateOf(warmShard, mergedFdt));

        deleteIndex(INDEX_NAME);
    }

    public void testDeleteIndexWhileDownloadHeld() throws Exception {
        String warmNode = startCluster();
        WarmShard warmShard = prepareEligibleIndex(INDEX_NAME, warmNode);
        String fdt = eligibleFdt(warmShard);
        Path wholeFilePath = warmShard.directory.getFilePath(fdt);
        Path shardDirectory = wholeFilePath.getParent();

        CountDownLatch entered = new CountDownLatch(1);
        CountDownLatch release = new CountDownLatch(1);
        warmShard.manager.setBeforeDownloadHook(holdDownload(entered, release));
        long usageBeforeDownload;
        try {
            searchWithSource(INDEX_NAME);
            assertTrue("download never reached the hook", entered.await(WAIT_TIMEOUT.seconds(), TimeUnit.SECONDS));
            usageBeforeDownload = warmShard.fileCache.usage();
            deleteIndex(INDEX_NAME);
        } finally {
            release.countDown();
            warmShard.manager.setBeforeDownloadHook(null);
        }

        FileCache fileCache = warmShard.fileCache;
        assertBusy(() -> {
            assertTrue(Files.exists(shardDirectory) == false || Files.exists(wholeFilePath) == false);
            assertFalse("whole-file cache entry left behind", hasWholeFileCacheEntry(fileCache, wholeFilePath));
            assertThat(fileCache.usage(), lessThanOrEqualTo(usageBeforeDownload));
        }, DOWNLOAD_TIMEOUT.seconds(), TimeUnit.SECONDS);

        ClusterHealthResponse health = client().admin().cluster().prepareHealth().get();
        assertFalse(health.isTimedOut());
        assertTrue(isNodeInCluster(warmNode));
    }

    public void testDeleteUnderSearchLoadDoesNotCrashNode() throws Exception {
        String warmNode = startCluster();
        WarmShard warmShard = prepareEligibleIndex(INDEX_NAME, warmNode);
        String fdt = eligibleFdt(warmShard);
        searchUntilPromoted(INDEX_NAME, warmShard, fdt);

        int threadCount = 4;
        AtomicBoolean stop = new AtomicBoolean(false);
        AtomicInteger completedSearches = new AtomicInteger();
        ConcurrentLinkedQueue<Throwable> unexpected = new ConcurrentLinkedQueue<>();
        List<Thread> searchers = new ArrayList<>();
        for (int t = 0; t < threadCount; t++) {
            Thread thread = new Thread(() -> {
                while (stop.get() == false) {
                    try {
                        client().prepareSearch(INDEX_NAME).setQuery(QueryBuilders.matchAllQuery()).setSize(20).get();
                        completedSearches.incrementAndGet();
                    } catch (Exception e) {
                        if (isIndexGoneException(e) == false) {
                            logger.info("unexpected search failure while the index was being deleted", e);
                            unexpected.add(new AssertionError(describeFailure(e), e));
                            return;
                        }
                        if (stop.get()) {
                            return;
                        }
                    }
                }
            }, "warm-full-file-searcher-" + t);
            searchers.add(thread);
            thread.start();
        }
        try {
            assertBusy(
                () -> assertThat(completedSearches.get(), greaterThanOrEqualTo(threadCount * 3)),
                WAIT_TIMEOUT.seconds(),
                TimeUnit.SECONDS
            );
            deleteIndex(INDEX_NAME);
        } finally {
            stop.set(true);
            for (Thread thread : searchers) {
                thread.join(WAIT_TIMEOUT.millis());
                assertFalse("search thread did not finish: " + thread.getName(), thread.isAlive());
            }
        }
        assertTrue("unexpected exceptions during delete under load: " + unexpected, unexpected.isEmpty());

        ClusterHealthResponse health = client().admin().cluster().prepareHealth().get();
        assertFalse(health.isTimedOut());
        assertTrue("warm node left the cluster", isNodeInCluster(warmNode));

        // The node still hosts new warm indices.
        createWarmIndex(SECOND_INDEX_NAME);
        for (int i = 0; i < 10; i++) {
            client().prepareIndex(SECOND_INDEX_NAME)
                .setId(docId(i))
                .setSource(BODY_FIELD, randomAlphaOfLength(32), "tag", "t1", "seq", i)
                .get();
        }
        refresh(SECOND_INDEX_NAME);
        assertHitCount(client().prepareSearch(SECOND_INDEX_NAME).setQuery(QueryBuilders.matchAllQuery()).get(), 10);
        deleteIndex(SECOND_INDEX_NAME);
    }

    public void testClusterStateKeepsApplyingWhileDownloadRuns() throws Exception {
        String warmNode = startCluster();
        WarmShard warmShard = prepareEligibleIndex(INDEX_NAME, warmNode);
        String fdt = eligibleFdt(warmShard);

        CountDownLatch entered = new CountDownLatch(1);
        CountDownLatch release = new CountDownLatch(1);
        warmShard.manager.setBeforeDownloadHook(holdDownload(entered, release));
        try {
            searchWithSource(INDEX_NAME);
            assertTrue("download never reached the hook", entered.await(WAIT_TIMEOUT.seconds(), TimeUnit.SECONDS));

            // Cluster state updates must complete within this limit while the download is held.
            TimeValue limit = TimeValue.timeValueSeconds(30);
            assertAcked(
                client().admin()
                    .cluster()
                    .prepareUpdateSettings()
                    .setPersistentSettings(
                        Settings.builder().put(ShardLimitValidator.SETTING_CLUSTER_MAX_SHARDS_PER_NODE.getKey(), RAISED_MAX_SHARDS_PER_NODE)
                    )
                    .get(limit)
            );
            assertAcked(client().admin().indices().prepareCreate(SECOND_INDEX_NAME).setSettings(warmIndexSettings()).get(limit));
            ensureGreen(limit, SECOND_INDEX_NAME);
        } finally {
            release.countDown();
            warmShard.manager.setBeforeDownloadHook(null);
        }

        // Let the held download finish before tearing down.
        searchUntilPromoted(INDEX_NAME, warmShard, fdt);

        assertAcked(
            client().admin()
                .cluster()
                .prepareUpdateSettings()
                .setPersistentSettings(Settings.builder().putNull(ShardLimitValidator.SETTING_CLUSTER_MAX_SHARDS_PER_NODE.getKey()))
                .get()
        );
        deleteIndex(SECOND_INDEX_NAME);
        deleteIndex(INDEX_NAME);
    }

    public void testRestartReusesTheWholeFileLeftOnDiskWithoutADownload() throws Exception {
        String warmNode = startCluster();
        WarmShard warmShard = prepareEligibleIndex(INDEX_NAME, warmNode);
        String fdt = eligibleFdt(warmShard);
        searchUntilPromoted(INDEX_NAME, warmShard, fdt);
        List<String> beforeRestart = queryFingerprint(INDEX_NAME);
        Path wholeFilePath = warmShard.directory.getFilePath(fdt);
        long wholeFileLength = Files.size(wholeFilePath);

        // Close the index so its last commit is synced to the remote store, restart the node and open the index again.
        // The whole file stays in the shard directory across the restart, untouched.
        assertShardsAcknowledged(client().admin().indices().prepareClose(INDEX_NAME).get());
        AtomicBoolean leftoverIntact = new AtomicBoolean(false);
        internalCluster().restartNode(warmNode, new InternalTestCluster.RestartCallback() {
            @Override
            public Settings onNodeStopped(String nodeName) throws Exception {
                leftoverIntact.set(Files.exists(wholeFilePath) && Files.size(wholeFilePath) == wholeFileLength);
                return super.onNodeStopped(nodeName);
            }
        });
        assertTrue("the whole file should have survived the shutdown intact", leftoverIntact.get());
        assertShardsAcknowledged(client().admin().indices().prepareOpen(INDEX_NAME).get());
        ensureGreen(ENSURE_GREEN_TIMEOUT, INDEX_NAME);

        // The restarted node has a fresh manager: the first searches promote by validating and reusing the leftover.
        WarmShard restarted = warmShard(INDEX_NAME, warmNode);
        searchUntilPromoted(INDEX_NAME, restarted, fdt);
        WarmFullFileStats stats = restarted.manager.stats();
        assertEquals("the leftover was not reused: " + stats, 1L, stats.reused());
        assertEquals("the leftover was downloaded again: " + stats, 0L, stats.downloadedBytes());
        assertEquals(FullFileState.State.FULL, stateOf(restarted, fdt));
        assertEquals(beforeRestart, queryFingerprint(INDEX_NAME));

        deleteIndex(INDEX_NAME);
    }

    public void testRelocationUnderSearchLoadClosesTheSourceFamilyAndPromotesOnTheTarget() throws Exception {
        String sourceNode = startCluster();
        WarmShard source = prepareEligibleIndex(INDEX_NAME, sourceNode);
        String fdt = eligibleFdt(source);
        searchUntilPromoted(INDEX_NAME, source, fdt);
        List<String> beforeMove = queryFingerprint(INDEX_NAME);
        FullFileState sourceFamily = requireFamilyState(source, fdt);
        String targetNode = internalCluster().startDataAndWarmNodes(1).get(0);

        // Searches keep running while the shard moves. A search may see a shard-unavailable failure while the copy it
        // was routed to hands over; anything else is a real failure.
        int threadCount = 4;
        AtomicBoolean stop = new AtomicBoolean(false);
        AtomicInteger completedSearches = new AtomicInteger();
        ConcurrentLinkedQueue<Throwable> unexpected = new ConcurrentLinkedQueue<>();
        List<Thread> searchers = new ArrayList<>();
        for (int t = 0; t < threadCount; t++) {
            Thread thread = new Thread(() -> {
                while (stop.get() == false) {
                    try {
                        client().prepareSearch(INDEX_NAME).setQuery(QueryBuilders.matchAllQuery()).setSize(20).get();
                        completedSearches.incrementAndGet();
                    } catch (Exception e) {
                        if (isIndexGoneException(e) == false) {
                            logger.info("unexpected search failure while the shard was relocating", e);
                            unexpected.add(new AssertionError(describeFailure(e), e));
                            return;
                        }
                    }
                }
            }, "warm-full-file-relocation-searcher-" + t);
            searchers.add(thread);
            thread.start();
        }
        try {
            assertBusy(
                () -> assertThat(completedSearches.get(), greaterThanOrEqualTo(threadCount * 3)),
                WAIT_TIMEOUT.seconds(),
                TimeUnit.SECONDS
            );
            assertAcked(
                client().admin().cluster().prepareReroute().add(new MoveAllocationCommand(INDEX_NAME, 0, sourceNode, targetNode)).get()
            );
            ClusterHealthResponse health = client().admin()
                .cluster()
                .prepareHealth(INDEX_NAME)
                .setWaitForEvents(Priority.LANGUID)
                .setWaitForNoRelocatingShards(true)
                .setWaitForGreenStatus()
                .setTimeout(ENSURE_GREEN_TIMEOUT)
                .get();
            assertFalse("relocation did not finish", health.isTimedOut());
            int searchesAfterMove = completedSearches.get();
            assertBusy(
                () -> assertThat(completedSearches.get(), greaterThanOrEqualTo(searchesAfterMove + threadCount)),
                WAIT_TIMEOUT.seconds(),
                TimeUnit.SECONDS
            );
        } finally {
            stop.set(true);
            for (Thread thread : searchers) {
                thread.join(WAIT_TIMEOUT.millis());
                assertFalse("search thread did not finish: " + thread.getName(), thread.isAlive());
            }
        }
        assertTrue("unexpected exceptions during the relocation: " + unexpected, unexpected.isEmpty());
        assertTrue("source node left the cluster", isNodeInCluster(sourceNode));
        assertTrue("target node left the cluster", isNodeInCluster(targetNode));

        // The source shard closed under the running searches: its family is closed, the budget is back and the mapping
        // is released once the last handle closed. Removing the shard's files from the source is IndicesStore's job.
        assertBusy(() -> {
            assertEquals(FullFileState.State.CLOSED, sourceFamily.state());
            assertTrue("source mapping not released", sourceFamily.cleanupDone());
            assertEquals("source budget not released", 0L, source.manager.stats().reservedBytes());
        }, DOWNLOAD_TIMEOUT.seconds(), TimeUnit.SECONDS);

        // The target promotes on its own, within its own budget, with the same results.
        WarmShard target = warmShard(INDEX_NAME, targetNode);
        searchUntilPromoted(INDEX_NAME, target, fdt);
        assertEquals(FullFileState.State.FULL, stateOf(target, fdt));
        assertThat(target.manager.stats().promoted(), greaterThanOrEqualTo(1L));
        assertEquals(beforeMove, queryFingerprint(INDEX_NAME));

        deleteIndex(INDEX_NAME);
    }

    public void testRestartRedownloadsAndNeverServesLeftover() throws Exception {
        String warmNode = startCluster();
        WarmShard warmShard = prepareEligibleIndex(INDEX_NAME, warmNode);
        String fdt = eligibleFdt(warmShard);
        searchUntilPromoted(INDEX_NAME, warmShard, fdt);
        List<String> beforeRestart = queryFingerprint(INDEX_NAME);
        Path wholeFilePath = warmShard.directory.getFilePath(fdt);

        // Close the index before the restart so its last commit is flushed and synced to the remote store, and open it
        // again afterwards. The whole file stays in the shard directory across the restart. While the node is down,
        // overwrite that leftover with zeros: if it were ever served, searches would no longer return the original
        // documents.
        CloseIndexResponse close = client().admin().indices().prepareClose(INDEX_NAME).get();
        assertShardsAcknowledged(close);
        AtomicBoolean leftoverSeen = new AtomicBoolean(false);
        internalCluster().restartNode(warmNode, new InternalTestCluster.RestartCallback() {
            @Override
            public Settings onNodeStopped(String nodeName) throws Exception {
                if (Files.exists(wholeFilePath)) {
                    leftoverSeen.set(true);
                    Files.write(wholeFilePath, new byte[(int) Files.size(wholeFilePath)]);
                }
                return super.onNodeStopped(nodeName);
            }
        });
        assertTrue("the whole file should have survived the shutdown", leftoverSeen.get());
        OpenIndexResponse open = client().admin().indices().prepareOpen(INDEX_NAME).get();
        assertShardsAcknowledged(open);
        ensureGreen(ENSURE_GREEN_TIMEOUT, INDEX_NAME);

        WarmShard restarted = warmShard(INDEX_NAME, warmNode);
        AtomicInteger downloads = new AtomicInteger();
        restarted.manager.setBeforeDownloadHook(downloads::incrementAndGet);
        try {
            searchUntilPromoted(INDEX_NAME, restarted, fdt);
        } finally {
            restarted.manager.setBeforeDownloadHook(null);
        }
        assertThat("no whole-file download after the restart", downloads.get(), greaterThanOrEqualTo(1));
        assertEquals(beforeRestart, queryFingerprint(INDEX_NAME));

        deleteIndex(INDEX_NAME);
    }

    private boolean isNodeInCluster(String nodeName) {
        for (DiscoveryNode node : client().admin().cluster().prepareState().get().getState().nodes()) {
            if (node.getName().equals(nodeName)) {
                return true;
            }
        }
        return false;
    }

    /**
     * True for the exceptions a search may see when its index is deleted underneath it: the index or shard is gone or
     * closed, no copy is available, or the search context was released. A search phase failure qualifies when every
     * shard failure (or, without shard failures, its own cause chain) does.
     */
    private static boolean isIndexGoneException(Throwable throwable) {
        SearchPhaseExecutionException phaseFailure = findInChain(throwable, SearchPhaseExecutionException.class);
        if (phaseFailure != null) {
            ShardSearchFailure[] failures = phaseFailure.shardFailures();
            if (failures.length == 0) {
                // The search action does not record shard-not-available failures (index or shard gone, shard closed),
                // so "all shards failed" with no shard failures and no cause means every shard failed that way.
                return phaseFailure.getCause() == null || chainHasIndexGoneCause(phaseFailure.getCause());
            }
            for (ShardSearchFailure failure : failures) {
                if (chainHasIndexGoneCause(failure.getCause()) == false) {
                    return false;
                }
            }
            return true;
        }
        return chainHasIndexGoneCause(throwable);
    }

    private static <T extends Throwable> T findInChain(Throwable throwable, Class<T> type) {
        Set<Throwable> seen = new HashSet<>();
        for (Throwable t = throwable; t != null && seen.add(t); t = t.getCause()) {
            if (type.isInstance(t)) {
                return type.cast(t);
            }
        }
        return null;
    }

    private static boolean chainHasIndexGoneCause(Throwable throwable) {
        Set<Throwable> seen = new HashSet<>();
        for (Throwable t = throwable; t != null && seen.add(t); t = t.getCause()) {
            if (isIndexGoneCause(t)) {
                return true;
            }
        }
        return false;
    }

    private static boolean isIndexGoneCause(Throwable cause) {
        if (cause instanceof IndexNotFoundException
            || cause instanceof IndexClosedException
            || cause instanceof NoShardAvailableActionException
            || cause instanceof ShardNotFoundException
            || cause instanceof IllegalIndexShardStateException
            || cause instanceof SearchContextMissingException
            || cause instanceof AlreadyClosedException) {
            return true;
        }
        // Exceptions without a transport serialization arrive wrapped, with the original class in the message.
        String message = cause.getMessage();
        return message != null
            && (message.contains("already_closed_exception")
                || message.contains("index_not_found_exception")
                || message.contains("no_shard_available_action_exception"));
    }

    private static String describeFailure(Throwable throwable) {
        StringBuilder sb = new StringBuilder(throwable.toString());
        SearchPhaseExecutionException phaseFailure = findInChain(throwable, SearchPhaseExecutionException.class);
        if (phaseFailure != null) {
            for (ShardSearchFailure failure : phaseFailure.shardFailures()) {
                sb.append(" | shard failure: ").append(failure.reason());
                for (Throwable t = failure.getCause(); t != null; t = t.getCause()) {
                    sb.append(" <- ").append(t.getClass().getName()).append(": ").append(t.getMessage());
                }
            }
        }
        for (Throwable t = throwable.getCause(); t != null; t = t.getCause()) {
            sb.append(" <- ").append(t.getClass().getName()).append(": ").append(t.getMessage());
        }
        return sb.toString();
    }
}

/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.storage;

import com.carrotsearch.randomizedtesting.annotations.ThreadLeakFilters;

import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.store.AlreadyClosedException;
import org.opensearch.action.NoShardAvailableActionException;
import org.opensearch.action.admin.cluster.health.ClusterHealthResponse;
import org.opensearch.action.admin.indices.close.CloseIndexResponse;
import org.opensearch.action.admin.indices.open.OpenIndexResponse;
import org.opensearch.action.bulk.BulkRequest;
import org.opensearch.action.bulk.BulkResponse;
import org.opensearch.action.search.SearchPhaseExecutionException;
import org.opensearch.action.search.SearchResponse;
import org.opensearch.action.search.ShardSearchFailure;
import org.opensearch.cluster.metadata.IndexMetadata;
import org.opensearch.cluster.node.DiscoveryNode;
import org.opensearch.common.lucene.Lucene;
import org.opensearch.common.settings.Settings;
import org.opensearch.common.unit.TimeValue;
import org.opensearch.common.util.FeatureFlags;
import org.opensearch.core.common.unit.ByteSizeUnit;
import org.opensearch.core.common.unit.ByteSizeValue;
import org.opensearch.index.IndexModule;
import org.opensearch.index.IndexNotFoundException;
import org.opensearch.index.TieredMergePolicyProvider;
import org.opensearch.index.engine.Engine;
import org.opensearch.index.engine.EngineConfig;
import org.opensearch.index.query.QueryBuilders;
import org.opensearch.index.shard.IllegalIndexShardStateException;
import org.opensearch.index.shard.IndexShard;
import org.opensearch.index.shard.ShardNotFoundException;
import org.opensearch.index.store.remote.file.CleanerDaemonThreadLeakFilter;
import org.opensearch.index.store.remote.filecache.CachedIndexInput;
import org.opensearch.index.store.remote.filecache.FileCache;
import org.opensearch.index.store.remote.utils.FileTypeUtils;
import org.opensearch.indices.IndexClosedException;
import org.opensearch.indices.IndicesService;
import org.opensearch.indices.ShardLimitValidator;
import org.opensearch.node.Node;
import org.opensearch.remotestore.RemoteStoreBaseIntegTestCase;
import org.opensearch.search.SearchContextMissingException;
import org.opensearch.search.SearchHit;
import org.opensearch.search.sort.SortOrder;
import org.opensearch.storage.directory.TieredDirectory;
import org.opensearch.storage.utils.DirectoryUtils;
import org.opensearch.storage.warmfullfile.FullFileState;
import org.opensearch.storage.warmfullfile.WarmFullFileManager;
import org.opensearch.storage.warmfullfile.WarmFullFileSettings;
import org.opensearch.test.InternalTestCluster;
import org.opensearch.test.OpenSearchIntegTestCase;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Arrays;
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
import static org.hamcrest.Matchers.lessThan;
import static org.hamcrest.Matchers.lessThanOrEqualTo;

/**
 * Integration tests for warm-tier whole-file promotion of {@code .fdt} (stored fields) files served by mmap.
 *
 * @opensearch.experimental
 */
@ThreadLeakFilters(filters = CleanerDaemonThreadLeakFilter.class)
@OpenSearchIntegTestCase.ClusterScope(scope = OpenSearchIntegTestCase.Scope.TEST, numDataNodes = 0, supportsDedicatedMasters = false)
public class WarmFullFileIT extends RemoteStoreBaseIntegTestCase {

    private static final String INDEX_NAME = "warm-full-file-idx";
    private static final String SECOND_INDEX_NAME = "warm-full-file-idx-2";
    private static final String FDT_EXTENSION = "fdt";
    private static final String FDT_SUFFIX = "." + FDT_EXTENSION;

    // 1100 documents with a 16 KiB random alphanumeric body give a stored-fields file of roughly 18 MB after a force
    // merge to one segment: above one 8 MiB block and below the per-cache-segment whole-file budget for up to 64
    // segments (4 GB cache / 64 segments / 2 = 32 MiB).
    private static final int NUM_DOCS = 1100;
    private static final int BODY_LENGTH = 16 * 1024;
    private static final int BULK_SIZE = 220;
    private static final long ONE_BLOCK_BYTES = 8L * 1024 * 1024;

    private static final TimeValue BLOCK_MODE_TIMEOUT = TimeValue.timeValueSeconds(90);
    private static final TimeValue DOWNLOAD_TIMEOUT = TimeValue.timeValueSeconds(120);

    private static final String MAPPING = "{\"properties\":{"
        + "\"body\":{\"type\":\"keyword\",\"index\":false,\"doc_values\":false},"
        + "\"tag\":{\"type\":\"keyword\"},"
        + "\"seq\":{\"type\":\"integer\"}}}";

    @Override
    protected boolean addMockIndexStorePlugin() {
        return false;
    }

    @Override
    protected boolean ignoreExternalCluster() {
        return true;
    }

    @Override
    protected Settings featureFlagSettings() {
        return Settings.builder().put(FeatureFlags.WRITABLE_WARM_INDEX_EXPERIMENTAL_FLAG, true).build();
    }

    @Override
    protected Settings nodeSettings(int nodeOrdinal) {
        ByteSizeValue cacheSize = new ByteSizeValue(4, ByteSizeUnit.GB);
        return Settings.builder()
            .put(super.nodeSettings(nodeOrdinal))
            .put(Node.NODE_SEARCH_CACHE_SIZE_SETTING.getKey(), cacheSize.toString())
            .build();
    }

    /** A warm shard and the node-level objects the tests inspect on the node that hosts it. */
    private static final class WarmShard {
        final String nodeName;
        final IndexShard shard;
        final TieredDirectory directory;
        final FileCache fileCache;
        final WarmFullFileManager manager;

        WarmShard(String nodeName, IndexShard shard, TieredDirectory directory, FileCache fileCache, WarmFullFileManager manager) {
            this.nodeName = nodeName;
            this.shard = shard;
            this.directory = directory;
            this.fileCache = fileCache;
            this.manager = manager;
        }
    }

    /** Settings for a 1p0r tiered-storage warm index: no compound segments, and the .fdt served by mmap. */
    private static Settings warmIndexSettings() {
        List<String> nioExtensions = IndexModule.INDEX_STORE_HYBRID_NIO_EXTENSIONS.getDefault(Settings.EMPTY)
            .stream()
            .filter(extension -> extension.equals(FDT_EXTENSION) == false)
            .collect(Collectors.toList());
        return Settings.builder()
            .put(IndexMetadata.SETTING_NUMBER_OF_SHARDS, 1)
            .put(IndexMetadata.SETTING_NUMBER_OF_REPLICAS, 0)
            .put(IndexModule.IS_WARM_INDEX_SETTING.getKey(), true)
            .put(IndexModule.INDEX_COMPOSITE_STORE_TYPE_SETTING.getKey(), "tiered-storage")
            .put(EngineConfig.INDEX_USE_COMPOUND_FILE.getKey(), false)
            .put(TieredMergePolicyProvider.INDEX_COMPOUND_FORMAT_SETTING.getKey(), false)
            .putList(IndexModule.INDEX_STORE_HYBRID_NIO_EXTENSIONS.getKey(), nioExtensions)
            .build();
    }

    private void createWarmIndex(String index) {
        assertAcked(client().admin().indices().prepareCreate(index).setSettings(warmIndexSettings()).setMapping(MAPPING).get());
        ensureGreen(index);
    }

    private void setFullFileExtensions(String... extensions) {
        assertAcked(
            client().admin()
                .cluster()
                .prepareUpdateSettings()
                .setPersistentSettings(Settings.builder().putList(WarmFullFileSettings.EXTENSIONS.getKey(), extensions))
                .get()
        );
    }

    private void clearFullFileExtensions() {
        assertAcked(
            client().admin()
                .cluster()
                .prepareUpdateSettings()
                .setPersistentSettings(Settings.builder().putNull(WarmFullFileSettings.EXTENSIONS.getKey()))
                .get()
        );
    }

    /** Indexes {@link #NUM_DOCS} large documents, force-merges to one segment and waits for block mode. */
    private void populateAndMergeToBlocks(String index, WarmShard warmShard) throws Exception {
        for (int from = 0; from < NUM_DOCS; from += BULK_SIZE) {
            BulkRequest bulk = new BulkRequest();
            for (int i = from; i < Math.min(NUM_DOCS, from + BULK_SIZE); i++) {
                bulk.add(
                    client().prepareIndex(index)
                        .setId("doc-" + i)
                        .setSource("body", randomAlphaOfLength(BODY_LENGTH), "tag", "t" + (i % 10), "seq", i)
                        .request()
                );
            }
            BulkResponse response = client().bulk(bulk).actionGet();
            assertFalse(response.buildFailureMessage(), response.hasFailures());
        }
        flushAndRefresh(index);
        assertNoFailures(client().admin().indices().prepareForceMerge(index).setMaxNumSegments(1).get());
        flushAndRefresh(index);
        waitForBlockMode(warmShard);
    }

    /** Waits until block files exist locally and no plain (whole) segment file remains in the shard directory. */
    private static void waitForBlockMode(WarmShard warmShard) throws Exception {
        assertBusy(() -> {
            List<String> localFiles = Arrays.asList(warmShard.directory.listLocalFiles());
            assertTrue("no block file yet in " + localFiles, localFiles.stream().anyMatch(FileTypeUtils::isBlockFile));
            assertTrue("whole segment files still local: " + localFiles, wholeSegmentFiles(localFiles).isEmpty());
        }, BLOCK_MODE_TIMEOUT.seconds(), TimeUnit.SECONDS);
    }

    /** The plain segment file names in a local listing: no block files, lock files, segments_N or temporary files. */
    private static List<String> wholeSegmentFiles(List<String> localFiles) {
        return localFiles.stream()
            .filter(file -> FileTypeUtils.isBlockFile(file) == false)
            .filter(file -> FileTypeUtils.isLockFile(file) == false)
            .filter(file -> FileTypeUtils.isSegmentsFile(file) == false)
            .filter(file -> FileTypeUtils.isTempFile(file) == false)
            .collect(Collectors.toList());
    }

    private WarmShard warmShard(String index, String nodeName) {
        IndexShard shard = internalCluster().getInstance(IndicesService.class, nodeName).indexService(resolveIndex(index)).getShard(0);
        TieredDirectory directory = unwrapToTieredDirectory(shard.store().directory());
        FileCache fileCache = internalCluster().getInstance(Node.class, nodeName).fileCache();
        WarmFullFileManager manager = internalCluster().getInstance(WarmFullFileManager.class, nodeName);
        assertNotNull("warm node has no file cache", fileCache);
        assertNotNull("warm node has no whole-file manager", manager);
        return new WarmShard(nodeName, shard, directory, fileCache, manager);
    }

    /** The files of every segment the shard's current reader is built from. */
    private static Set<String> segmentFiles(IndexShard shard) throws IOException {
        try (Engine.Searcher searcher = shard.acquireSearcher("warm-full-file-it")) {
            Set<String> files = new HashSet<>();
            for (LeafReaderContext leaf : searcher.getDirectoryReader().leaves()) {
                files.addAll(Lucene.segmentReader(leaf.reader()).getSegmentInfo().files());
            }
            return files;
        }
    }

    /** The single .fdt of the force-merged segment, checked to be eligible by size and to fit the whole-file budget. */
    private static String eligibleFdt(WarmShard warmShard) throws Exception {
        List<String> fdts = segmentFiles(warmShard.shard).stream().filter(f -> f.endsWith(FDT_SUFFIX)).collect(Collectors.toList());
        assertEquals("expected exactly one stored-fields file after the force merge: " + fdts, 1, fdts.size());
        String fdt = fdts.get(0);
        long length = warmShard.directory.fileLength(fdt);
        assertThat("stored-fields file must exceed one block", length, greaterThan(ONE_BLOCK_BYTES));
        long perSegmentCapacity = (warmShard.fileCache.capacity() + warmShard.manager.segments() - 1) / warmShard.manager.segments();
        long budget = perSegmentCapacity / 2;
        assertThat("stored-fields file must fit the per-segment whole-file budget", length, lessThan(budget));
        return fdt;
    }

    /**
     * True once the whole file is in the shard directory under its plain name with its full size, the cache entry at the
     * whole-file key reports the full length, and the family is FULL.
     */
    private static boolean isPromoted(WarmShard warmShard, String fileName) throws IOException {
        if (Arrays.asList(warmShard.directory.listLocalFiles()).contains(fileName) == false) {
            return false;
        }
        long length = warmShard.directory.fileLength(fileName);
        Path key = warmShard.directory.getFilePath(fileName);
        if (Files.exists(key) == false || Files.size(key) != length) {
            return false;
        }
        CachedIndexInput entry = warmShard.fileCache.get(key);
        if (entry == null) {
            return false;
        }
        try {
            if (entry.length() != length) {
                return false;
            }
        } finally {
            warmShard.fileCache.decRef(key);
        }
        return familyState(warmShard, fileName) == FullFileState.State.FULL;
    }

    /** True if the cache holds any entry at the whole-file key of the given file. */
    private static boolean hasWholeFileCacheEntry(FileCache fileCache, Path wholeFilePath) {
        CachedIndexInput entry = fileCache.get(wholeFilePath);
        if (entry == null) {
            return false;
        }
        fileCache.decRef(wholeFilePath);
        return true;
    }

    /** The family's promotion state, or null if the file has no switchable entry. */
    private static FullFileState.State familyState(WarmShard warmShard, String fileName) {
        Path key = warmShard.directory.getFilePath(fileName + DirectoryUtils.SWITCHABLE_PREFIX);
        CachedIndexInput entry = warmShard.fileCache.get(key);
        if (entry == null) {
            return null;
        }
        try {
            FullFileState state = entry.fullFileState();
            return state == null ? null : state.state();
        } finally {
            warmShard.fileCache.decRef(key);
        }
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
            assertTrue("whole file not promoted yet, family state " + familyState(warmShard, fdt), isPromoted(warmShard, fdt));
        }, DOWNLOAD_TIMEOUT.seconds(), TimeUnit.SECONDS);
    }

    /** Closes and re-opens the index so that the reader opens its file handles from the remote (block) copy. */
    private void reopenIndex(String index) {
        CloseIndexResponse close = client().admin().indices().prepareClose(index).get();
        assertTrue(close.isShardsAcknowledged());
        OpenIndexResponse open = client().admin().indices().prepareOpen(index).get();
        assertTrue(open.isShardsAcknowledged());
        ensureGreen(TimeValue.timeValueSeconds(90), index);
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

    /** Starts a cluster-manager node and one data+warm node; returns the warm node's name. */
    private String startCluster() {
        InternalTestCluster cluster = internalCluster();
        cluster.startClusterManagerOnlyNode();
        return cluster.startDataAndWarmNodes(1).get(0);
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

    private void deleteIndex(String index) {
        assertAcked(client().admin().indices().prepareDelete(index).get());
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
            assertTrue(wholeSegmentFiles(Arrays.asList(warmShard.directory.listLocalFiles())).isEmpty());
            assertFalse(hasWholeFileCacheEntry(warmShard.fileCache, wholeFilePath));
            assertEquals(FullFileState.State.BLOCK, familyState(warmShard, fdt));
        }

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
        assertEquals(FullFileState.State.BLOCK, familyState(warmShard, fdt));

        List<String> beforePromotion = queryFingerprint(INDEX_NAME);
        assertEquals(featureOff, beforePromotion);

        searchUntilPromoted(INDEX_NAME, warmShard, fdt);

        List<String> afterPromotion = queryFingerprint(INDEX_NAME);
        assertEquals(beforePromotion, afterPromotion);
        assertEquals(FullFileState.State.FULL, familyState(warmShard, fdt));

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

        assertEquals(FullFileState.State.FULL, familyState(warmShard, fdt));
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

        List<String> wholeLocalFiles = wholeSegmentFiles(Arrays.asList(warmShard.directory.listLocalFiles()));
        assertEquals("only the stored-fields file may be a whole local file: " + wholeLocalFiles, List.of(fdt), wholeLocalFiles);
        for (String other : otherFiles) {
            Path otherWholeFilePath = warmShard.directory.getFilePath(other);
            assertFalse(other + " has a whole-file cache entry", hasWholeFileCacheEntry(warmShard.fileCache, otherWholeFilePath));
            FullFileState.State state = familyState(warmShard, other);
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
            assertEquals(FullFileState.State.BLOCK, familyState(second, secondFdt));
        }
        assertTrue(wholeSegmentFiles(Arrays.asList(second.directory.listLocalFiles())).isEmpty());

        // The already promoted family keeps serving from its whole file.
        assertTrue(isPromoted(first, firstFdt));
        assertEquals(FullFileState.State.FULL, familyState(first, firstFdt));
        assertEquals(firstFingerprint, queryFingerprint(INDEX_NAME));

        deleteIndex(INDEX_NAME);
        deleteIndex(SECOND_INDEX_NAME);
    }

    public void testDeleteIndexWhileDownloadHeld() throws Exception {
        String warmNode = startCluster();
        WarmShard warmShard = prepareEligibleIndex(INDEX_NAME, warmNode);
        String fdt = eligibleFdt(warmShard);
        Path wholeFilePath = warmShard.directory.getFilePath(fdt);
        Path shardDirectory = wholeFilePath.getParent();

        CountDownLatch entered = new CountDownLatch(1);
        CountDownLatch release = new CountDownLatch(1);
        warmShard.manager.setBeforeDownloadHook(() -> {
            entered.countDown();
            try {
                release.await(2, TimeUnit.MINUTES);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
        });
        long usageBeforeDownload;
        try {
            searchWithSource(INDEX_NAME);
            assertTrue("download never reached the hook", entered.await(60, TimeUnit.SECONDS));
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
            assertBusy(() -> assertThat(completedSearches.get(), greaterThanOrEqualTo(threadCount * 3)), 60, TimeUnit.SECONDS);
            deleteIndex(INDEX_NAME);
        } finally {
            stop.set(true);
            for (Thread thread : searchers) {
                thread.join(TimeUnit.SECONDS.toMillis(60));
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
                .setId("doc-" + i)
                .setSource("body", randomAlphaOfLength(32), "tag", "t1", "seq", i)
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
        warmShard.manager.setBeforeDownloadHook(() -> {
            entered.countDown();
            try {
                release.await(2, TimeUnit.MINUTES);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
        });
        try {
            searchWithSource(INDEX_NAME);
            assertTrue("download never reached the hook", entered.await(60, TimeUnit.SECONDS));

            TimeValue limit = TimeValue.timeValueSeconds(30);
            assertAcked(
                client().admin()
                    .cluster()
                    .prepareUpdateSettings()
                    .setPersistentSettings(Settings.builder().put(ShardLimitValidator.SETTING_CLUSTER_MAX_SHARDS_PER_NODE.getKey(), 1500))
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
        assertTrue(close.isShardsAcknowledged());
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
        assertTrue(open.isShardsAcknowledged());
        ensureGreen(TimeValue.timeValueSeconds(90), INDEX_NAME);

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

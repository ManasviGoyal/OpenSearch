/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.storage;

import com.carrotsearch.randomizedtesting.annotations.ThreadLeakFilters;

import org.apache.logging.log4j.Level;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.core.LogEvent;
import org.opensearch.action.admin.indices.delete.DeleteIndexRequest;
import org.opensearch.action.bulk.BulkRequest;
import org.opensearch.action.bulk.BulkResponse;
import org.opensearch.action.search.SearchResponse;
import org.opensearch.cluster.metadata.IndexMetadata;
import org.opensearch.common.regex.Regex;
import org.opensearch.common.settings.Settings;
import org.opensearch.common.util.FeatureFlags;
import org.opensearch.core.common.unit.ByteSizeUnit;
import org.opensearch.core.common.unit.ByteSizeValue;
import org.opensearch.index.IndexModule;
import org.opensearch.index.TieredMergePolicyProvider;
import org.opensearch.index.engine.EngineConfig;
import org.opensearch.index.query.QueryBuilders;
import org.opensearch.index.shard.IndexShard;
import org.opensearch.index.store.remote.file.CleanerDaemonThreadLeakFilter;
import org.opensearch.index.store.remote.filecache.CachedIndexInput;
import org.opensearch.index.store.remote.filecache.FileCache;
import org.opensearch.index.store.remote.utils.FileTypeUtils;
import org.opensearch.indices.IndicesService;
import org.opensearch.node.Node;
import org.opensearch.remotestore.RemoteStoreBaseIntegTestCase;
import org.opensearch.storage.directory.TieredDirectory;
import org.opensearch.storage.utils.DirectoryUtils;
import org.opensearch.storage.warmfullfile.FullFileState;
import org.opensearch.storage.warmfullfile.WarmFullFileManager;
import org.opensearch.storage.warmfullfile.WarmFullFileSettings;
import org.opensearch.test.InternalTestCluster;
import org.opensearch.test.MockLogAppender;
import org.opensearch.test.OpenSearchIntegTestCase;

import java.io.IOException;
import java.nio.file.Path;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;

import static org.opensearch.test.hamcrest.OpenSearchAssertions.assertAcked;
import static org.opensearch.test.hamcrest.OpenSearchAssertions.assertHitCount;

/**
 * Integration tests for warm-tier whole-file promotion of a file that the shard's local hybridfs directory serves
 * with NIO. The {@code .fdt} extension is on the default {@code index.store.hybrid.nio.extensions} list, so once the
 * whole file is downloaded the master input is not Lucene's mmap input: the promotion is declined for the lifetime
 * of the file family, the downloaded copy is dropped, and the node logs one WARN per extension.
 *
 * @opensearch.experimental
 */
@ThreadLeakFilters(filters = CleanerDaemonThreadLeakFilter.class)
@OpenSearchIntegTestCase.ClusterScope(scope = OpenSearchIntegTestCase.Scope.TEST, numDataNodes = 0, supportsDedicatedMasters = false)
public class WarmFullFileNioIT extends RemoteStoreBaseIntegTestCase {

    private static final String NIO_INDEX = "warm-nio-idx";
    private static final String NIO_INDEX_2 = "warm-nio-idx-2";
    private static final String MMAP_INDEX = "warm-mmap-idx";
    private static final String FDT = "fdt";
    private static final String BODY_FIELD = "body";
    private static final String KNOWN_DOC_ID = "doc-0";
    private static final int NUM_DOCS = 1500;
    private static final int BODY_LENGTH = 16 * 1024;
    private static final int BULK_SIZE = 250;
    private static final long ONE_BLOCK_BYTES = 8L * 1024 * 1024;
    private static final String MANAGER_LOGGER = WarmFullFileManager.class.getName();
    // Matches the one-per-extension WARN logged when a downloaded whole file is served by NIO instead of mmap.
    private static final String FDT_DECLINED_WARN = "*whole-file promotion of [.fdt] files is permanently declined on this node:*not mmap*";

    /** The one-per-extension WARN for .fdt naming the given whole-file path as the first declined file. */
    private static String fdtDeclinedWarnFor(Path firstFile) {
        return "*whole-file promotion of [.fdt] files is permanently declined on this node:*not mmap (first file [" + firstFile + "])*";
    }

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
        Settings.Builder featureSettings = Settings.builder();
        featureSettings.put(FeatureFlags.WRITABLE_WARM_INDEX_EXPERIMENTAL_FLAG, true);
        return featureSettings.build();
    }

    @Override
    protected Settings nodeSettings(int nodeOrdinal) {
        // Large enough that half of one cache segment (the per-segment whole-file budget) admits a ~32 MiB .fdt.
        ByteSizeValue cacheSize = new ByteSizeValue(4, ByteSizeUnit.GB);
        return Settings.builder()
            .put(super.nodeSettings(nodeOrdinal))
            .put(Node.NODE_SEARCH_CACHE_SIZE_SETTING.getKey(), cacheSize.toString())
            .build();
    }

    /**
     * One NIO-served .fdt: the first search downloads the whole file once, the download is declined because the
     * master is an NIO input, the copy is removed, and the family is never retried while it lives.
     */
    public void testNioServedFileIsDownloadedOnceDeclinedAndNeverRetried() throws Exception {
        String warmNode = startCluster();
        WarmFullFileManager manager = internalCluster().getInstance(WarmFullFileManager.class, warmNode);
        FileCache fileCache = internalCluster().getInstance(Node.class, warmNode).fileCache();

        assertAcked(client().admin().indices().prepareCreate(NIO_INDEX).setSettings(warmIndexSettings().build()).get());
        ensureGreen(NIO_INDEX);
        String knownBody = indexDocs(NIO_INDEX);
        mergeToBlocks(NIO_INDEX);
        enableFdtPromotion();
        reopenAndVerify(NIO_INDEX);

        TieredDirectory directory = shardDirectory(NIO_INDEX);
        String fdt = singleFdt(directory);
        Path switchableKey = directory.getFilePath(fdt + DirectoryUtils.SWITCHABLE_PREFIX);
        Path wholeFileKey = directory.getFilePath(fdt);
        assertAdmissible(manager, fileCache, fileCache.get(switchableKey), switchableKey);

        AtomicInteger downloads = new AtomicInteger();
        manager.setBeforeDownloadHook(downloads::incrementAndGet);
        try (MockLogAppender appender = MockLogAppender.createForLoggers(LogManager.getLogger(WarmFullFileManager.class))) {
            appender.addExpectation(
                new MockLogAppender.SeenEventExpectation("one WARN for fdt", MANAGER_LOGGER, Level.WARN, fdtDeclinedWarnFor(wholeFileKey))
            );
            CountingEventExpectation declinedWarns = new CountingEventExpectation(MANAGER_LOGGER, Level.WARN, FDT_DECLINED_WARN, 1);
            appender.addExpectation(declinedWarns);

            // The first search clones the stored-fields input and starts the download.
            assertBusy(() -> {
                assertSearchCorrect(NIO_INDEX, knownBody);
                assertEquals(1, downloads.get());
            }, 60, TimeUnit.SECONDS);
            // The download completes and the family is declined for good.
            assertBusy(() -> {
                FullFileState state = familyState(fileCache, switchableKey);
                assertEquals(Long.MAX_VALUE, state.retryAt());
                assertEquals(FullFileState.State.BLOCK, state.state());
            }, 60, TimeUnit.SECONDS);

            // Later searches keep working from blocks and never start another download.
            for (int i = 0; i < 100; i++) {
                assertSearchCorrect(NIO_INDEX, knownBody);
            }
            assertEquals("exactly one whole-file download", 1, downloads.get());

            // The downloaded copy is gone: no plain .fdt in the shard directory and no whole-file cache entry.
            assertBusy(() -> assertNoWholeFile(directory, fileCache, fdt), 30, TimeUnit.SECONDS);
            assertTrue("blocks are served locally", Arrays.stream(directory.listLocalFiles()).anyMatch(f -> f.startsWith(fdt + "_block_")));

            // retryAt == Long.MAX_VALUE: the family stays BLOCK and the trigger can never fire again while it lives.
            FullFileState state = familyState(fileCache, switchableKey);
            assertEquals(FullFileState.State.BLOCK, state.state());
            assertEquals(Long.MAX_VALUE, state.retryAt());
            appender.assertAllExpectationsMatched();
        } finally {
            manager.setBeforeDownloadHook(null);
        }

        assertAcked(client().admin().indices().delete(new DeleteIndexRequest(NIO_INDEX)).get());
    }

    /**
     * Two warm indices, each with an NIO-served .fdt: each file is downloaded and declined, but only the first
     * decline of the extension is a WARN; the second file's decline is not.
     */
    public void testOnlyOneWarnPerExtensionAcrossSegments() throws Exception {
        String warmNode = startCluster();
        WarmFullFileManager manager = internalCluster().getInstance(WarmFullFileManager.class, warmNode);
        FileCache fileCache = internalCluster().getInstance(Node.class, warmNode).fileCache();

        assertAcked(client().admin().indices().prepareCreate(NIO_INDEX).setSettings(warmIndexSettings().build()).get());
        assertAcked(client().admin().indices().prepareCreate(NIO_INDEX_2).setSettings(warmIndexSettings().build()).get());
        ensureGreen(NIO_INDEX, NIO_INDEX_2);
        String knownBody1 = indexDocs(NIO_INDEX);
        String knownBody2 = indexDocs(NIO_INDEX_2);
        mergeToBlocks(NIO_INDEX);
        mergeToBlocks(NIO_INDEX_2);
        enableFdtPromotion();
        reopenAndVerify(NIO_INDEX);
        reopenAndVerify(NIO_INDEX_2);

        TieredDirectory directory1 = shardDirectory(NIO_INDEX);
        TieredDirectory directory2 = shardDirectory(NIO_INDEX_2);
        String fdt1 = singleFdt(directory1);
        String fdt2 = singleFdt(directory2);
        Path switchableKey1 = directory1.getFilePath(fdt1 + DirectoryUtils.SWITCHABLE_PREFIX);
        Path switchableKey2 = directory2.getFilePath(fdt2 + DirectoryUtils.SWITCHABLE_PREFIX);
        assertAdmissible(manager, fileCache, fileCache.get(switchableKey1), switchableKey1);
        assertAdmissible(manager, fileCache, fileCache.get(switchableKey2), switchableKey2);

        AtomicInteger downloads = new AtomicInteger();
        manager.setBeforeDownloadHook(downloads::incrementAndGet);
        try (MockLogAppender appender = MockLogAppender.createForLoggers(LogManager.getLogger(WarmFullFileManager.class))) {
            appender.addExpectation(
                new MockLogAppender.SeenEventExpectation(
                    "one WARN for the first fdt",
                    MANAGER_LOGGER,
                    Level.WARN,
                    "*first file [" + directory1.getFilePath(fdt1) + "]*"
                )
            );
            appender.addExpectation(
                new MockLogAppender.UnseenEventExpectation(
                    "no WARN for the second fdt",
                    MANAGER_LOGGER,
                    Level.WARN,
                    "*first file [" + directory2.getFilePath(fdt2) + "]*"
                )
            );
            appender.addExpectation(new CountingEventExpectation(MANAGER_LOGGER, Level.WARN, FDT_DECLINED_WARN, 1));

            // First index: download, decline, WARN.
            assertBusy(() -> {
                assertSearchCorrect(NIO_INDEX, knownBody1);
                assertEquals(1, downloads.get());
            }, 60, TimeUnit.SECONDS);
            assertBusy(() -> assertEquals(Long.MAX_VALUE, familyState(fileCache, switchableKey1).retryAt()), 60, TimeUnit.SECONDS);

            // Second index: download, decline, no WARN.
            assertBusy(() -> {
                assertSearchCorrect(NIO_INDEX_2, knownBody2);
                assertEquals(2, downloads.get());
            }, 60, TimeUnit.SECONDS);
            assertBusy(() -> assertEquals(Long.MAX_VALUE, familyState(fileCache, switchableKey2).retryAt()), 60, TimeUnit.SECONDS);

            for (int i = 0; i < 20; i++) {
                assertSearchCorrect(NIO_INDEX, knownBody1);
                assertSearchCorrect(NIO_INDEX_2, knownBody2);
            }
            assertEquals("one download per file", 2, downloads.get());
            assertEquals(FullFileState.State.BLOCK, familyState(fileCache, switchableKey1).state());
            assertEquals(FullFileState.State.BLOCK, familyState(fileCache, switchableKey2).state());
            assertBusy(() -> {
                assertNoWholeFile(directory1, fileCache, fdt1);
                assertNoWholeFile(directory2, fileCache, fdt2);
            }, 30, TimeUnit.SECONDS);
            appender.assertAllExpectationsMatched();
        } finally {
            manager.setBeforeDownloadHook(null);
        }

        assertAcked(client().admin().indices().delete(new DeleteIndexRequest(NIO_INDEX, NIO_INDEX_2)).get());
    }

    /**
     * The decline is per file family, not per node: on the same node an index whose hybridfs directory serves
     * .fdt with mmap promotes its .fdt to a whole local file while the NIO-served index stays declined.
     */
    public void testMmapServedFileOnSameNodeStillPromotes() throws Exception {
        String warmNode = startCluster();
        WarmFullFileManager manager = internalCluster().getInstance(WarmFullFileManager.class, warmNode);
        FileCache fileCache = internalCluster().getInstance(Node.class, warmNode).fileCache();

        List<String> nioExtensionsWithoutFdt = IndexModule.INDEX_STORE_HYBRID_NIO_EXTENSIONS.getDefault(Settings.EMPTY)
            .stream()
            .filter(extension -> extension.equals(FDT) == false)
            .collect(Collectors.toList());
        assertAcked(client().admin().indices().prepareCreate(NIO_INDEX).setSettings(warmIndexSettings().build()).get());
        assertAcked(
            client().admin()
                .indices()
                .prepareCreate(MMAP_INDEX)
                .setSettings(
                    warmIndexSettings().putList(IndexModule.INDEX_STORE_HYBRID_NIO_EXTENSIONS.getKey(), nioExtensionsWithoutFdt).build()
                )
                .get()
        );
        ensureGreen(NIO_INDEX, MMAP_INDEX);
        String nioBody = indexDocs(NIO_INDEX);
        String mmapBody = indexDocs(MMAP_INDEX);
        mergeToBlocks(NIO_INDEX);
        mergeToBlocks(MMAP_INDEX);
        enableFdtPromotion();
        reopenAndVerify(NIO_INDEX);
        reopenAndVerify(MMAP_INDEX);

        TieredDirectory nioDirectory = shardDirectory(NIO_INDEX);
        TieredDirectory mmapDirectory = shardDirectory(MMAP_INDEX);
        String nioFdt = singleFdt(nioDirectory);
        String mmapFdt = singleFdt(mmapDirectory);
        Path nioSwitchableKey = nioDirectory.getFilePath(nioFdt + DirectoryUtils.SWITCHABLE_PREFIX);
        Path mmapSwitchableKey = mmapDirectory.getFilePath(mmapFdt + DirectoryUtils.SWITCHABLE_PREFIX);
        long nioLength = assertAdmissible(manager, fileCache, fileCache.get(nioSwitchableKey), nioSwitchableKey);
        long mmapLength = assertAdmissible(manager, fileCache, fileCache.get(mmapSwitchableKey), mmapSwitchableKey);

        AtomicInteger downloads = new AtomicInteger();
        manager.setBeforeDownloadHook(downloads::incrementAndGet);
        try (MockLogAppender appender = MockLogAppender.createForLoggers(LogManager.getLogger(WarmFullFileManager.class))) {
            appender.addExpectation(
                new MockLogAppender.SeenEventExpectation(
                    "one WARN for the NIO-served fdt",
                    MANAGER_LOGGER,
                    Level.WARN,
                    "*first file [" + nioDirectory.getFilePath(nioFdt) + "]*"
                )
            );
            appender.addExpectation(
                new MockLogAppender.UnseenEventExpectation(
                    "no WARN for the mmap-served fdt",
                    MANAGER_LOGGER,
                    Level.WARN,
                    "*first file [" + mmapDirectory.getFilePath(mmapFdt) + "]*"
                )
            );

            // NIO-served index: downloaded once, declined for good.
            assertBusy(() -> {
                assertSearchCorrect(NIO_INDEX, nioBody);
                assertEquals(1, downloads.get());
            }, 60, TimeUnit.SECONDS);
            assertBusy(() -> assertEquals(Long.MAX_VALUE, familyState(fileCache, nioSwitchableKey).retryAt()), 60, TimeUnit.SECONDS);
            assertEquals(FullFileState.State.BLOCK, familyState(fileCache, nioSwitchableKey).state());

            // mmap-served index on the same node: downloaded once and promoted to FULL.
            assertBusy(() -> {
                assertSearchCorrect(MMAP_INDEX, mmapBody);
                assertEquals(2, downloads.get());
            }, 60, TimeUnit.SECONDS);
            assertBusy(
                () -> assertEquals(FullFileState.State.FULL, familyState(fileCache, mmapSwitchableKey).state()),
                60,
                TimeUnit.SECONDS
            );

            // The whole file is in the shard directory under its plain name and in the cache at full length.
            assertTrue(Arrays.asList(mmapDirectory.listLocalFiles()).contains(mmapFdt));
            Path mmapWholeFileKey = mmapDirectory.getFilePath(mmapFdt);
            CachedIndexInput wholeFile = fileCache.get(mmapWholeFileKey);
            assertNotNull("whole-file cache entry", wholeFile);
            try {
                assertEquals(mmapLength, wholeFile.length());
            } finally {
                fileCache.decRef(mmapWholeFileKey);
            }

            // Searches on both indices stay correct; the declined family stays declined and the FULL family stays FULL.
            for (int i = 0; i < 20; i++) {
                assertSearchCorrect(NIO_INDEX, nioBody);
                assertSearchCorrect(MMAP_INDEX, mmapBody);
            }
            assertEquals("one download per file", 2, downloads.get());
            FullFileState nioState = familyState(fileCache, nioSwitchableKey);
            assertEquals(FullFileState.State.BLOCK, nioState.state());
            assertEquals(Long.MAX_VALUE, nioState.retryAt());
            assertEquals(nioLength, nioState.fileLength());
            assertEquals(FullFileState.State.FULL, familyState(fileCache, mmapSwitchableKey).state());
            assertBusy(() -> assertNoWholeFile(nioDirectory, fileCache, nioFdt), 30, TimeUnit.SECONDS);
            appender.assertAllExpectationsMatched();
        } finally {
            manager.setBeforeDownloadHook(null);
        }

        assertAcked(client().admin().indices().delete(new DeleteIndexRequest(NIO_INDEX, MMAP_INDEX)).get());
    }

    /** Starts a cluster-manager node and one data+warm node, and enables whole-file promotion for .fdt files. */
    private String startCluster() {
        InternalTestCluster internalTestCluster = internalCluster();
        internalTestCluster.startClusterManagerOnlyNode();
        return internalTestCluster.startDataAndWarmNodes(1).get(0);
    }

    /**
     * Enables whole-file promotion for .fdt. Called only once every index under test is in block mode and right before
     * the reopen, so that no handle is promotable before the test has installed its download hook and log appender.
     */
    private void enableFdtPromotion() {
        assertAcked(
            client().admin()
                .cluster()
                .prepareUpdateSettings()
                .setPersistentSettings(Settings.builder().putList(WarmFullFileSettings.EXTENSIONS.getKey(), FDT))
                .get()
        );
    }

    /** A 1-shard, 0-replica tiered-storage warm index that never writes compound (.cfs) segments. */
    private static Settings.Builder warmIndexSettings() {
        return Settings.builder()
            .put(IndexMetadata.SETTING_NUMBER_OF_SHARDS, 1)
            .put(IndexMetadata.SETTING_NUMBER_OF_REPLICAS, 0)
            .put(IndexModule.IS_WARM_INDEX_SETTING.getKey(), true)
            .put(IndexModule.INDEX_COMPOSITE_STORE_TYPE_SETTING.getKey(), "tiered-storage")
            .put(EngineConfig.INDEX_USE_COMPOUND_FILE.getKey(), false)
            .put(TieredMergePolicyProvider.INDEX_COMPOUND_FORMAT_SETTING.getKey(), false);
    }

    /**
     * Indexes {@link #NUM_DOCS} documents with a {@link #BODY_LENGTH}-character body each.
     * @return the body of the document with id {@link #KNOWN_DOC_ID}
     */
    private String indexDocs(String index) {
        String knownBody = null;
        for (int from = 0; from < NUM_DOCS; from += BULK_SIZE) {
            BulkRequest bulkRequest = new BulkRequest();
            for (int i = from; i < Math.min(from + BULK_SIZE, NUM_DOCS); i++) {
                String id = "doc-" + i;
                String body = randomAlphaOfLength(BODY_LENGTH);
                if (id.equals(KNOWN_DOC_ID)) {
                    knownBody = body;
                }
                bulkRequest.add(client().prepareIndex(index).setId(id).setSource(BODY_FIELD, body).request());
            }
            BulkResponse response = client().bulk(bulkRequest).actionGet();
            assertFalse(response.buildFailureMessage(), response.hasFailures());
        }
        assertNotNull(knownBody);
        return knownBody;
    }

    /**
     * Force-merges the index to one segment and waits until every Lucene data file of the shard has been uploaded and
     * is served from blocks.
     */
    private void mergeToBlocks(String index) throws Exception {
        flushAndRefresh(index);
        client().admin().indices().prepareForceMerge(index).setMaxNumSegments(1).get();
        flushAndRefresh(index);
        TieredDirectory directory = shardDirectory(index);
        assertBusy(() -> {
            List<String> localDataFiles = Arrays.stream(directory.listLocalFiles())
                .filter(file -> file.contains(FileTypeUtils.BLOCK_FILE_IDENTIFIER) == false)
                .filter(file -> FileTypeUtils.isLockFile(file) == false)
                .filter(file -> FileTypeUtils.isSegmentsFile(file) == false)
                .collect(Collectors.toList());
            assertTrue("local data files still present: " + localDataFiles, localDataFiles.isEmpty());
        }, 60, TimeUnit.SECONDS);
    }

    /** Closes and reopens the index so the searcher's file handles are opened on remote-mode files. */
    private void reopenAndVerify(String index) {
        assertAcked(client().admin().indices().prepareClose(index).get());
        assertAcked(client().admin().indices().prepareOpen(index).get());
        ensureGreen(index);
        assertHitCount(client().prepareSearch(index).setSize(0).setQuery(QueryBuilders.matchAllQuery()).get(), NUM_DOCS);
    }

    private TieredDirectory shardDirectory(String index) {
        IndexShard shard = internalCluster().getDataNodeInstance(IndicesService.class).indexService(resolveIndex(index)).getShardOrNull(0);
        assertNotNull(shard);
        return unwrapToTieredDirectory(shard.store().directory());
    }

    /** @return the name of the shard's only .fdt file */
    private static String singleFdt(TieredDirectory directory) throws IOException {
        List<String> fdts = Arrays.stream(directory.listAll()).filter(file -> file.endsWith("." + FDT)).collect(Collectors.toList());
        assertEquals("one segment, one .fdt: " + Arrays.toString(directory.listAll()), 1, fdts.size());
        return fdts.get(0);
    }

    /**
     * Checks the file is larger than one block and fits the per-segment whole-file budget, so a promotion attempt is
     * admitted and reaches the download.
     * @return the file length
     */
    private static long assertAdmissible(WarmFullFileManager manager, FileCache fileCache, CachedIndexInput entry, Path switchableKey) {
        assertNotNull("switchable entry at " + switchableKey, entry);
        try {
            long length = entry.fullFileState().fileLength();
            long perSegmentCapacity = (fileCache.capacity() + manager.segments() - 1) / manager.segments();
            long budget = perSegmentCapacity / 2;
            assertTrue(".fdt of " + length + " bytes must exceed one block", length > ONE_BLOCK_BYTES);
            assertTrue(".fdt of " + length + " bytes must fit the per-segment budget of " + budget, length < budget);
            return length;
        } finally {
            fileCache.decRef(switchableKey);
        }
    }

    private static FullFileState familyState(FileCache fileCache, Path switchableKey) {
        CachedIndexInput entry = fileCache.get(switchableKey);
        assertNotNull("switchable entry at " + switchableKey, entry);
        try {
            return entry.fullFileState();
        } finally {
            fileCache.decRef(switchableKey);
        }
    }

    /** Asserts the shard directory holds no plain copy of the file and the cache has no whole-file entry for it. */
    private static void assertNoWholeFile(TieredDirectory directory, FileCache fileCache, String name) throws IOException {
        assertFalse("whole file present: " + name, Arrays.asList(directory.listLocalFiles()).contains(name));
        Path wholeFileKey = directory.getFilePath(name);
        CachedIndexInput entry = fileCache.get(wholeFileKey);
        if (entry != null) {
            fileCache.decRef(wholeFileKey);
        }
        assertNull("whole-file cache entry present: " + wholeFileKey, entry);
    }

    /** Searches with _source: the total hit count and the known document's body must match what was indexed. */
    private void assertSearchCorrect(String index, String knownBody) {
        SearchResponse all = client().prepareSearch(index).setQuery(QueryBuilders.matchAllQuery()).setSize(10).get();
        assertHitCount(all, NUM_DOCS);
        assertEquals(10, all.getHits().getHits().length);
        for (int i = 0; i < all.getHits().getHits().length; i++) {
            assertEquals(BODY_LENGTH, ((String) all.getHits().getAt(i).getSourceAsMap().get(BODY_FIELD)).length());
        }
        SearchResponse known = client().prepareSearch(index).setQuery(QueryBuilders.idsQuery().addIds(KNOWN_DOC_ID)).get();
        assertHitCount(known, 1);
        assertEquals(knownBody, known.getHits().getAt(0).getSourceAsMap().get(BODY_FIELD));
    }

    /** Counts the events of one logger and level whose formatted message matches a wildcard pattern. */
    private static final class CountingEventExpectation implements MockLogAppender.LoggingExpectation {
        private final String loggerName;
        private final Level level;
        private final String pattern;
        private final int expected;
        private final AtomicInteger seen = new AtomicInteger();

        CountingEventExpectation(String loggerName, Level level, String pattern, int expected) {
            this.loggerName = loggerName;
            this.level = level;
            this.pattern = pattern;
            this.expected = expected;
        }

        @Override
        public void match(LogEvent event) {
            if (event.getLevel().equals(level)
                && event.getLoggerName().equals(loggerName)
                && Regex.simpleMatch(pattern, event.getMessage().getFormattedMessage())) {
                seen.incrementAndGet();
            }
        }

        @Override
        public void assertMatched() {
            assertEquals("events matching [" + pattern + "]", expected, seen.get());
        }
    }
}

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
import org.opensearch.action.admin.indices.close.CloseIndexResponse;
import org.opensearch.action.admin.indices.open.OpenIndexResponse;
import org.opensearch.action.bulk.BulkRequest;
import org.opensearch.action.bulk.BulkResponse;
import org.opensearch.action.support.clustermanager.ShardsAcknowledgedResponse;
import org.opensearch.cluster.metadata.IndexMetadata;
import org.opensearch.common.lucene.Lucene;
import org.opensearch.common.settings.Settings;
import org.opensearch.common.unit.TimeValue;
import org.opensearch.common.util.FeatureFlags;
import org.opensearch.core.common.unit.ByteSizeUnit;
import org.opensearch.core.common.unit.ByteSizeValue;
import org.opensearch.index.IndexModule;
import org.opensearch.index.TieredMergePolicyProvider;
import org.opensearch.index.engine.Engine;
import org.opensearch.index.engine.EngineConfig;
import org.opensearch.index.shard.IndexShard;
import org.opensearch.index.store.remote.file.AbstractBlockIndexInput;
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
import org.opensearch.test.OpenSearchIntegTestCase;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

import static org.opensearch.test.hamcrest.OpenSearchAssertions.assertAcked;
import static org.opensearch.test.hamcrest.OpenSearchAssertions.assertNoFailures;
import static org.hamcrest.Matchers.greaterThan;
import static org.hamcrest.Matchers.lessThan;

/**
 * Cluster scaffold and shard-level helpers shared by the warm-tier whole-file promotion integration tests: a
 * cluster-manager node plus one data+warm node with a 4 GB file cache, 1p0r tiered-storage warm indices without
 * compound segments, bulk indexing of large documents, and inspection of the shard directory, the file cache and the
 * whole-file manager of the warm node.
 */
@ThreadLeakFilters(filters = CleanerDaemonThreadLeakFilter.class)
@OpenSearchIntegTestCase.ClusterScope(scope = OpenSearchIntegTestCase.Scope.TEST, numDataNodes = 0, supportsDedicatedMasters = false)
public abstract class WarmFullFileBaseIT extends RemoteStoreBaseIntegTestCase {

    protected static final String FDT_EXTENSION = "fdt";
    protected static final String FDT_SUFFIX = "." + FDT_EXTENSION;
    protected static final String BODY_FIELD = "body";

    // 1100 documents with a 16 KiB random alphanumeric body give a stored-fields file of roughly 18 MB after a force
    // merge to one segment: above one 8 MiB block and below the per-cache-segment whole-file budget for up to 64
    // segments (4 GB cache / 64 segments / 2 = 32 MiB).
    protected static final int NUM_DOCS = 1100;
    protected static final int BODY_LENGTH = 16 * 1024;
    protected static final int BULK_SIZE = 220;
    protected static final long ONE_BLOCK_BYTES = AbstractBlockIndexInput.Builder.DEFAULT_BLOCK_SIZE;
    private static final ByteSizeValue CACHE_SIZE = new ByteSizeValue(4, ByteSizeUnit.GB);

    /** Upper bound for waits on a search, a download hook or a background thread. */
    protected static final TimeValue WAIT_TIMEOUT = TimeValue.timeValueSeconds(60);
    protected static final TimeValue BLOCK_MODE_TIMEOUT = TimeValue.timeValueSeconds(90);
    protected static final TimeValue ENSURE_GREEN_TIMEOUT = TimeValue.timeValueSeconds(90);
    protected static final TimeValue DOWNLOAD_TIMEOUT = TimeValue.timeValueSeconds(120);

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
        return Settings.builder()
            .put(super.nodeSettings(nodeOrdinal))
            .put(Node.NODE_SEARCH_CACHE_SIZE_SETTING.getKey(), CACHE_SIZE.toString())
            .build();
    }

    /** A warm shard and the node-level objects the tests inspect on the node that hosts it. */
    protected static final class WarmShard {
        final IndexShard shard;
        final TieredDirectory directory;
        final FileCache fileCache;
        final WarmFullFileManager manager;

        WarmShard(IndexShard shard, TieredDirectory directory, FileCache fileCache, WarmFullFileManager manager) {
            this.shard = shard;
            this.directory = directory;
            this.fileCache = fileCache;
            this.manager = manager;
        }
    }

    /** Starts a cluster-manager node and one data+warm node; returns the warm node's name. */
    protected String startCluster() {
        InternalTestCluster cluster = internalCluster();
        cluster.startClusterManagerOnlyNode();
        return cluster.startDataAndWarmNodes(1).get(0);
    }

    /** The extensions that the local hybridfs directory of a warm index created by {@link #createWarmIndex} serves with NIO. */
    protected List<String> nioExtensions() {
        return nioExtensionsWithoutFdt();
    }

    /** The default NIO extension list. */
    protected static List<String> defaultNioExtensions() {
        return IndexModule.INDEX_STORE_HYBRID_NIO_EXTENSIONS.getDefault(Settings.EMPTY);
    }

    /** The default NIO extension list without {@code fdt}, so that stored-fields files are served by mmap. */
    protected static List<String> nioExtensionsWithoutFdt() {
        return defaultNioExtensions().stream().filter(extension -> extension.equals(FDT_EXTENSION) == false).collect(Collectors.toList());
    }

    /** Settings for a 1p0r tiered-storage warm index without compound segments, serving {@link #nioExtensions()} with NIO. */
    protected Settings warmIndexSettings() {
        return warmIndexSettings(nioExtensions());
    }

    /** Settings for a 1p0r tiered-storage warm index without compound segments, serving the given extensions with NIO. */
    protected static Settings warmIndexSettings(List<String> nioExtensions) {
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

    protected void createWarmIndex(String index) {
        createWarmIndex(index, warmIndexSettings());
    }

    /** Creates a warm index with the given settings and the mapping for the documents of {@link #indexDocs}, and waits for green. */
    protected void createWarmIndex(String index, Settings settings) {
        assertAcked(client().admin().indices().prepareCreate(index).setSettings(settings).setMapping(MAPPING).get());
        ensureGreen(index);
    }

    protected void setFullFileExtensions(String... extensions) {
        assertAcked(
            client().admin()
                .cluster()
                .prepareUpdateSettings()
                .setPersistentSettings(Settings.builder().putList(WarmFullFileSettings.EXTENSIONS.getKey(), extensions))
                .get()
        );
    }

    protected void clearFullFileExtensions() {
        assertAcked(
            client().admin()
                .cluster()
                .prepareUpdateSettings()
                .setPersistentSettings(Settings.builder().putNull(WarmFullFileSettings.EXTENSIONS.getKey()))
                .get()
        );
    }

    protected static String docId(int seq) {
        return "doc-" + seq;
    }

    /**
     * Indexes the documents with sequence numbers in {@code [from, to)}, each with a {@link #BODY_LENGTH}-character
     * random body, in bulks of {@link #BULK_SIZE}.
     * @return the body of the first document, {@code docId(from)}
     */
    protected String indexDocs(String index, int from, int to) {
        String firstBody = null;
        for (int start = from; start < to; start += BULK_SIZE) {
            BulkRequest bulk = new BulkRequest();
            for (int i = start; i < Math.min(to, start + BULK_SIZE); i++) {
                String body = randomAlphaOfLength(BODY_LENGTH);
                if (i == from) {
                    firstBody = body;
                }
                bulk.add(
                    client().prepareIndex(index).setId(docId(i)).setSource(BODY_FIELD, body, "tag", "t" + (i % 10), "seq", i).request()
                );
            }
            BulkResponse response = client().bulk(bulk).actionGet();
            assertFalse(response.buildFailureMessage(), response.hasFailures());
        }
        assertNotNull("no document indexed", firstBody);
        return firstBody;
    }

    /** Flushes, force-merges the index to one segment, flushes again and waits for block mode. */
    protected void mergeToBlocks(String index, WarmShard warmShard) throws Exception {
        flushAndRefresh(index);
        assertNoFailures(client().admin().indices().prepareForceMerge(index).setMaxNumSegments(1).get());
        flushAndRefresh(index);
        waitForBlockMode(warmShard);
    }

    /** Waits until block files exist locally and no plain (whole) segment file remains in the shard directory. */
    protected static void waitForBlockMode(WarmShard warmShard) throws Exception {
        assertBusy(() -> {
            List<String> localFiles = localFiles(warmShard);
            assertTrue("no block file yet in " + localFiles, localFiles.stream().anyMatch(FileTypeUtils::isBlockFile));
            assertTrue("whole segment files still local: " + localFiles, wholeSegmentFiles(localFiles).isEmpty());
        }, BLOCK_MODE_TIMEOUT.seconds(), TimeUnit.SECONDS);
    }

    /** The names of the files currently in the shard's local directory. */
    protected static List<String> localFiles(WarmShard warmShard) throws IOException {
        return Arrays.asList(warmShard.directory.listLocalFiles());
    }

    /** The plain segment file names in a local listing: no block files, lock files, segments_N or temporary files. */
    protected static List<String> wholeSegmentFiles(List<String> localFiles) {
        return localFiles.stream()
            .filter(file -> FileTypeUtils.isBlockFile(file) == false)
            .filter(file -> FileTypeUtils.isLockFile(file) == false)
            .filter(file -> FileTypeUtils.isSegmentsFile(file) == false)
            .filter(file -> FileTypeUtils.isTempFile(file) == false)
            .collect(Collectors.toList());
    }

    protected WarmShard warmShard(String index, String nodeName) {
        IndexShard shard = internalCluster().getInstance(IndicesService.class, nodeName).indexService(resolveIndex(index)).getShard(0);
        TieredDirectory directory = unwrapToTieredDirectory(shard.store().directory());
        FileCache fileCache = internalCluster().getInstance(Node.class, nodeName).fileCache();
        WarmFullFileManager manager = internalCluster().getInstance(WarmFullFileManager.class, nodeName);
        assertNotNull("warm node has no file cache", fileCache);
        assertNotNull("warm node has no whole-file manager", manager);
        return new WarmShard(shard, directory, fileCache, manager);
    }

    /** The files of every segment the shard's current reader is built from. */
    protected static Set<String> segmentFiles(IndexShard shard) throws IOException {
        try (Engine.Searcher searcher = shard.acquireSearcher("warm-full-file-it")) {
            Set<String> files = new HashSet<>();
            for (LeafReaderContext leaf : searcher.getDirectoryReader().leaves()) {
                files.addAll(Lucene.segmentReader(leaf.reader()).getSegmentInfo().files());
            }
            return files;
        }
    }

    /** The sorted stored-fields file names of the segments the shard's current reader is built from. */
    protected static List<String> fdts(IndexShard shard) throws IOException {
        return segmentFiles(shard).stream().filter(f -> f.endsWith(FDT_SUFFIX)).sorted().collect(Collectors.toList());
    }

    /** The single .fdt of the force-merged segment, checked to be eligible by size and to fit the whole-file budget. */
    protected static String eligibleFdt(WarmShard warmShard) throws Exception {
        List<String> fdts = fdts(warmShard.shard);
        assertEquals("expected exactly one stored-fields file after the force merge: " + fdts, 1, fdts.size());
        String fdt = fdts.get(0);
        assertAdmissibleLength(fdt, warmShard.directory.fileLength(fdt), perSegmentBudget(warmShard));
        return fdt;
    }

    /** The whole-file budget of one cache segment on the warm node: half of the cache capacity per segment. */
    protected static long perSegmentBudget(WarmShard warmShard) {
        return warmShard.manager.stats().perSegmentBudgetBytes();
    }

    /** Asserts a file of the given length exceeds one block and fits the per-segment whole-file budget. */
    protected static void assertAdmissibleLength(String fileName, long length, long budget) {
        assertThat(fileName + " must exceed one block", length, greaterThan(ONE_BLOCK_BYTES));
        assertThat(fileName + " must fit the per-segment whole-file budget of " + budget + " bytes", length, lessThan(budget));
    }

    /**
     * True once the whole file is in the shard directory under its plain name with its full size, the cache entry at the
     * whole-file key reports the full length, and the family is FULL.
     */
    protected static boolean isPromoted(WarmShard warmShard, String fileName) throws IOException {
        if (localFiles(warmShard).contains(fileName) == false) {
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
        return stateOf(warmShard, fileName) == FullFileState.State.FULL;
    }

    /** True if the cache holds any entry at the whole-file key of the given file. */
    protected static boolean hasWholeFileCacheEntry(FileCache fileCache, Path wholeFilePath) {
        CachedIndexInput entry = fileCache.get(wholeFilePath);
        if (entry == null) {
            return false;
        }
        fileCache.decRef(wholeFilePath);
        return true;
    }

    /** The family's promotion state, or null if the file has no switchable entry or that entry carries no state. */
    protected static FullFileState familyState(WarmShard warmShard, String fileName) {
        Path key = warmShard.directory.getFilePath(fileName + DirectoryUtils.SWITCHABLE_PREFIX);
        CachedIndexInput entry = warmShard.fileCache.get(key);
        if (entry == null) {
            return null;
        }
        try {
            return entry.fullFileState();
        } finally {
            warmShard.fileCache.decRef(key);
        }
    }

    /** The family's promotion state; fails if the file has no switchable entry. */
    protected static FullFileState requireFamilyState(WarmShard warmShard, String fileName) {
        FullFileState state = familyState(warmShard, fileName);
        assertNotNull("no switchable entry for " + fileName, state);
        return state;
    }

    /** The family's state name, or null if the file has no switchable entry. */
    protected static FullFileState.State stateOf(WarmShard warmShard, String fileName) {
        FullFileState state = familyState(warmShard, fileName);
        return state == null ? null : state.state();
    }

    protected static void assertShardsAcknowledged(ShardsAcknowledgedResponse response) {
        assertAcked(response);
        assertTrue("not acknowledged by the shards", response.isShardsAcknowledged());
    }

    /** Closes and re-opens the index so that the reader opens its file handles from the remote (block) copy. */
    protected void reopenIndex(String index) {
        CloseIndexResponse close = client().admin().indices().prepareClose(index).get();
        assertShardsAcknowledged(close);
        OpenIndexResponse open = client().admin().indices().prepareOpen(index).get();
        assertShardsAcknowledged(open);
        ensureGreen(ENSURE_GREEN_TIMEOUT, index);
    }

    protected void deleteIndex(String... indices) {
        assertAcked(client().admin().indices().prepareDelete(indices).get());
    }
}

/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.storage.warmfullfile;

import org.opensearch.action.admin.indices.cache.clear.ClearIndicesCacheResponse;
import org.opensearch.action.bulk.BulkRequest;
import org.opensearch.action.bulk.BulkResponse;
import org.opensearch.action.search.SearchResponse;
import org.opensearch.common.settings.Settings;
import org.opensearch.core.common.unit.ByteSizeUnit;
import org.opensearch.core.common.unit.ByteSizeValue;
import org.opensearch.index.TieredMergePolicyProvider;
import org.opensearch.node.Node;
import org.opensearch.search.aggregations.AggregationBuilders;
import org.opensearch.search.aggregations.bucket.terms.Terms;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

import static org.opensearch.test.hamcrest.OpenSearchAssertions.assertAcked;
import static org.opensearch.test.hamcrest.OpenSearchAssertions.assertNoFailures;
import static org.hamcrest.Matchers.greaterThan;
import static org.hamcrest.Matchers.greaterThanOrEqualTo;

/**
 * Promoted whole files that share one cache segment's budget. A promotion that does not fit is declined and evicts the
 * whole file used longest ago to make room, and clearing the file cache drops a promoted file under its open reader.
 * An evicted file answers from blocks with the same results and is promoted again by its next searches.
 *
 * <p>Uses doc-values files ({@code .dvd}). The whole-file budget is one per file cache segment, and a file lands in
 * the segment its path hashes to, so the second index's merged segment is named so that its {@code .dvd} shares the
 * first index's cache segment. The budget fraction is set so that one such file fits the segment's budget and two
 * do not.
 */
public class WarmFullFileEvictionIT extends WarmFullFileBaseIT {

    private static final String INDEX_A = "warm-evict-a";
    private static final String INDEX_B = "warm-evict-b";
    private static final String DVD_EXTENSION = "dvd";
    private static final String KEYWORD_FIELD = "kw";
    private static final String KEYWORD_MAPPING = "{\"properties\":{\"" + KEYWORD_FIELD + "\":{\"type\":\"keyword\",\"index\":false}}}";
    /**
     * Per file cache segment. The whole-file budget is half of it, 16 MB: it holds one {@code .dvd} of about
     * {@code DOCS * BODY_LENGTH} bytes, 10 MB, and not two.
     */
    private static final long CACHE_PER_SEGMENT = new ByteSizeValue(32, ByteSizeUnit.MB).getBytes();
    /** Documents per index: a doc-values file over one block and under the budget. */
    private static final int DOCS = 640;
    /** How many segment names to try when steering the second index's file into the first one's cache segment. */
    private static final int MAX_STEERING_FLUSHES = 400;

    /**
     * The file cache is split into a power-of-two number of segments by the machine's processor count, so the cache
     * size is scaled with it to keep the per-segment budget, half a segment, the same on every machine.
     */
    @Override
    protected Settings nodeSettings(int nodeOrdinal) {
        int processors = Runtime.getRuntime().availableProcessors();
        int segments = 1 << (Integer.SIZE - Integer.numberOfLeadingZeros(processors - 1));
        return Settings.builder()
            .put(super.nodeSettings(nodeOrdinal))
            .put(Node.NODE_SEARCH_CACHE_SIZE_SETTING.getKey(), new ByteSizeValue(CACHE_PER_SEGMENT * segments).toString())
            .build();
    }

    public void testAFullBudgetEvictsTheWholeFileUsedLongestAgoAndClearingTheCacheDropsOneUnderItsReader() throws Exception {
        String warmNode = startCluster();
        createKeywordIndex(INDEX_A);
        indexKeywordDocs(INDEX_A, 0, DOCS);
        mergeToBlocks(INDEX_A, warmShard(INDEX_A, warmNode));
        WarmShard a = warmShard(INDEX_A, warmNode);
        String dvdA = dvd(a);
        int targetSegment = a.fileCache.segmentIndexFor(a.directory.getFilePath(dvdA));

        createKeywordIndex(INDEX_B);
        indexKeywordDocs(INDEX_B, 0, DOCS);
        steerMergedSegmentIntoCacheSegment(INDEX_B, warmShard(INDEX_B, warmNode), targetSegment);
        mergeToBlocks(INDEX_B, warmShard(INDEX_B, warmNode));
        WarmShard b = warmShard(INDEX_B, warmNode);
        String dvdB = dvd(b);
        assertEquals(
            "the second file must share the first one's cache segment",
            targetSegment,
            b.fileCache.segmentIndexFor(b.directory.getFilePath(dvdB))
        );

        setFullFileExtensions(DVD_EXTENSION);
        reopenIndex(INDEX_A);
        reopenIndex(INDEX_B);
        a = warmShard(INDEX_A, warmNode);
        b = warmShard(INDEX_B, warmNode);
        waitForBlockMode(a);
        waitForBlockMode(b);
        long lengthA = a.directory.fileLength(dvdA);
        long lengthB = b.directory.fileLength(dvdB);
        long budget = perSegmentBudget(a);
        assertAdmissibleLength(dvdA, lengthA, budget);
        assertAdmissibleLength(dvdB, lengthB, budget);
        assertThat("the budget must not hold both files", lengthA + lengthB, greaterThan(budget));
        Path wholeFileA = a.directory.getFilePath(dvdA);
        Path wholeFileB = b.directory.getFilePath(dvdB);
        WarmFullFileManager manager = a.manager;

        // A promotes. B's first promotion finds the budget full: it is declined, and A, the only other whole file of
        // the segment, is evicted to make room. B is promoted by its next searches once A's release has given the
        // budget back.
        searchUntilPromoted(INDEX_A, a, dvdA);
        String fingerprintA = fingerprint(INDEX_A);
        FullFileState familyA = requireFamilyState(a, dvdA);
        searchUntilPromoted(INDEX_B, b, dvdB);
        FullFileState familyB = requireFamilyState(b, dvdB);
        assertThat(manager.counters().get("declined.BUDGET_EXCEEDED"), greaterThanOrEqualTo(1L));
        assertEquals(1L, manager.counters().get("evictedForBudget").longValue());
        assertEquals(1L, manager.counters().get("evicted").longValue());
        assertEquals(lengthA, manager.counters().get("evictedBytes").longValue());
        assertEquals(FullFileState.State.BLOCK, familyA.state());
        assertBusy(() -> {
            assertTrue("A's mapping released under its open reader", familyA.cleanupDone());
            assertFalse("A's whole file deleted", Files.exists(wholeFileA));
            assertEquals("only B holds budget", lengthB, manager.counters().get("reservedBytes").longValue());
        }, DOWNLOAD_TIMEOUT.seconds(), TimeUnit.SECONDS);
        String fingerprintB = fingerprint(INDEX_B);

        // Clearing B's file cache drops its whole file, an unreferenced entry between searches: its mapping is released
        // under its open reader and its budget goes back. Nothing is evicted for the budget this time.
        ClearIndicesCacheResponse cleared = client().admin().indices().prepareClearCache(INDEX_B).setFileCache(true).get();
        assertEquals(cleared.toString(), 0, cleared.getFailedShards());
        assertEquals(2L, manager.counters().get("evicted").longValue());
        assertEquals(lengthA + lengthB, manager.counters().get("evictedBytes").longValue());
        assertEquals(1L, manager.counters().get("evictedForBudget").longValue());
        assertEquals(FullFileState.State.BLOCK, familyB.state());
        assertBusy(() -> {
            assertTrue("B's mapping released under its open reader", familyB.cleanupDone());
            assertFalse("B's whole file deleted", Files.exists(wholeFileB));
            assertEquals("B's budget given back", 0L, manager.counters().get("reservedBytes").longValue());
        }, DOWNLOAD_TIMEOUT.seconds(), TimeUnit.SECONDS);

        // B answers the same from blocks, and its next searches promote it again.
        assertEquals(fingerprintB, fingerprint(INDEX_B));
        searchUntilPromoted(INDEX_B, b, dvdB);
        // The promotion is counted just after the family turns FULL.
        assertBusy(() -> assertEquals(3L, manager.counters().get("promoted").longValue()), WAIT_TIMEOUT.seconds(), TimeUnit.SECONDS);
        assertEquals(fingerprintB, fingerprint(INDEX_B));
        assertEquals(FullFileState.State.FULL, familyB.state());

        // A, evicted for the budget, answers the same from blocks. Its searches ask for its whole file again, which
        // takes the budget back from B the same way.
        assertEquals(fingerprintA, fingerprint(INDEX_A));
        searchUntilPromoted(INDEX_A, a, dvdA);
        assertEquals(2L, manager.counters().get("evictedForBudget").longValue());
        assertEquals(FullFileState.State.BLOCK, familyB.state());
        assertEquals(fingerprintA, fingerprint(INDEX_A));
        assertEquals(FullFileState.State.FULL, familyA.state());

        deleteIndex(INDEX_A, INDEX_B);
    }

    private void createKeywordIndex(String index) {
        Settings settings = Settings.builder()
            .put(warmIndexSettings(defaultNioExtensions()))
            // No background merges: the number of segments decides the merged segment's name (see steering).
            .put(TieredMergePolicyProvider.INDEX_MERGE_POLICY_SEGMENTS_PER_TIER_SETTING.getKey(), 1000.0)
            .put(TieredMergePolicyProvider.INDEX_MERGE_POLICY_MAX_MERGE_AT_ONCE_SETTING.getKey(), 1000)
            .build();
        assertAcked(client().admin().indices().prepareCreate(index).setSettings(settings).setMapping(KEYWORD_MAPPING).get());
        ensureGreen(index);
    }

    /** Indexes documents whose keyword is {@link #BODY_LENGTH} random letters, so the doc values file is a whole-file candidate. */
    private void indexKeywordDocs(String index, int from, int to) {
        for (int start = from; start < to; start += BULK_SIZE) {
            BulkRequest bulk = new BulkRequest();
            for (int i = start; i < Math.min(to, start + BULK_SIZE); i++) {
                bulk.add(client().prepareIndex(index).setId(docId(i)).setSource(KEYWORD_FIELD, randomAlphaOfLength(BODY_LENGTH)).request());
            }
            BulkResponse response = client().bulk(bulk).actionGet();
            assertFalse(response.buildFailureMessage(), response.hasFailures());
        }
        refresh(index);
    }

    /**
     * Lucene names each new segment {@code _N} with a counter that every flushed or merged segment advances, so the
     * segment a force merge produces is named by how many segments exist before it. Flushes one-document segments
     * until the merged segment's {@code .dvd} would hash to the target file cache segment.
     */
    private void steerMergedSegmentIntoCacheSegment(String index, WarmShard warmShard, int targetSegment) throws Exception {
        Set<String> files = segmentFiles(warmShard.shard);
        long counter = files.stream().map(WarmFullFileEvictionIT::segmentName).mapToLong(name -> Long.parseLong(name, 36)).max().orElse(-1)
            + 1;
        // A doc-values file is named <segment>_<format>_<suffix>.dvd; keep the part after the segment name.
        String dvd = files.stream().filter(f -> f.endsWith("." + DVD_EXTENSION)).findFirst().orElseThrow();
        String suffix = dvd.substring(1 + segmentName(dvd).length());
        int flushes = -1;
        for (int extra = 0; extra < MAX_STEERING_FLUSHES; extra++) {
            String candidate = "_" + Long.toString(counter + extra, 36) + suffix;
            if (warmShard.fileCache.segmentIndexFor(warmShard.directory.getFilePath(candidate)) == targetSegment) {
                flushes = extra;
                break;
            }
        }
        assertTrue("no segment name within " + MAX_STEERING_FLUSHES + " flushes hashes to cache segment " + targetSegment, flushes >= 0);
        for (int i = 0; i < flushes; i++) {
            client().prepareIndex(index).setId("steer-" + i).setSource(KEYWORD_FIELD, "steer").get();
            refresh(index);
        }
        logger.info(
            "--> steering {}: {} extra flushes so the merged segment's file is {}",
            index,
            flushes,
            "_" + Long.toString(counter + flushes, 36) + suffix
        );
    }

    /** The segment name of a segment file without its leading underscore: {@code _3f_Lucene90_0.dvd} is {@code 3f}. */
    private static String segmentName(String file) {
        int end = 1;
        while (end < file.length() && file.charAt(end) != '_' && file.charAt(end) != '.') {
            end++;
        }
        return file.substring(1, end);
    }

    /** The single .dvd of the force-merged segment. */
    private static String dvd(WarmShard warmShard) throws Exception {
        List<String> dvds = segmentFiles(warmShard.shard).stream()
            .filter(f -> f.endsWith("." + DVD_EXTENSION))
            .sorted()
            .collect(Collectors.toList());
        assertEquals("expected exactly one doc-values file after the force merge: " + dvds, 1, dvds.size());
        return dvds.get(0);
    }

    /**
     * A terms aggregation on the keyword field, which reads the segment's doc values on every search: a size-0 request
     * is served from the shard request cache by default, which would read nothing after the first search.
     */
    private SearchResponse searchKeywords(String index) {
        SearchResponse response = client().prepareSearch(index)
            .setSize(0)
            .setRequestCache(false)
            .addAggregation(AggregationBuilders.terms("top").field(KEYWORD_FIELD).size(1))
            .get();
        assertNoFailures(response);
        return response;
    }

    private void searchUntilPromoted(String index, WarmShard warmShard, String dvd) throws Exception {
        assertBusy(() -> {
            searchKeywords(index);
            assertTrue(
                "whole file not promoted yet, family state "
                    + stateOf(warmShard, dvd)
                    + ", retry at "
                    + requireFamilyState(warmShard, dvd).retryAt()
                    + " (now "
                    + warmShard.manager.relativeTimeInMillis()
                    + "), counters "
                    + warmShard.manager.counters(),
                isPromoted(warmShard, dvd)
            );
        }, DOWNLOAD_TIMEOUT.seconds(), TimeUnit.SECONDS);
    }

    /** The hit count and the top keyword bucket, which are the same whether the file is read from blocks or whole. */
    private String fingerprint(String index) {
        SearchResponse response = searchKeywords(index);
        Terms terms = response.getAggregations().get("top");
        Map<String, Long> top = terms.getBuckets()
            .stream()
            .collect(Collectors.toMap(Terms.Bucket::getKeyAsString, Terms.Bucket::getDocCount));
        return response.getHits().getTotalHits().value() + ":" + top;
    }
}

/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.index.store.remote.utils.cache;

import org.opensearch.common.io.PathUtils;
import org.opensearch.index.store.remote.filecache.CachedIndexInput;
import org.opensearch.index.store.remote.filecache.FileCache;
import org.opensearch.storage.warmfullfile.WarmFullFileManager;
import org.opensearch.test.OpenSearchTestCase;

import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;

/**
 * Guards {@link WarmFullFileManager}'s re-derivation of {@link SegmentedCache#segmentFor}: the manager computes a
 * segment index from the same path hash and segment count instead of changing {@code SegmentedCache}. The per-segment
 * whole-file budget is correct as long as two paths share a budget counter exactly when they share a cache segment, and
 * the manager's segment count equals the node cache's. This test lives in the cache's package because
 * {@code segmentFor} is package-private.
 */
public class WarmFullFileSegmentIndexTests extends OpenSearchTestCase {

    private static SegmentedCache<Path, CachedIndexInput> newCache(int concurrencyLevel) {
        return SegmentedCache.<Path, CachedIndexInput>builder()
            .capacity(1_000_000)
            .weigher(CachedIndexInput::length)
            .listener(n -> {})
            .concurrencyLevel(concurrencyLevel)
            .build();
    }

    /** Same shape as the node FileCache built by NodeCacheService: default concurrency level (availableProcessors). */
    private static SegmentedCache<Path, CachedIndexInput> newDefaultCache() {
        return SegmentedCache.<Path, CachedIndexInput>builder()
            .capacity(1_000_000)
            .weigher(CachedIndexInput::length)
            .listener(n -> {})
            .build();
    }

    /** capacity() is perSegmentCapacity * segments, so this is exact. */
    private static int segmentCount(SegmentedCache<?, ?> cache) {
        return (int) (cache.capacity() / cache.getPerSegmentCapacity());
    }

    /** Realistic FileCache keys: shard-directory-shaped paths with Lucene-style file names. */
    private List<Path> randomShardFilePaths(int count) {
        List<Path> paths = new ArrayList<>(count);
        for (int i = 0; i < count; i++) {
            paths.add(
                PathUtils.get(
                    "/",
                    "data",
                    "nodes",
                    String.valueOf(randomIntBetween(0, 3)),
                    "indices",
                    randomAlphaOfLength(22),
                    String.valueOf(randomIntBetween(0, 15)),
                    "index",
                    "_" + Integer.toString(randomIntBetween(0, 99_999), 36) + "." + randomFrom("faiss", "faissc", "veq", "si", "fdt")
                )
            );
        }
        return paths;
    }

    private void assertSameIndexIffSameSegment(SegmentedCache<Path, CachedIndexInput> cache, int segments, List<Path> paths) {
        for (int i = 0; i < paths.size(); i++) {
            Path a = paths.get(i);
            int ia = WarmFullFileManager.segmentIndexFor(a, segments);
            assertTrue("index in range", ia >= 0 && ia < segments);
            for (int j = i + 1; j < paths.size(); j++) {
                Path b = paths.get(j);
                boolean sameSegment = cache.segmentFor(a) == cache.segmentFor(b);
                boolean sameIndex = ia == WarmFullFileManager.segmentIndexFor(b, segments);
                assertEquals("segments=" + segments + " a=" + a + " b=" + b, sameSegment, sameIndex);
            }
        }
    }

    public void testSameIndexIffSameSegmentForSeveralSegmentCounts() {
        List<Path> paths = randomShardFilePaths(200);
        for (int concurrencyLevel : new int[] { 1, 2, 3, 4, 7, 8, 16, 32, 64, 96 }) {
            SegmentedCache<Path, CachedIndexInput> cache = newCache(concurrencyLevel);
            int segments = segmentCount(cache);
            assertEquals("power of two", 0, segments & (segments - 1));
            assertTrue(
                "ceilingNextPowerOfTwo(concurrencyLevel)",
                segments >= concurrencyLevel && segments < 2 * Math.max(1, concurrencyLevel)
            );
            assertSameIndexIffSameSegment(cache, segments, paths);
        }
    }

    public void testProductionManagerSegmentCountMatchesDefaultFileCache() {
        SegmentedCache<Path, CachedIndexInput> cache = newDefaultCache();
        FileCache fileCache = new FileCache(cache);
        // Production constructor: segments derive from Runtime.availableProcessors(), slots from allocatedProcessors.
        WarmFullFileManager manager = new WarmFullFileManager(fileCache, Runnable::run, () -> 0L, 8, (task, delayMillis) -> {});

        assertEquals("manager budgets over the same number of segments as the node cache", segmentCount(cache), manager.segments());
        assertSameIndexIffSameSegment(cache, manager.segments(), randomShardFilePaths(200));
    }
}

/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.storage;

import org.apache.logging.log4j.Level;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.core.LogEvent;
import org.opensearch.action.search.SearchResponse;
import org.opensearch.common.regex.Regex;
import org.opensearch.index.query.QueryBuilders;
import org.opensearch.index.store.remote.filecache.CachedIndexInput;
import org.opensearch.index.store.remote.utils.FileTypeUtils;
import org.opensearch.storage.warmfullfile.FullFileState;
import org.opensearch.storage.warmfullfile.WarmFullFileManager;
import org.opensearch.test.MockLogAppender;

import java.io.IOException;
import java.nio.file.Path;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import static org.opensearch.test.hamcrest.OpenSearchAssertions.assertHitCount;

/**
 * Integration tests for warm-tier whole-file promotion of a file that the shard's local hybridfs directory serves
 * with NIO. The {@code .fdt} extension is on the default {@code index.store.hybrid.nio.extensions} list, so once the
 * whole file is downloaded the master input is not Lucene's mmap input: the promotion is declined for the lifetime
 * of the file family, the downloaded copy is dropped, and the node logs one WARN per extension.
 */
public class WarmFullFileNioIT extends WarmFullFileBaseIT {

    private static final String NIO_INDEX = "warm-nio-idx";
    private static final String NIO_INDEX_2 = "warm-nio-idx-2";
    private static final String MMAP_INDEX = "warm-mmap-idx";
    private static final String KNOWN_DOC_ID = docId(0);
    private static final String MANAGER_LOGGER = WarmFullFileManager.class.getName();
    // The one-per-extension WARN logged when a downloaded whole file is served by NIO instead of mmap, up to the point
    // where it names the first declined file.
    private static final String FDT_DECLINED_WARN_PREFIX = "*whole-file promotion of ["
        + FDT_SUFFIX
        + "] files is permanently declined on this node:*not mmap";
    private static final String FDT_DECLINED_WARN = FDT_DECLINED_WARN_PREFIX + "*";

    /** The one-per-extension WARN for .fdt naming the given whole-file path as the first declined file. */
    private static String fdtDeclinedWarnFor(Path firstFile) {
        return FDT_DECLINED_WARN_PREFIX + " (first file [" + firstFile + "])*";
    }

    /** Any WARN naming the given whole-file path as the first declined file. */
    private static String firstFileWarnFor(Path firstFile) {
        return "*first file [" + firstFile + "]*";
    }

    /** The default list, which includes {@code fdt}: stored-fields files are served with NIO. */
    @Override
    protected List<String> nioExtensions() {
        return defaultNioExtensions();
    }

    /**
     * One NIO-served .fdt: the first search downloads the whole file once, the download is declined because the
     * master is an NIO input, the copy is removed, and the family is never retried while it lives.
     */
    public void testNioServedFileIsDownloadedOnceDeclinedAndNeverRetried() throws Exception {
        String warmNode = startCluster();
        createWarmIndex(NIO_INDEX);
        String knownBody = indexDocs(NIO_INDEX, 0, NUM_DOCS);
        mergeToBlocks(NIO_INDEX, warmShard(NIO_INDEX, warmNode));
        enableFdtPromotion();
        WarmShard nio = reopenAndVerify(NIO_INDEX, warmNode);
        WarmFullFileManager manager = nio.manager;

        String fdt = eligibleFdt(nio);
        Path wholeFileKey = nio.directory.getFilePath(fdt);
        assertStateLengthMatchesDirectory(nio, fdt);

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
            }, WAIT_TIMEOUT.seconds(), TimeUnit.SECONDS);
            // The download completes and the family is declined for good.
            assertBusy(() -> {
                FullFileState state = requireFamilyState(nio, fdt);
                assertEquals(Long.MAX_VALUE, state.retryAt());
                assertEquals(FullFileState.State.BLOCK, state.state());
            }, WAIT_TIMEOUT.seconds(), TimeUnit.SECONDS);

            // Later searches keep working from blocks and never start another download.
            for (int i = 0; i < 100; i++) {
                assertSearchCorrect(NIO_INDEX, knownBody);
            }
            assertEquals("exactly one whole-file download", 1, downloads.get());

            // The downloaded copy is gone: no plain .fdt in the shard directory and no whole-file cache entry.
            assertBusy(() -> assertNoWholeFile(nio, fdt), WAIT_TIMEOUT.seconds(), TimeUnit.SECONDS);
            assertTrue(
                "blocks are served locally",
                localFiles(nio).stream().anyMatch(f -> f.startsWith(fdt + FileTypeUtils.BLOCK_FILE_IDENTIFIER))
            );

            // retryAt == Long.MAX_VALUE: the family stays BLOCK and the trigger can never fire again while it lives.
            FullFileState state = requireFamilyState(nio, fdt);
            assertEquals(FullFileState.State.BLOCK, state.state());
            assertEquals(Long.MAX_VALUE, state.retryAt());
            appender.assertAllExpectationsMatched();
        } finally {
            manager.setBeforeDownloadHook(null);
        }

        deleteIndex(NIO_INDEX);
    }

    /**
     * Two warm indices, each with an NIO-served .fdt: each file is downloaded and declined, but only the first
     * decline of the extension is a WARN; the second file's decline is not.
     */
    public void testOnlyOneWarnPerExtensionAcrossSegments() throws Exception {
        String warmNode = startCluster();
        createWarmIndex(NIO_INDEX);
        createWarmIndex(NIO_INDEX_2);
        String knownBody1 = indexDocs(NIO_INDEX, 0, NUM_DOCS);
        String knownBody2 = indexDocs(NIO_INDEX_2, 0, NUM_DOCS);
        mergeToBlocks(NIO_INDEX, warmShard(NIO_INDEX, warmNode));
        mergeToBlocks(NIO_INDEX_2, warmShard(NIO_INDEX_2, warmNode));
        enableFdtPromotion();
        WarmShard first = reopenAndVerify(NIO_INDEX, warmNode);
        WarmShard second = reopenAndVerify(NIO_INDEX_2, warmNode);
        WarmFullFileManager manager = first.manager;

        String fdt1 = eligibleFdt(first);
        String fdt2 = eligibleFdt(second);
        assertStateLengthMatchesDirectory(first, fdt1);
        assertStateLengthMatchesDirectory(second, fdt2);

        AtomicInteger downloads = new AtomicInteger();
        manager.setBeforeDownloadHook(downloads::incrementAndGet);
        try (MockLogAppender appender = MockLogAppender.createForLoggers(LogManager.getLogger(WarmFullFileManager.class))) {
            appender.addExpectation(
                new MockLogAppender.SeenEventExpectation(
                    "one WARN for the first fdt",
                    MANAGER_LOGGER,
                    Level.WARN,
                    firstFileWarnFor(first.directory.getFilePath(fdt1))
                )
            );
            appender.addExpectation(
                new MockLogAppender.UnseenEventExpectation(
                    "no WARN for the second fdt",
                    MANAGER_LOGGER,
                    Level.WARN,
                    firstFileWarnFor(second.directory.getFilePath(fdt2))
                )
            );
            appender.addExpectation(new CountingEventExpectation(MANAGER_LOGGER, Level.WARN, FDT_DECLINED_WARN, 1));

            // First index: download, decline, WARN.
            assertBusy(() -> {
                assertSearchCorrect(NIO_INDEX, knownBody1);
                assertEquals(1, downloads.get());
            }, WAIT_TIMEOUT.seconds(), TimeUnit.SECONDS);
            assertBusy(
                () -> assertEquals(Long.MAX_VALUE, requireFamilyState(first, fdt1).retryAt()),
                WAIT_TIMEOUT.seconds(),
                TimeUnit.SECONDS
            );

            // Second index: download, decline, no WARN.
            assertBusy(() -> {
                assertSearchCorrect(NIO_INDEX_2, knownBody2);
                assertEquals(2, downloads.get());
            }, WAIT_TIMEOUT.seconds(), TimeUnit.SECONDS);
            assertBusy(
                () -> assertEquals(Long.MAX_VALUE, requireFamilyState(second, fdt2).retryAt()),
                WAIT_TIMEOUT.seconds(),
                TimeUnit.SECONDS
            );

            for (int i = 0; i < 20; i++) {
                assertSearchCorrect(NIO_INDEX, knownBody1);
                assertSearchCorrect(NIO_INDEX_2, knownBody2);
            }
            assertEquals("one download per file", 2, downloads.get());
            assertEquals(FullFileState.State.BLOCK, requireFamilyState(first, fdt1).state());
            assertEquals(FullFileState.State.BLOCK, requireFamilyState(second, fdt2).state());
            assertBusy(() -> {
                assertNoWholeFile(first, fdt1);
                assertNoWholeFile(second, fdt2);
            }, WAIT_TIMEOUT.seconds(), TimeUnit.SECONDS);
            appender.assertAllExpectationsMatched();
        } finally {
            manager.setBeforeDownloadHook(null);
        }

        deleteIndex(NIO_INDEX, NIO_INDEX_2);
    }

    /**
     * The decline is per file family, not per node: on the same node an index whose hybridfs directory serves
     * .fdt with mmap promotes its .fdt to a whole local file while the NIO-served index stays declined.
     */
    public void testMmapServedFileOnSameNodeStillPromotes() throws Exception {
        String warmNode = startCluster();
        createWarmIndex(NIO_INDEX);
        createWarmIndex(MMAP_INDEX, warmIndexSettings(nioExtensionsWithoutFdt()));
        String nioBody = indexDocs(NIO_INDEX, 0, NUM_DOCS);
        String mmapBody = indexDocs(MMAP_INDEX, 0, NUM_DOCS);
        mergeToBlocks(NIO_INDEX, warmShard(NIO_INDEX, warmNode));
        mergeToBlocks(MMAP_INDEX, warmShard(MMAP_INDEX, warmNode));
        enableFdtPromotion();
        WarmShard nio = reopenAndVerify(NIO_INDEX, warmNode);
        WarmShard mmap = reopenAndVerify(MMAP_INDEX, warmNode);
        WarmFullFileManager manager = nio.manager;

        String nioFdt = eligibleFdt(nio);
        String mmapFdt = eligibleFdt(mmap);
        long nioLength = assertStateLengthMatchesDirectory(nio, nioFdt);
        long mmapLength = assertStateLengthMatchesDirectory(mmap, mmapFdt);

        AtomicInteger downloads = new AtomicInteger();
        manager.setBeforeDownloadHook(downloads::incrementAndGet);
        try (MockLogAppender appender = MockLogAppender.createForLoggers(LogManager.getLogger(WarmFullFileManager.class))) {
            appender.addExpectation(
                new MockLogAppender.SeenEventExpectation(
                    "one WARN for the NIO-served fdt",
                    MANAGER_LOGGER,
                    Level.WARN,
                    firstFileWarnFor(nio.directory.getFilePath(nioFdt))
                )
            );
            appender.addExpectation(
                new MockLogAppender.UnseenEventExpectation(
                    "no WARN for the mmap-served fdt",
                    MANAGER_LOGGER,
                    Level.WARN,
                    firstFileWarnFor(mmap.directory.getFilePath(mmapFdt))
                )
            );

            // NIO-served index: downloaded once, declined for good.
            assertBusy(() -> {
                assertSearchCorrect(NIO_INDEX, nioBody);
                assertEquals(1, downloads.get());
            }, WAIT_TIMEOUT.seconds(), TimeUnit.SECONDS);
            assertBusy(
                () -> assertEquals(Long.MAX_VALUE, requireFamilyState(nio, nioFdt).retryAt()),
                WAIT_TIMEOUT.seconds(),
                TimeUnit.SECONDS
            );
            assertEquals(FullFileState.State.BLOCK, requireFamilyState(nio, nioFdt).state());

            // mmap-served index on the same node: downloaded once and promoted to FULL.
            assertBusy(() -> {
                assertSearchCorrect(MMAP_INDEX, mmapBody);
                assertEquals(2, downloads.get());
            }, WAIT_TIMEOUT.seconds(), TimeUnit.SECONDS);
            assertBusy(
                () -> assertEquals(FullFileState.State.FULL, requireFamilyState(mmap, mmapFdt).state()),
                WAIT_TIMEOUT.seconds(),
                TimeUnit.SECONDS
            );

            // The whole file is in the shard directory under its plain name and in the cache at full length.
            assertTrue(localFiles(mmap).contains(mmapFdt));
            Path mmapWholeFileKey = mmap.directory.getFilePath(mmapFdt);
            CachedIndexInput wholeFile = mmap.fileCache.get(mmapWholeFileKey);
            assertNotNull("whole-file cache entry", wholeFile);
            try {
                assertEquals(mmapLength, wholeFile.length());
            } finally {
                mmap.fileCache.decRef(mmapWholeFileKey);
            }

            // Searches on both indices stay correct; the declined family stays declined and the FULL family stays FULL.
            for (int i = 0; i < 20; i++) {
                assertSearchCorrect(NIO_INDEX, nioBody);
                assertSearchCorrect(MMAP_INDEX, mmapBody);
            }
            assertEquals("one download per file", 2, downloads.get());
            FullFileState nioState = requireFamilyState(nio, nioFdt);
            assertEquals(FullFileState.State.BLOCK, nioState.state());
            assertEquals(Long.MAX_VALUE, nioState.retryAt());
            assertEquals(nioLength, nioState.fileLength());
            assertEquals(FullFileState.State.FULL, requireFamilyState(mmap, mmapFdt).state());
            assertBusy(() -> assertNoWholeFile(nio, nioFdt), WAIT_TIMEOUT.seconds(), TimeUnit.SECONDS);
            appender.assertAllExpectationsMatched();
        } finally {
            manager.setBeforeDownloadHook(null);
        }

        deleteIndex(NIO_INDEX, MMAP_INDEX);
    }

    /**
     * Enables whole-file promotion for .fdt. Called only once every index under test is in block mode and right before
     * the reopen, so that no handle is promotable before the test has installed its download hook and log appender.
     */
    private void enableFdtPromotion() {
        setFullFileExtensions(FDT_EXTENSION);
    }

    /** Closes and reopens the index so the searcher's file handles are opened on remote-mode files, and checks the hit count. */
    private WarmShard reopenAndVerify(String index, String warmNode) {
        reopenIndex(index);
        assertHitCount(client().prepareSearch(index).setSize(0).setQuery(QueryBuilders.matchAllQuery()).get(), NUM_DOCS);
        return warmShard(index, warmNode);
    }

    /**
     * Asserts that the length recorded in the family's state is the file's length in the shard directory.
     * @return the file length
     */
    private static long assertStateLengthMatchesDirectory(WarmShard warmShard, String fdt) throws IOException {
        long length = warmShard.directory.fileLength(fdt);
        assertEquals("family state length for " + fdt, length, requireFamilyState(warmShard, fdt).fileLength());
        return length;
    }

    /** Asserts the shard directory holds no plain copy of the file and the cache has no whole-file entry for it. */
    private static void assertNoWholeFile(WarmShard warmShard, String name) throws IOException {
        assertFalse("whole file present: " + name, localFiles(warmShard).contains(name));
        Path wholeFileKey = warmShard.directory.getFilePath(name);
        assertFalse("whole-file cache entry present: " + wholeFileKey, hasWholeFileCacheEntry(warmShard.fileCache, wholeFileKey));
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

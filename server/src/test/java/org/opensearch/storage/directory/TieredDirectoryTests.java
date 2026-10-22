/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.storage.directory;

import com.carrotsearch.randomizedtesting.annotations.ThreadLeakFilters;

import org.apache.logging.log4j.Level;
import org.apache.logging.log4j.LogManager;
import org.apache.lucene.store.DataAccessHint;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.FSDirectory;
import org.apache.lucene.store.FilterIndexInput;
import org.apache.lucene.store.FlushInfo;
import org.apache.lucene.store.IOContext;
import org.apache.lucene.store.IndexInput;
import org.apache.lucene.store.IndexOutput;
import org.apache.lucene.store.MergeInfo;
import org.apache.lucene.store.ReadOnceHint;
import org.opensearch.common.settings.ClusterSettings;
import org.opensearch.common.settings.Settings;
import org.opensearch.index.store.remote.file.CleanerDaemonThreadLeakFilter;
import org.opensearch.index.store.remote.filecache.CachedIndexInput;
import org.opensearch.index.store.remote.filecache.FileCache;
import org.opensearch.index.store.remote.filecache.FileCacheFactory;
import org.opensearch.index.store.remote.utils.FileTypeUtils;
import org.opensearch.plugins.IndexStorePlugin;
import org.opensearch.storage.indexinput.SwitchableIndexInput;
import org.opensearch.storage.indexinput.SwitchableIndexInputWrapper;
import org.opensearch.storage.prefetch.TieredStoragePrefetchSettings;
import org.opensearch.storage.warmfullfile.FullFileState;
import org.opensearch.storage.warmfullfile.WarmFullFileManager;
import org.opensearch.storage.warmfullfile.WarmFullFileSettings;
import org.opensearch.test.MockLogAppender;
import org.opensearch.test.junit.annotations.TestLogging;
import org.junit.Before;

import java.io.IOException;
import java.nio.file.NoSuchFileException;
import java.nio.file.Path;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.TimeUnit;
import java.util.function.Supplier;

import static org.opensearch.common.settings.ClusterSettings.BUILT_IN_CLUSTER_SETTINGS;
import static org.opensearch.storage.utils.DirectoryUtils.getFilePath;
import static org.opensearch.storage.utils.DirectoryUtils.getFilePathSwitchable;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 * Tests for {@link TieredDirectory}.
 */
@ThreadLeakFilters(filters = CleanerDaemonThreadLeakFilter.class)
public class TieredDirectoryTests extends TieredStorageBaseTestCase {

    private FileCache fileCache;
    private FSDirectory localDirectory;
    private TieredDirectory tieredDirectory;

    private static final String[] LOCAL_FILES = new String[] {
        "_1.cfe",
        "_1.cfe_block_0",
        "_1.cfe_block_1",
        "_2.cfe",
        "_0.cfe_block_7",
        "_0.cfs_block_7",
        "_x.abc_block_0",
        "temp_file.tmp" };
    private static final String FILE_PRESENT_LOCALLY = "_1.cfe";
    private static final String FILE_RENAMED = "_1_new.cfe";
    private static final String FILE_PRESENT_IN_REMOTE_ONLY = "_0.si";
    private static final String NON_EXISTENT_FILE = "non_existent_file";
    private static final String TEMP_FILE = "temp_file.tmp";

    @Before
    public void setup() throws IOException {
        setupRemoteSegmentStoreDirectory();
        populateMetadata();
        remoteSegmentStoreDirectory.init();
        localDirectory = FSDirectory.open(createTempDir());
        removeExtraFSFiles();
        int concurrencyLevel = randomIntBetween(1, 2);
        fileCache = FileCacheFactory.createConcurrentLRUFileCache(FILE_CACHE_CAPACITY, concurrencyLevel);
        tieredDirectory = new TieredDirectory(
            localDirectory,
            remoteSegmentStoreDirectory,
            fileCache,
            threadPool,
            getMockPrefetchSettingsSupplier(),
            getMockWarmFullFileSettingsSupplier(),
            null // manager not needed: feature off (empty extensions), nothing promotes
        );
        addFilesToDirectory(LOCAL_FILES);
    }

    private Supplier<TieredStoragePrefetchSettings> getMockPrefetchSettingsSupplier() {
        return () -> {
            TieredStoragePrefetchSettings settings = mock(TieredStoragePrefetchSettings.class);
            when(settings.getReadAheadBlockCount()).thenReturn(TieredStoragePrefetchSettings.DEFAULT_READ_AHEAD_BLOCK_COUNT);
            when(settings.getReadAheadEnableFileFormats()).thenReturn(TieredStoragePrefetchSettings.READ_AHEAD_ENABLE_FILE_FORMATS);
            when(settings.isStoredFieldsPrefetchEnabled()).thenReturn(true);
            return settings;
        };
    }

    // Feature off (empty extensions): no file is eligible, so the fixture directory never promotes anything.
    private Supplier<WarmFullFileSettings> getMockWarmFullFileSettingsSupplier() {
        return () -> {
            WarmFullFileSettings settings = mock(WarmFullFileSettings.class);
            when(settings.getExtensions()).thenReturn(Set.of());
            return settings;
        };
    }

    public void testSecondCacheFileForLiveFamilyReturnsItUnchanged() throws Exception {
        // cacheFile creates a family only when the key has no live entry: a second call for the same file returns the
        // first family, closes nothing, and takes one more reference on it, as a put would have.
        Path key = getFilePathSwitchable(localDirectory, FILE_PRESENT_IN_REMOTE_ONLY);
        CachedIndexInput first = tieredDirectory.cacheFile(FILE_PRESENT_IN_REMOTE_ONLY, true);
        assertNotNull(first);
        assertFalse(first.isClosed());
        assertEquals(Integer.valueOf(1), fileCache.getRef(key));

        CachedIndexInput second = tieredDirectory.cacheFile(FILE_PRESENT_IN_REMOTE_ONLY, true);
        assertSame("the live family is kept", first, second);
        assertFalse("nothing was closed", first.isClosed());
        assertEquals(FullFileState.State.BLOCK, first.fullFileState().state());
        assertEquals("each call leaves one reference for its caller", Integer.valueOf(2), fileCache.getRef(key));
        fileCache.decRef(key);
        fileCache.decRef(key);
    }

    public void testCacheFileReplacesClosedEntry() throws Exception {
        // A closed entry at the key stands for nothing: cacheFile replaces it with a live family.
        Path key = getFilePathSwitchable(localDirectory, FILE_PRESENT_IN_REMOTE_ONLY);
        CachedIndexInput stale = tieredDirectory.cacheFile(FILE_PRESENT_IN_REMOTE_ONLY, true);
        fileCache.decRef(key);
        stale.close();
        assertTrue(stale.isClosed());

        CachedIndexInput live = tieredDirectory.cacheFile(FILE_PRESENT_IN_REMOTE_ONLY, true);
        assertNotSame(stale, live);
        assertFalse(live.isClosed());
        assertEquals(FullFileState.State.BLOCK, live.fullFileState().state());
        fileCache.decRef(key);
    }

    public void testConcurrentOpenInputForMissingFamilyCreatesOneEntry() throws Exception {
        // n threads open the same remote-only file at once while the cache has no entry for it: exactly one family is
        // created, every handle reads the same bytes, and after all handles close the family holds no reference.
        populateData();
        Path key = getFilePathSwitchable(localDirectory, FILE_PRESENT_IN_REMOTE_ONLY);
        assertNull(fileCache.get(key));
        int threads = scaledRandomIntBetween(4, 12);
        CyclicBarrier start = new CyclicBarrier(threads);
        List<IndexInput> handles = new CopyOnWriteArrayList<>();
        List<Throwable> failures = new CopyOnWriteArrayList<>();
        Thread[] openers = new Thread[threads];
        for (int i = 0; i < threads; i++) {
            openers[i] = new Thread(() -> {
                try {
                    start.await();
                    handles.add(tieredDirectory.openInput(FILE_PRESENT_IN_REMOTE_ONLY, IOContext.DEFAULT));
                } catch (Throwable t) {
                    failures.add(t);
                }
            }, "opener-" + i);
            openers[i].start();
        }
        for (Thread opener : openers) {
            opener.join();
        }
        assertTrue("no opener failed: " + failures, failures.isEmpty());
        assertEquals(threads, handles.size());

        CachedIndexInput family = fileCache.get(key);
        assertNotNull("one family was created", family);
        assertFalse(family.isClosed());
        assertEquals(FullFileState.State.BLOCK, family.fullFileState().state());
        fileCache.decRef(key);
        assertEquals("one reference per open handle, none left by openInput itself", Integer.valueOf(threads), fileCache.getRef(key));

        byte expected = handles.get(0).readByte();
        for (IndexInput handle : handles.subList(1, handles.size())) {
            assertEquals("every handle reads the same family", expected, handle.readByte());
        }
        for (IndexInput handle : handles) {
            handle.close();
        }
        assertSame("the family is unchanged after the handles close", family, fileCache.get(key));
        fileCache.decRef(key);
        assertEquals("the handles released their references", Integer.valueOf(0), fileCache.getRef(key));
        assertFalse(family.isClosed());
    }

    public void testListAll() throws IOException {
        String[] actualFileNames = tieredDirectory.listAll();
        String[] expectedFileNames = new String[] {
            "_0.cfe",
            "_0.cfs",
            "_0.si",
            "_1.cfe",
            "_2.cfe",
            "_x.abc",
            "segments_1",
            "temp_file.tmp" };
        assertArrayEquals(expectedFileNames, actualFileNames);
    }

    public void testDeleteFile() throws IOException {
        assertTrue(existsInTieredDirectory(TEMP_FILE));
        tieredDirectory.deleteFile(TEMP_FILE);
        assertFalse(existsInTieredDirectory(TEMP_FILE));

        assertFalse(existsInTieredDirectory(NON_EXISTENT_FILE));
        Exception exception = null;
        try {
            tieredDirectory.deleteFile(NON_EXISTENT_FILE);
        } catch (Exception e) {
            exception = e;
        }
        assertNull(exception);

        assertTrue(existsInTieredDirectory(FILE_PRESENT_LOCALLY));
        assertTrue(existsInLocalDirectory(FILE_PRESENT_LOCALLY));
        assertTrue(existsInFileCache(getFilePathSwitchable(localDirectory, FILE_PRESENT_LOCALLY)));
        assertTrue(existsInFileCache(getFilePath(localDirectory, FILE_PRESENT_LOCALLY)));
        tieredDirectory.deleteFile(FILE_PRESENT_LOCALLY);
        assertFalse(existsInTieredDirectory(FILE_PRESENT_LOCALLY));
        assertFalse(existsInLocalDirectory(FILE_PRESENT_LOCALLY));
        assertFalse(existsInFileCache(getFilePathSwitchable(localDirectory, FILE_PRESENT_LOCALLY)));
        assertFalse(existsInFileCache(getFilePath(localDirectory, FILE_PRESENT_LOCALLY)));

        assertTrue(existsInTieredDirectory(FILE_PRESENT_IN_REMOTE_ONLY));
        assertTrue(existsInRemoteDirectory(FILE_PRESENT_IN_REMOTE_ONLY));
        tieredDirectory.deleteFile(FILE_PRESENT_IN_REMOTE_ONLY);
        assertTrue(existsInTieredDirectory(FILE_PRESENT_IN_REMOTE_ONLY));
        assertTrue(existsInRemoteDirectory(FILE_PRESENT_IN_REMOTE_ONLY));
    }

    public void testRename() throws IOException {
        assertThrows(NoSuchFileException.class, () -> tieredDirectory.rename(NON_EXISTENT_FILE, FILE_RENAMED));

        assertTrue(existsInTieredDirectory(FILE_PRESENT_LOCALLY));
        assertTrue(existsInFileCache(getFilePathSwitchable(localDirectory, FILE_PRESENT_LOCALLY)));
        assertFalse(existsInFileCache(getFilePathSwitchable(localDirectory, FILE_RENAMED)));
        tieredDirectory.rename(FILE_PRESENT_LOCALLY, FILE_RENAMED);
        assertFalse(existsInTieredDirectory(FILE_PRESENT_LOCALLY));
        assertFalse(existsInFileCache(getFilePathSwitchable(localDirectory, FILE_PRESENT_LOCALLY)));
        assertTrue(existsInFileCache(getFilePathSwitchable(localDirectory, FILE_RENAMED)));
    }

    public void testOpenInput() throws IOException {
        populateData();

        assertFalse(existsInTieredDirectory(NON_EXISTENT_FILE));
        assertThrows(NoSuchFileException.class, () -> tieredDirectory.openInput(NON_EXISTENT_FILE, IOContext.DEFAULT));

        assertTrue(existsInLocalDirectory(TEMP_FILE) && FileTypeUtils.isTempFile(TEMP_FILE));

        assertEquals(
            tieredDirectory.openInput(TEMP_FILE, IOContext.DEFAULT).toString(),
            localDirectory.openInput(TEMP_FILE, IOContext.DEFAULT).toString()
        );

        assertNotNull(fileCache.get(getFilePathSwitchable(localDirectory, FILE_PRESENT_LOCALLY)));
        IndexInput indexInput = tieredDirectory.openInput(FILE_PRESENT_LOCALLY, IOContext.DEFAULT);
        assertNotNull(indexInput);
        assertTrue(indexInput instanceof SwitchableIndexInputWrapper);
        assertFalse(getSwitchableIndexInputFromWrapper(indexInput).isCachedFromRemote());

        assertNull(fileCache.get(getFilePathSwitchable(localDirectory, FILE_PRESENT_IN_REMOTE_ONLY)));
        indexInput = tieredDirectory.openInput(FILE_PRESENT_IN_REMOTE_ONLY, IOContext.DEFAULT);
        assertNotNull(indexInput);
        assertTrue(indexInput instanceof SwitchableIndexInputWrapper);
        assertTrue(getSwitchableIndexInputFromWrapper(indexInput).isCachedFromRemote());
    }

    public void testIndexInputCleanup() throws IOException {
        populateData();
        Path path = getFilePathSwitchable(localDirectory, FILE_PRESENT_LOCALLY);
        assertEquals(1, (int) fileCache.getRef(path));
        IndexInput indexInput = tieredDirectory.openInput(FILE_PRESENT_LOCALLY, IOContext.DEFAULT);
        assertEquals(2, (int) fileCache.getRef(path));
        decRefToZero(path);
        createUnclosedClones(indexInput, path);
        triggerGarbageCollectionAndAssertClonesClosed(path);
    }

    public void testFamilyIsMarkedRemoteAfterSyncAndAtCreationFromRemote() throws IOException {
        // A family created for a just-written local file is not promotable; afterSyncToRemote marks it remote once the
        // switch has completed. A family created for a file already in the remote store is promotable from the start.
        populateData();
        String localFile = "_0.si";
        addFilesToDirectory(new String[] { localFile });
        Path localKey = getFilePathSwitchable(localDirectory, localFile);
        CachedIndexInput entry = fileCache.get(localKey);
        try {
            assertFalse("local-mode family not yet promotable", entry.fullFileState().isRemoteMode());
        } finally {
            fileCache.decRef(localKey);
        }

        tieredDirectory.afterSyncToRemote(localFile);

        entry = fileCache.get(localKey);
        try {
            assertTrue("marked remote once the switch completed", entry.fullFileState().isRemoteMode());
        } finally {
            fileCache.decRef(localKey);
        }

        IndexInput remoteHandle = tieredDirectory.openInput(FILE_PRESENT_IN_REMOTE_ONLY, IOContext.DEFAULT);
        Path remoteKey = getFilePathSwitchable(localDirectory, FILE_PRESENT_IN_REMOTE_ONLY);
        entry = fileCache.get(remoteKey);
        try {
            assertTrue("family created from remote is promotable at once", entry.fullFileState().isRemoteMode());
        } finally {
            fileCache.decRef(remoteKey);
        }
        remoteHandle.close();
    }

    public void testFamilyIsNotMarkedRemoteWhenSwitchFails() throws IOException {
        // FILE_PRESENT_LOCALLY is not registered in the remote store metadata, so switchToRemote rejects it and the
        // family keeps serving the local copy. It must not be flagged remote-mode, or whole-file promotion could
        // remove the only copy of the file.
        Path localKey = getFilePathSwitchable(localDirectory, FILE_PRESENT_LOCALLY);
        assertTrue(existsInLocalDirectory(FILE_PRESENT_LOCALLY));

        tieredDirectory.afterSyncToRemote(FILE_PRESENT_LOCALLY);

        assertTrue("local copy is still the only copy", existsInLocalDirectory(FILE_PRESENT_LOCALLY));
        CachedIndexInput entry = fileCache.get(localKey);
        assertNotNull(entry);
        try {
            SwitchableIndexInput original = (SwitchableIndexInput) entry.getIndexInput();
            assertFalse("switch did not happen", original.hasSwitchedToRemote());
            assertFalse("family stays non-promotable while serving the local copy", entry.fullFileState().isRemoteMode());
        } finally {
            fileCache.decRef(localKey);
        }
    }

    public void testSwitchFileToRemoteWithoutCacheEntryDoesNotThrow() {
        Path key = getFilePathSwitchable(localDirectory, NON_EXISTENT_FILE);
        assertNull(fileCache.getRef(key));

        tieredDirectory.switchFileToRemote(NON_EXISTENT_FILE);

        assertNull(fileCache.getRef(key));
    }

    public void testAfterSyncToRemote() throws IOException {
        populateData();
        String fileName = "_0.si";
        String fileNameBlock = "_0.si_block_0";
        addFilesToDirectory(new String[] { fileName });

        assertTrue(existsInLocalDirectory(fileName));
        assertEquals(1, (int) fileCache.getRef(getFilePathSwitchable(localDirectory, fileName)));
        assertEquals(1, (int) fileCache.getRef(getFilePath(localDirectory, fileName)));
        assertNull(fileCache.getRef(getFilePath(localDirectory, fileNameBlock)));

        IndexInput indexInput = tieredDirectory.openInput(fileName, IOContext.DEFAULT);

        assertTrue(indexInput instanceof SwitchableIndexInputWrapper);
        assertEquals(2, (int) fileCache.getRef(getFilePathSwitchable(localDirectory, fileName)));
        assertEquals(2, (int) fileCache.getRef(getFilePath(localDirectory, fileName)));
        assertFalse(getSwitchableIndexInputFromWrapper(indexInput).isCachedFromRemote());

        indexInput.close();

        assertEquals(1, (int) fileCache.getRef(getFilePathSwitchable(localDirectory, fileName)));
        assertEquals(1, (int) fileCache.getRef(getFilePath(localDirectory, fileName)));

        tieredDirectory.afterSyncToRemote(fileName);

        assertFalse(existsInLocalDirectory(fileName));
        assertTrue(existsInRemoteDirectory(fileName));
        assertEquals(0, (int) fileCache.getRef(getFilePathSwitchable(localDirectory, fileName)));
        assertNull(fileCache.getRef(getFilePath(localDirectory, fileName)));
        assertNull(fileCache.getRef(getFilePath(localDirectory, fileNameBlock)));

        indexInput = tieredDirectory.openInput(fileName, IOContext.DEFAULT);

        assertEquals(1, (int) fileCache.getRef(getFilePathSwitchable(localDirectory, fileName)));
        assertEquals(1, (int) fileCache.getRef(getFilePath(localDirectory, fileNameBlock)));

        indexInput.close();

        assertEquals(0, (int) fileCache.getRef(getFilePathSwitchable(localDirectory, fileName)));
        assertEquals(0, (int) fileCache.getRef(getFilePath(localDirectory, fileNameBlock)));

        fileCache.prune();
        assertNull(fileCache.getRef(getFilePathSwitchable(localDirectory, fileName)));
        assertNull(fileCache.getRef(getFilePath(localDirectory, fileNameBlock)));
    }

    // ---------------- whole-file eligibility ----------------

    private static final long ONE_BLOCK_BYTES = TieredDirectory.ONE_BLOCK_BYTES;
    private static final String FAISS_FILE = "_0_NativeEngines990KnnVectorsFormat_0.faiss";

    private static ClusterSettings newClusterSettings() {
        return new ClusterSettings(Settings.EMPTY, BUILT_IN_CLUSTER_SETTINGS);
    }

    private static void setExtensions(ClusterSettings clusterSettings, String... extensions) {
        clusterSettings.applySettings(Settings.builder().putList(WarmFullFileSettings.EXTENSIONS.getKey(), List.of(extensions)).build());
    }

    /** A real WarmFullFileSettings over its own ClusterSettings, already configured with the given extensions. */
    private static WarmFullFileSettings realSettings(String... extensions) {
        ClusterSettings clusterSettings = newClusterSettings();
        WarmFullFileSettings settings = new WarmFullFileSettings(clusterSettings);
        if (extensions.length > 0) {
            setExtensions(clusterSettings, extensions);
        }
        return settings;
    }

    /** A second TieredDirectory over the same local directory, remote directory and file cache as the fixture's. */
    private TieredDirectory directoryWith(Supplier<WarmFullFileSettings> settingsSupplier, WarmFullFileManager manager) {
        return new TieredDirectory(
            localDirectory,
            remoteSegmentStoreDirectory,
            fileCache,
            threadPool,
            getMockPrefetchSettingsSupplier(),
            settingsSupplier,
            manager
        );
    }

    private WarmFullFileManager recordingManager(RecordingExecutor executor) {
        return new WarmFullFileManager(fileCache, executor, () -> 0L, 8, (task, delayMillis) -> {});
    }

    /** An original input in remote (block) mode whose whole-file length is {@code length}, as openInput passes it. */
    private static SwitchableIndexInput remoteOriginal(long length) {
        SwitchableIndexInput original = mock(SwitchableIndexInput.class);
        when(original.length()).thenReturn(length);
        when(original.hasSwitchedToRemote()).thenReturn(true);
        return original;
    }

    private FullFileState familyStateOf(String name) {
        Path key = getFilePathSwitchable(localDirectory, name);
        CachedIndexInput entry = fileCache.get(key);
        assertNotNull("switchable entry for " + name, entry);
        fileCache.decRef(key);
        return entry.fullFileState();
    }

    public void testEligibilityIgnoresTheExtensionAtOpen() {
        // The setting is read at trigger time, not at open, so a handle opened before an extension is configured can
        // promote later and one opened while it was configured stops when it is removed. Open-time eligibility is the
        // same with the setting empty, with the file's extension listed, and with another extension listed.
        SwitchableIndexInput original = remoteOriginal(ONE_BLOCK_BYTES + 1);
        for (TieredDirectory dir : List.of(
            directoryWith(() -> realSettings(), null),
            directoryWith(() -> realSettings(".FAISS", "veq"), null),
            directoryWith(() -> realSettings("vec"), null)
        )) {
            assertTrue(dir.isEligibleForFullFile(FAISS_FILE, IOContext.DEFAULT, original));
            assertTrue(dir.isEligibleForFullFile("_0_Lucene99_0.veq", IOContext.DEFAULT, original));
            assertTrue(dir.isEligibleForFullFile("_0_Lucene99_0.vec", IOContext.DEFAULT, original));
            assertTrue(
                "a file without an extension passes the open-time rules",
                dir.isEligibleForFullFile("faiss", IOContext.DEFAULT, original)
            );
        }
    }

    public void testEligibilityExcludesLuceneSpecialFiles() {
        TieredDirectory dir = directoryWith(() -> realSettings("cfs", "lock", "si", "faiss"), null);
        SwitchableIndexInput original = remoteOriginal(ONE_BLOCK_BYTES + 1);

        assertTrue("control: a plain file of a configured extension", dir.isEligibleForFullFile("_0.si", IOContext.DEFAULT, original));
        assertFalse(".cfs is excluded even when configured", dir.isEligibleForFullFile("_0.cfs", IOContext.DEFAULT, original));
        assertFalse("segments_N", dir.isEligibleForFullFile("segments_1", IOContext.DEFAULT, original));
        assertFalse(
            "segments_N with a configured-looking suffix",
            dir.isEligibleForFullFile("segments_1.faiss", IOContext.DEFAULT, original)
        );
        assertFalse("write.lock", dir.isEligibleForFullFile("write.lock", IOContext.DEFAULT, original));
    }

    @TestLogging(value = "org.opensearch.storage.directory.TieredDirectory:DEBUG", reason = "the rejection line is logged at DEBUG")
    public void testEligibilityRejectionOfConfiguredExtensionIsLoggedAtDebug() throws Exception {
        TieredDirectory dir = directoryWith(() -> realSettings("faiss", "cfs"), null);
        SwitchableIndexInput original = remoteOriginal(ONE_BLOCK_BYTES + 1);
        String loggerName = TieredDirectory.class.getName();
        try (MockLogAppender appender = MockLogAppender.createForLoggers(LogManager.getLogger(TieredDirectory.class))) {
            appender.addExpectation(
                new MockLogAppender.SeenEventExpectation(
                    "compound file rejection names the rule",
                    loggerName,
                    Level.DEBUG,
                    "*[_0.cfs]*not eligible: segments, lock or compound file*"
                )
            );
            appender.addExpectation(
                new MockLogAppender.SeenEventExpectation(
                    "read-once rejection names the rule",
                    loggerName,
                    Level.DEBUG,
                    "*[_0.faiss]*not eligible: read-once or sequential hint*"
                )
            );
            appender.addExpectation(
                new MockLogAppender.UnseenEventExpectation("an unconfigured extension is not logged", loggerName, Level.DEBUG, "*[_0.vec]*")
            );
            appender.addExpectation(
                new MockLogAppender.UnseenEventExpectation("an eligible file is not logged", loggerName, Level.DEBUG, "*[_1.faiss]*")
            );
            assertFalse(dir.isEligibleForFullFile("_0.cfs", IOContext.DEFAULT, original));
            assertFalse(dir.isEligibleForFullFile("_0.faiss", IOContext.READONCE, original));
            assertFalse(
                "rejected, but its extension is not configured, so not logged",
                dir.isEligibleForFullFile("_0.vec", IOContext.READONCE, original)
            );
            assertTrue(dir.isEligibleForFullFile("_1.faiss", IOContext.DEFAULT, original));
            appender.assertAllExpectationsMatched();
        }
    }

    public void testEligibilityRequiresPlainDefaultReadContext() {
        TieredDirectory dir = directoryWith(() -> realSettings("faiss"), null);
        SwitchableIndexInput original = remoteOriginal(ONE_BLOCK_BYTES + 1);

        assertTrue(dir.isEligibleForFullFile(FAISS_FILE, IOContext.DEFAULT, original));
        assertTrue(
            "RANDOM hint is a plain read",
            dir.isEligibleForFullFile(FAISS_FILE, IOContext.DEFAULT.withHints(DataAccessHint.RANDOM), original)
        );
        assertFalse("READONCE", dir.isEligibleForFullFile(FAISS_FILE, IOContext.READONCE, original));
        assertFalse(
            "DEFAULT with a ReadOnce hint",
            dir.isEligibleForFullFile(FAISS_FILE, IOContext.DEFAULT.withHints(ReadOnceHint.INSTANCE), original)
        );
        assertFalse(
            "DEFAULT with a SEQUENTIAL hint",
            dir.isEligibleForFullFile(FAISS_FILE, IOContext.DEFAULT.withHints(DataAccessHint.SEQUENTIAL), original)
        );
        assertFalse(
            "merge context",
            dir.isEligibleForFullFile(FAISS_FILE, IOContext.merge(new MergeInfo(1, ONE_BLOCK_BYTES + 1, false, 1)), original)
        );
        assertFalse(
            "flush context",
            dir.isEligibleForFullFile(FAISS_FILE, IOContext.flush(new FlushInfo(1, ONE_BLOCK_BYTES + 1)), original)
        );
    }

    public void testEligibilitySizeBounds() {
        TieredDirectory dir = directoryWith(() -> realSettings("faiss"), null);

        assertFalse("exactly one block", dir.isEligibleForFullFile(FAISS_FILE, IOContext.DEFAULT, remoteOriginal(ONE_BLOCK_BYTES)));
        assertTrue("one byte over a block", dir.isEligibleForFullFile(FAISS_FILE, IOContext.DEFAULT, remoteOriginal(ONE_BLOCK_BYTES + 1)));
        assertTrue(
            "one byte under the maximum",
            dir.isEligibleForFullFile(FAISS_FILE, IOContext.DEFAULT, remoteOriginal(WarmFullFileManager.MAX_WHOLE_FILE_BYTES - 1))
        );
        assertFalse(
            "exactly the maximum",
            dir.isEligibleForFullFile(FAISS_FILE, IOContext.DEFAULT, remoteOriginal(WarmFullFileManager.MAX_WHOLE_FILE_BYTES))
        );
        assertFalse(
            "above the maximum",
            dir.isEligibleForFullFile(FAISS_FILE, IOContext.DEFAULT, remoteOriginal(WarmFullFileManager.MAX_WHOLE_FILE_BYTES + 1))
        );
    }

    public void testEligibilityDoesNotDependOnRemoteModeAtOpen() {
        // A handle opened while the file is still local (e.g. by the searcher at refresh time, before the upload) is
        // eligible; whether the file has switched to remote mode is checked by the wrapper when a clone or slice is
        // taken, so the handle becomes promotable after the upload without being reopened.
        TieredDirectory dir = directoryWith(() -> realSettings("faiss"), null);
        SwitchableIndexInput local = mock(SwitchableIndexInput.class);
        when(local.length()).thenReturn(ONE_BLOCK_BYTES + 1);
        when(local.hasSwitchedToRemote()).thenReturn(false);

        assertTrue("local (not yet uploaded) file", dir.isEligibleForFullFile(FAISS_FILE, IOContext.DEFAULT, local));
        assertTrue(
            "same file once in remote mode",
            dir.isEligibleForFullFile(FAISS_FILE, IOContext.DEFAULT, remoteOriginal(ONE_BLOCK_BYTES + 1))
        );
    }

    public void testOpenTimeEligibilityDoesNotNeedTheSetting() {
        // The open-time rules are structural; whether the feature is on for a file is the trigger's check against the
        // manager's current extensions (a null manager means the feature is off and nothing ever triggers).
        SwitchableIndexInput original = remoteOriginal(ONE_BLOCK_BYTES + 1);

        assertTrue(
            "real settings, default (empty)",
            directoryWith(() -> realSettings(), null).isEligibleForFullFile(FAISS_FILE, IOContext.DEFAULT, original)
        );
        assertTrue(
            "mock settings, empty",
            directoryWith(getMockWarmFullFileSettingsSupplier(), null).isEligibleForFullFile(FAISS_FILE, IOContext.DEFAULT, original)
        );
        assertTrue("supplier returns null", directoryWith(() -> null, null).isEligibleForFullFile(FAISS_FILE, IOContext.DEFAULT, original));
        assertTrue("no supplier", directoryWith(null, null).isEligibleForFullFile(FAISS_FILE, IOContext.DEFAULT, original));
    }

    public void testDynamicExtensionUpdateLeavesOpenTimeEligibilityUnchanged() {
        // Adding or removing the extension changes what the trigger does with a handle, never the open-time decision.
        ClusterSettings clusterSettings = newClusterSettings();
        WarmFullFileSettings settings = new WarmFullFileSettings(clusterSettings);
        TieredDirectory dir = directoryWith(() -> settings, null);
        SwitchableIndexInput original = remoteOriginal(9L * 1024 * 1024);

        assertTrue("before the update", dir.isEligibleForFullFile(FAISS_FILE, IOContext.DEFAULT, original));
        setExtensions(clusterSettings, "faiss");
        assertTrue("after the update", dir.isEligibleForFullFile(FAISS_FILE, IOContext.DEFAULT, original));
        clusterSettings.applySettings(Settings.EMPTY);
        assertTrue("after the extension is removed again", dir.isEligibleForFullFile(FAISS_FILE, IOContext.DEFAULT, original));
    }

    public void testDynamicExtensionUpdateNeverPromotesFileOfOneBlockOrLess() throws Exception {
        // The fixture file is well under one block, so even once its extension is configured no handle may promote:
        // a handle opened before the update and one opened after it both stay on blocks and submit nothing.
        populateData();
        ClusterSettings clusterSettings = newClusterSettings();
        WarmFullFileSettings settings = new WarmFullFileSettings(clusterSettings);
        RecordingExecutor executor = new RecordingExecutor();
        TieredDirectory dir = directoryWith(() -> settings, recordingManager(executor));
        long length = remoteSegmentStoreDirectory.fileLength(FILE_PRESENT_IN_REMOTE_ONLY);
        assertTrue("fixture file must fit in one block", length > 0 && length <= ONE_BLOCK_BYTES);

        IndexInput before = dir.openInput(FILE_PRESENT_IN_REMOTE_ONLY, IOContext.DEFAULT);
        setExtensions(clusterSettings, "si");
        assertTrue(settings.isExtensionEligible("si"));
        IndexInput after = dir.openInput(FILE_PRESENT_IN_REMOTE_ONLY, IOContext.DEFAULT);
        assertTrue(getSwitchableIndexInputFromWrapper(after).hasSwitchedToRemote());

        IndexInput beforeClone = before.clone();
        IndexInput afterClone = after.clone();
        IndexInput beforeSlice = before.slice("s", 0, Math.min(16, length));
        IndexInput afterSlice = after.slice("s", 0, Math.min(16, length));
        assertEquals("no promotion submitted by either handle", 0, executor.submitted.get());
        assertTrue(beforeClone instanceof SwitchableIndexInputWrapper);
        assertTrue(afterClone instanceof SwitchableIndexInputWrapper);
        assertTrue(beforeSlice instanceof SwitchableIndexInputWrapper);
        assertTrue(afterSlice instanceof SwitchableIndexInputWrapper);
        assertEquals(FullFileState.State.BLOCK, familyStateOf(FILE_PRESENT_IN_REMOTE_ONLY).state());

        beforeClone.close();
        afterClone.close();
        beforeSlice.close();
        afterSlice.close();
        before.close();
        after.close();
    }

    public void testFeatureOffNeverPromotesEvenWithManager() throws Exception {
        // Empty extension list with a live manager: opening, reading, cloning and slicing the fixture files must submit
        // no promotion, and every family stays BLOCK.
        populateData();
        RecordingExecutor executor = new RecordingExecutor();
        TieredDirectory dir = directoryWith(getMockWarmFullFileSettingsSupplier(), recordingManager(executor));

        for (String name : new String[] { FILE_PRESENT_LOCALLY, FILE_PRESENT_IN_REMOTE_ONLY }) {
            IndexInput handle = dir.openInput(name, IOContext.DEFAULT);
            assertTrue(handle instanceof SwitchableIndexInputWrapper);
            IndexInput clone = handle.clone();
            assertTrue("block clone for " + name, clone instanceof SwitchableIndexInputWrapper);
            long length = handle.length();
            if (length > 0) {
                IndexInput slice = handle.slice("s", 0, Math.min(16, length));
                assertTrue("block slice for " + name, slice instanceof SwitchableIndexInputWrapper);
                assertEquals(clone.readByte(), slice.readByte());
                slice.close();
            }
            clone.close();
            handle.close();
            assertEquals("family stays BLOCK for " + name, FullFileState.State.BLOCK, familyStateOf(name).state());
        }
        assertEquals("no promotion submitted", 0, executor.submitted.get());
    }

    public void testOpenInputServesTempAndCorruptedMarkerFilesFromLocalDirectory() throws IOException {
        // Neither file type gets a switchable entry or a wrapper: openInput hands them to the local directory before
        // any eligibility decision.
        String corrupted = "corrupted_" + randomAlphaOfLength(8);
        try (IndexOutput out = localDirectory.createOutput(corrupted, IOContext.DEFAULT)) {
            out.writeByte((byte) 42);
        }
        assertTrue(FileTypeUtils.isCorruptedFile(corrupted));

        IndexInput corruptedInput = tieredDirectory.openInput(corrupted, IOContext.DEFAULT);
        assertFalse(corruptedInput instanceof SwitchableIndexInputWrapper);
        assertEquals(localDirectory.openInput(corrupted, IOContext.DEFAULT).toString(), corruptedInput.toString());
        assertEquals(1, corruptedInput.length());
        assertEquals((byte) 42, corruptedInput.readByte());
        assertNull("no switchable entry for a corrupted marker", fileCache.get(getFilePathSwitchable(localDirectory, corrupted)));
        corruptedInput.close();

        IndexInput tempInput = tieredDirectory.openInput(TEMP_FILE, IOContext.DEFAULT);
        assertFalse(tempInput instanceof SwitchableIndexInputWrapper);
        assertEquals(localDirectory.openInput(TEMP_FILE, IOContext.DEFAULT).toString(), tempInput.toString());
        tempInput.close();
    }

    private void removeExtraFSFiles() throws IOException {
        HashSet<String> allFiles = new HashSet<>(Arrays.asList(localDirectory.listAll()));
        allFiles.stream().filter(FileTypeUtils::isExtraFSFile).forEach(file -> {
            try {
                localDirectory.deleteFile(file);
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        });
    }

    public void testTieredDirectoryFactory() throws IOException {
        TieredDirectoryFactory factory = new TieredDirectoryFactory(
            getMockPrefetchSettingsSupplier(),
            getMockWarmFullFileSettingsSupplier(),
            null // manager not needed: feature off
        );
        FSDirectory factoryLocalDir = FSDirectory.open(createTempDir());
        IndexStorePlugin.DirectoryFactory localDirFactory = mock(IndexStorePlugin.DirectoryFactory.class);
        when(localDirFactory.newDirectory(any(), any())).thenReturn(factoryLocalDir);
        FileCache factoryFileCache = FileCacheFactory.createConcurrentLRUFileCache(FILE_CACHE_CAPACITY, 1);

        Directory result = factory.newDirectory(null, null, localDirFactory, remoteSegmentStoreDirectory, factoryFileCache, threadPool);
        assertTrue(result instanceof TieredDirectory);
        result.close();
    }

    private void addFilesToDirectory(String[] files) throws IOException {
        for (String file : files) {
            IndexOutput indexOutput = tieredDirectory.createOutput(file, IOContext.DEFAULT);
            indexOutput.close();
        }
    }

    private SwitchableIndexInput getSwitchableIndexInputFromWrapper(IndexInput indexInput) {
        assertTrue(indexInput instanceof SwitchableIndexInputWrapper);
        return (SwitchableIndexInput) (((FilterIndexInput) indexInput).getDelegate());
    }

    private void decRefToZero(Path path) {
        int refCount = fileCache.getRef(path);
        for (int i = 0; i < refCount; i++) {
            fileCache.decRef(path);
        }
    }

    private void createUnclosedClones(IndexInput indexInput, Path path) {
        IndexInput clone = indexInput.clone();
        IndexInput cloneOfClone = clone.clone();
        assertEquals(2, (int) fileCache.getRef(path));
    }

    private void triggerGarbageCollectionAndAssertClonesClosed(Path path) {
        try {
            assertBusy(() -> {
                System.gc();
                assertEquals("Expected refCount to drop to original count", (int) fileCache.getRef(path), 0);
            }, 5, TimeUnit.SECONDS);
        } catch (Exception e) {
            logger.error("Exception thrown while triggering gc", e);
            fail();
        }
    }

    private boolean existsInLocalDirectory(String name) throws IOException {
        return Arrays.asList(localDirectory.listAll()).contains(name);
    }

    private boolean existsInRemoteDirectory(String name) throws IOException {
        return Arrays.asList(remoteSegmentStoreDirectory.listAll()).contains(name);
    }

    private boolean existsInTieredDirectory(String name) throws IOException {
        return Arrays.asList(tieredDirectory.listAll()).contains(name);
    }

    private boolean existsInFileCache(Path path) {
        return (fileCache.get(path) != null);
    }
}

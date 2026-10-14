/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.storage.directory;

import com.carrotsearch.randomizedtesting.annotations.ThreadLeakFilters;

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
import org.junit.Before;

import java.io.IOException;
import java.nio.file.NoSuchFileException;
import java.nio.file.Path;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.Executor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
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

    // Feature OFF (empty extensions): these existing tests exercise the inert path, so nothing is ever promoted.
    private Supplier<WarmFullFileSettings> getMockWarmFullFileSettingsSupplier() {
        return () -> {
            WarmFullFileSettings settings = mock(WarmFullFileSettings.class);
            when(settings.getExtensions()).thenReturn(Set.of());
            when(settings.isExtensionEligible(org.mockito.ArgumentMatchers.anyString())).thenReturn(false);
            return settings;
        };
    }

    public void testCacheFilePutOverLiveEntryClosesTheReplacedFamily() throws Exception {
        // RemovalReason.REPLACED never calls close(), so two openInput calls racing to create a family would leak the
        // first family's mapping/reference/budget. cacheFile must close the entry it replaced.
        java.nio.file.Path key = org.opensearch.storage.utils.DirectoryUtils.getFilePathSwitchable(
            localDirectory,
            FILE_PRESENT_IN_REMOTE_ONLY
        );
        tieredDirectory.cacheFile(FILE_PRESENT_IN_REMOTE_ONLY, true);
        org.opensearch.index.store.remote.filecache.CachedIndexInput first = fileCache.get(key);
        fileCache.decRef(key);
        assertNotNull(first);
        assertFalse(first.isClosed());

        tieredDirectory.cacheFile(FILE_PRESENT_IN_REMOTE_ONLY, true);
        org.opensearch.index.store.remote.filecache.CachedIndexInput second = fileCache.get(key);
        fileCache.decRef(key);

        assertNotSame(first, second);
        assertTrue("replaced entry closed", first.isClosed());
        assertEquals(org.opensearch.storage.warmfullfile.FullFileState.State.CLOSED, first.fullFileState().state());
        assertFalse("new entry live", second.isClosed());
        assertEquals(org.opensearch.storage.warmfullfile.FullFileState.State.BLOCK, second.fullFileState().state());
    }

    public void testHandleOfReplacedFamilyKeepsReadingBlocksAndNeverPromotes() throws Exception {
        // The losing side of the two-thread openInput race: a handle returned just before a racing cacheFile replaced
        // the family keeps reading blocks correctly, but it belongs to the CLOSED old family, so it never triggers a
        // promotion and never gets a whole-file view. Only handles opened afterwards belong to the live family. (All
        // reads stay inside block 0, which the fixture serves once.)
        populateData();
        Path key = getFilePathSwitchable(localDirectory, FILE_PRESENT_IN_REMOTE_ONLY);

        IndexInput first = tieredDirectory.openInput(FILE_PRESENT_IN_REMOTE_ONLY, IOContext.DEFAULT);
        org.opensearch.index.store.remote.filecache.CachedIndexInput oldEntry = fileCache.get(key);
        fileCache.decRef(key);
        org.opensearch.storage.warmfullfile.FullFileState oldState = oldEntry.fullFileState();
        assertTrue("fixture file must be readable", first.length() > 0);
        byte before = first.readByte();

        tieredDirectory.cacheFile(FILE_PRESENT_IN_REMOTE_ONLY, true); // the race: a second put over the live family
        org.opensearch.index.store.remote.filecache.CachedIndexInput newEntry = fileCache.get(key);
        fileCache.decRef(key);
        assertNotSame(oldEntry, newEntry);
        assertTrue("replaced entry closed", oldEntry.isClosed());
        assertEquals(org.opensearch.storage.warmfullfile.FullFileState.State.CLOSED, oldState.state());
        assertEquals(org.opensearch.storage.warmfullfile.FullFileState.State.BLOCK, newEntry.fullFileState().state());

        // The old handle still reads blocks, and agrees with a handle from the live family.
        first.seek(0);
        assertEquals(before, first.readByte());
        IndexInput fresh = tieredDirectory.openInput(FILE_PRESENT_IN_REMOTE_ONLY, IOContext.DEFAULT);
        assertEquals(before, fresh.readByte());

        // But it never promotes: its clone is a block wrapper, reads the same bytes, and the old family stays CLOSED.
        first.seek(0);
        IndexInput clone = first.clone();
        assertTrue("block clone, not a whole-file view", clone instanceof SwitchableIndexInputWrapper);
        assertEquals(before, clone.readByte());
        assertEquals(org.opensearch.storage.warmfullfile.FullFileState.State.CLOSED, oldState.state());

        clone.close();
        fresh.close();
        first.close();
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

    private static final long ONE_BLOCK_BYTES = 8L * 1024 * 1024;
    private static final String FAISS_FILE = "_0_NativeEngines990KnnVectorsFormat_0.faiss";

    /** Records promote() submissions without running them, so trigger behaviour is isolated from downloads. */
    private static final class RecordingExecutor implements Executor {
        final AtomicInteger submitted = new AtomicInteger();

        @Override
        public void execute(Runnable command) {
            submitted.incrementAndGet();
        }
    }

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
        return new WarmFullFileManager(fileCache, executor, () -> 0L, 8);
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

    public void testEligibilityRequiresConfiguredExtension() {
        TieredDirectory dir = directoryWith(() -> realSettings(".FAISS", "veq"), null);
        SwitchableIndexInput original = remoteOriginal(ONE_BLOCK_BYTES + 1);

        assertTrue(dir.isEligibleForFullFile(FAISS_FILE, IOContext.DEFAULT, original));
        assertTrue("extension match ignores case", dir.isEligibleForFullFile("_0.FAISS", IOContext.DEFAULT, original));
        assertTrue(dir.isEligibleForFullFile("_0_Lucene99_0.veq", IOContext.DEFAULT, original));
        assertFalse("extension not configured", dir.isEligibleForFullFile("_0_Lucene99_0.vec", IOContext.DEFAULT, original));
        assertFalse("only the last extension counts", dir.isEligibleForFullFile("_0.faiss.vec", IOContext.DEFAULT, original));
        assertFalse("no extension", dir.isEligibleForFullFile("faiss", IOContext.DEFAULT, original));
        assertFalse("trailing dot", dir.isEligibleForFullFile("_0.faiss.", IOContext.DEFAULT, original));
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

    public void testEligibilityRequiresRemoteMode() {
        TieredDirectory dir = directoryWith(() -> realSettings("faiss"), null);
        SwitchableIndexInput local = mock(SwitchableIndexInput.class);
        when(local.length()).thenReturn(ONE_BLOCK_BYTES + 1);
        when(local.hasSwitchedToRemote()).thenReturn(false);

        assertFalse("local (not yet uploaded) file", dir.isEligibleForFullFile(FAISS_FILE, IOContext.DEFAULT, local));
        assertTrue(
            "same file once in remote mode",
            dir.isEligibleForFullFile(FAISS_FILE, IOContext.DEFAULT, remoteOriginal(ONE_BLOCK_BYTES + 1))
        );
    }

    public void testEligibilityOffWithoutConfiguredExtensions() {
        SwitchableIndexInput original = remoteOriginal(ONE_BLOCK_BYTES + 1);

        assertFalse(
            "real settings, default (empty)",
            directoryWith(() -> realSettings(), null).isEligibleForFullFile(FAISS_FILE, IOContext.DEFAULT, original)
        );
        assertFalse(
            "mock settings, empty",
            directoryWith(getMockWarmFullFileSettingsSupplier(), null).isEligibleForFullFile(FAISS_FILE, IOContext.DEFAULT, original)
        );
        assertFalse(
            "supplier returns null",
            directoryWith(() -> null, null).isEligibleForFullFile(FAISS_FILE, IOContext.DEFAULT, original)
        );
        assertFalse("no supplier", directoryWith(null, null).isEligibleForFullFile(FAISS_FILE, IOContext.DEFAULT, original));
    }

    public void testDynamicExtensionUpdateChangesEligibilityDecidedAfterwards() {
        ClusterSettings clusterSettings = newClusterSettings();
        WarmFullFileSettings settings = new WarmFullFileSettings(clusterSettings);
        TieredDirectory dir = directoryWith(() -> settings, null);
        SwitchableIndexInput original = remoteOriginal(9L * 1024 * 1024);

        assertFalse("before the update", dir.isEligibleForFullFile(FAISS_FILE, IOContext.DEFAULT, original));
        setExtensions(clusterSettings, "faiss");
        assertTrue("after the update", dir.isEligibleForFullFile(FAISS_FILE, IOContext.DEFAULT, original));
        clusterSettings.applySettings(Settings.EMPTY);
        assertFalse("after the extension is removed again", dir.isEligibleForFullFile(FAISS_FILE, IOContext.DEFAULT, original));
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

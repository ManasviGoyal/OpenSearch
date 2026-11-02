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
import org.opensearch.common.util.io.IOUtils;
import org.opensearch.index.store.remote.file.CleanerDaemonThreadLeakFilter;
import org.opensearch.index.store.remote.filecache.CachedIndexInput;
import org.opensearch.index.store.remote.filecache.FileCache;
import org.opensearch.index.store.remote.filecache.FileCacheFactory;
import org.opensearch.index.store.remote.utils.FileTypeUtils;
import org.opensearch.plugins.IndexStorePlugin;
import org.opensearch.storage.indexinput.CachedSwitchableIndexInput;
import org.opensearch.storage.indexinput.PromotableCachedSwitchableIndexInput;
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
        // The fixture files' extensions are listed, so their entries carry a family; the files fit in one block, so no
        // handle is eligible and nothing promotes.
        WarmFullFileManager manager = recordingManager(new RecordingExecutor());
        manager.onExtensionsChanged(Set.of("cfe", "si"));
        tieredDirectory = new TieredDirectory(
            localDirectory,
            remoteSegmentStoreDirectory,
            fileCache,
            threadPool,
            getMockPrefetchSettingsSupplier(),
            manager
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

    // The fixture files fit in one block, so no handle is eligible and the fixture directory never promotes anything.
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
        assertEquals(FullFileState.State.BLOCK, familyOf(first).state());
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
        assertEquals(FullFileState.State.BLOCK, familyOf(live).state());
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
        assertEquals(FullFileState.State.BLOCK, familyOf(family).state());
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
            assertFalse("local-mode family not yet promotable", familyOf(entry).isRemoteMode());
        } finally {
            fileCache.decRef(localKey);
        }

        tieredDirectory.afterSyncToRemote(localFile);

        entry = fileCache.get(localKey);
        try {
            assertTrue("marked remote once the switch completed", familyOf(entry).isRemoteMode());
        } finally {
            fileCache.decRef(localKey);
        }

        IndexInput remoteHandle = tieredDirectory.openInput(FILE_PRESENT_IN_REMOTE_ONLY, IOContext.DEFAULT);
        Path remoteKey = getFilePathSwitchable(localDirectory, FILE_PRESENT_IN_REMOTE_ONLY);
        entry = fileCache.get(remoteKey);
        try {
            assertTrue("family created from remote is promotable at once", familyOf(entry).isRemoteMode());
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
            assertFalse("family stays non-promotable while serving the local copy", familyOf(entry).isRemoteMode());
        } finally {
            fileCache.decRef(localKey);
        }
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

    /** A second TieredDirectory over the same local directory, remote directory and file cache as the fixture's. */
    private TieredDirectory directoryWith(WarmFullFileManager manager) {
        return new TieredDirectory(
            localDirectory,
            remoteSegmentStoreDirectory,
            fileCache,
            threadPool,
            getMockPrefetchSettingsSupplier(),
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

    /** The family of a switchable entry, or null for the existing entry, which carries none. */
    private static FullFileState familyOf(CachedIndexInput entry) {
        return entry instanceof PromotableCachedSwitchableIndexInput promotable ? promotable.fullFileState() : null;
    }

    private FullFileState familyStateOf(String name) {
        Path key = getFilePathSwitchable(localDirectory, name);
        CachedIndexInput entry = fileCache.get(key);
        assertNotNull("switchable entry for " + name, entry);
        fileCache.decRef(key);
        return familyOf(entry);
    }

    private Class<?> entryClassOf(String name) {
        Path key = getFilePathSwitchable(localDirectory, name);
        CachedIndexInput entry = fileCache.get(key);
        assertNotNull("switchable entry for " + name, entry);
        fileCache.decRef(key);
        return entry.getClass();
    }

    public void testEligibilityIgnoresTheExtensionAtOpen() {
        // The extension is not an open-time rule: it selects the entry type when the family is created, and the trigger
        // checks it against the manager so a handle opened while it was listed stops when it is removed. Any extension,
        // or none, passes the open-time rules.
        TieredDirectory dir = directoryWith(null);
        SwitchableIndexInput original = remoteOriginal(ONE_BLOCK_BYTES + 1);

        assertTrue(dir.isEligibleForFullFile(FAISS_FILE, IOContext.DEFAULT, original));
        assertTrue(dir.isEligibleForFullFile("_0_Lucene99_0.veq", IOContext.DEFAULT, original));
        assertTrue(dir.isEligibleForFullFile("_0_Lucene99_0.vec", IOContext.DEFAULT, original));
        assertTrue(
            "a file without an extension passes the open-time rules",
            dir.isEligibleForFullFile("faiss", IOContext.DEFAULT, original)
        );
    }

    public void testEligibilityExcludesLuceneSpecialFiles() {
        TieredDirectory dir = directoryWith(null);
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
        TieredDirectory dir = directoryWith(null);
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
        TieredDirectory dir = directoryWith(null);

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
        TieredDirectory dir = directoryWith(null);
        SwitchableIndexInput local = mock(SwitchableIndexInput.class);
        when(local.length()).thenReturn(ONE_BLOCK_BYTES + 1);
        when(local.hasSwitchedToRemote()).thenReturn(false);

        assertTrue("local (not yet uploaded) file", dir.isEligibleForFullFile(FAISS_FILE, IOContext.DEFAULT, local));
        assertTrue(
            "same file once in remote mode",
            dir.isEligibleForFullFile(FAISS_FILE, IOContext.DEFAULT, remoteOriginal(ONE_BLOCK_BYTES + 1))
        );
    }

    public void testExtensionUpdateAppliesToFamiliesCreatedAfterwards() throws Exception {
        // The entry type is chosen when the family is created: a file whose extension is not listed gets the existing
        // entry, and listing the extension later leaves a live entry as it is. A family created after the update is
        // promotable, but the fixture file fits in one block, so its handles still stay on blocks and submit nothing.
        populateData();
        ClusterSettings clusterSettings = newClusterSettings();
        WarmFullFileSettings settings = new WarmFullFileSettings(clusterSettings);
        RecordingExecutor executor = new RecordingExecutor();
        WarmFullFileManager manager = recordingManager(executor);
        settings.addListener(manager::onExtensionsChanged);
        TieredDirectory dir = directoryWith(manager);
        long length = remoteSegmentStoreDirectory.fileLength(FILE_PRESENT_IN_REMOTE_ONLY);
        assertTrue("fixture file must fit in one block", length > 0 && length <= ONE_BLOCK_BYTES);

        IndexInput before = dir.openInput(FILE_PRESENT_IN_REMOTE_ONLY, IOContext.DEFAULT);
        assertEquals(CachedSwitchableIndexInput.class, entryClassOf(FILE_PRESENT_IN_REMOTE_ONLY));
        setExtensions(clusterSettings, "si");
        assertTrue(manager.isExtensionEligible("si"));
        IndexInput after = dir.openInput(FILE_PRESENT_IN_REMOTE_ONLY, IOContext.DEFAULT);
        assertEquals("the live entry is kept", CachedSwitchableIndexInput.class, entryClassOf(FILE_PRESENT_IN_REMOTE_ONLY));
        assertNull("the existing entry carries no family", familyStateOf(FILE_PRESENT_IN_REMOTE_ONLY));
        assertEquals(SwitchableIndexInputWrapper.class, before.getClass());
        assertEquals(SwitchableIndexInputWrapper.class, after.getClass());
        IOUtils.close(before, after);

        // Dropping the entry, as a shard close does, and opening again creates a promotable family.
        fileCache.remove(getFilePathSwitchable(localDirectory, FILE_PRESENT_IN_REMOTE_ONLY));
        IndexInput recreated = dir.openInput(FILE_PRESENT_IN_REMOTE_ONLY, IOContext.DEFAULT);
        assertEquals(PromotableCachedSwitchableIndexInput.class, entryClassOf(FILE_PRESENT_IN_REMOTE_ONLY));
        assertTrue(getSwitchableIndexInputFromWrapper(recreated).hasSwitchedToRemote());
        assertEquals(FullFileState.State.BLOCK, familyStateOf(FILE_PRESENT_IN_REMOTE_ONLY).state());
        assertEquals("one block or less: the existing wrapper", SwitchableIndexInputWrapper.class, recreated.getClass());
        IndexInput clone = recreated.clone();
        IndexInput slice = recreated.slice("s", 0, Math.min(16, length));
        assertEquals(SwitchableIndexInputWrapper.class, clone.getClass());
        assertEquals(SwitchableIndexInputWrapper.class, slice.getClass());
        assertEquals("no promotion submitted by any handle", 0, executor.submitted.get());
        assertEquals(FullFileState.State.BLOCK, familyStateOf(FILE_PRESENT_IN_REMOTE_ONLY).state());
        IOUtils.close(slice, clone, recreated);
    }

    public void testFeatureOffNeverPromotesEvenWithManager() throws Exception {
        // A live manager with no listed extensions: every file this directory creates gets the existing entry, which
        // carries no family, and the existing wrapper, whose clones and slices are existing wrappers too; nothing is
        // submitted.
        populateData();
        RecordingExecutor executor = new RecordingExecutor();
        TieredDirectory dir = directoryWith(recordingManager(executor));
        String localFile = "_3.cfe";
        dir.createOutput(localFile, IOContext.DEFAULT).close(); // written through this directory, not the fixture's

        assertExistingEntriesAndHandles(dir, localFile, FILE_PRESENT_IN_REMOTE_ONLY);
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

    public void testIneligibleHandleIsAPlainSwitchableWrapper() throws Exception {
        // An ineligible open (merge or flush context, read-once hint, a file of one block or less) gets the plain
        // wrapper, whose clones and slices are plain too; nothing is requested. The eligibility rules themselves are
        // covered by the tests above; this one checks the routing.
        populateData();
        RecordingExecutor executor = new RecordingExecutor();
        WarmFullFileManager manager = recordingManager(executor);
        manager.onExtensionsChanged(Set.of("si"));
        TieredDirectory dir = directoryWith(manager);
        assertFalse(
            "fixture file fits in one block, so even a DEFAULT open is ineligible",
            dir.isEligibleForFullFile(FILE_PRESENT_IN_REMOTE_ONLY, IOContext.DEFAULT, remoteOriginal(ONE_BLOCK_BYTES))
        );

        for (IOContext context : List.of(
            IOContext.DEFAULT,
            IOContext.READONCE,
            IOContext.merge(new MergeInfo(1, ONE_BLOCK_BYTES + 1, false, 1)),
            IOContext.flush(new FlushInfo(1, ONE_BLOCK_BYTES + 1))
        )) {
            IndexInput handle = dir.openInput(FILE_PRESENT_IN_REMOTE_ONLY, context);
            assertEquals(context.toString(), SwitchableIndexInputWrapper.class, handle.getClass());
            IndexInput clone = handle.clone();
            assertEquals(SwitchableIndexInputWrapper.class, clone.getClass());
            IndexInput slice = handle.slice("s", 0, Math.min(16, handle.length()));
            assertEquals(SwitchableIndexInputWrapper.class, slice.getClass());
            IOUtils.close(slice, clone, handle);
        }
        assertEquals("no promotion requested", 0, executor.submitted.get());
        assertNotNull("the entry still carries its family", familyStateOf(FILE_PRESENT_IN_REMOTE_ONLY));
    }

    public void testWithoutManagerOpensPlainSwitchableInputs() throws Exception {
        // No manager means nothing can promote: the directory creates the existing entry, which carries no family, and
        // hands out the existing wrapper, whose clones and slices are existing wrappers too.
        populateData();
        TieredDirectory dir = directoryWith(null);
        String localFile = "_3.cfe";
        dir.createOutput(localFile, IOContext.DEFAULT).close(); // written through this directory, not the fixture's

        assertExistingEntriesAndHandles(dir, localFile, FILE_PRESENT_IN_REMOTE_ONLY);
    }

    /** Opens each file through {@code dir} and checks that its entry, handle, clone and slice are the existing classes. */
    private void assertExistingEntriesAndHandles(TieredDirectory dir, String... names) throws IOException {
        for (String name : names) {
            IndexInput handle = dir.openInput(name, IOContext.DEFAULT);
            assertEquals(name, SwitchableIndexInputWrapper.class, handle.getClass());
            IndexInput clone = handle.clone();
            assertEquals(SwitchableIndexInputWrapper.class, clone.getClass());
            long length = handle.length();
            if (length > 0) {
                IndexInput slice = handle.slice("s", 0, Math.min(16, length));
                assertEquals(SwitchableIndexInputWrapper.class, slice.getClass());
                assertEquals(clone.readByte(), slice.readByte());
                slice.close();
            }
            clone.close();
            handle.close();
            assertEquals(CachedSwitchableIndexInput.class, entryClassOf(name));
            assertNull("the existing entry carries no family", familyStateOf(name));
        }
    }

    public void testTieredDirectoryFactory() throws IOException {
        TieredDirectoryFactory factory = new TieredDirectoryFactory(getMockPrefetchSettingsSupplier(), null /* feature off */);
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

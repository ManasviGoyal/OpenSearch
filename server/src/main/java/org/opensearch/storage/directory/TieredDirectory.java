/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.storage.directory;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.lucene.index.IndexFileNames;
import org.apache.lucene.store.DataAccessHint;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.IOContext;
import org.apache.lucene.store.IndexInput;
import org.apache.lucene.store.ReadOnceHint;
import org.opensearch.common.annotation.ExperimentalApi;
import org.opensearch.index.store.CompositeDirectory;
import org.opensearch.index.store.RemoteSegmentStoreDirectory;
import org.opensearch.index.store.remote.file.AbstractBlockIndexInput;
import org.opensearch.index.store.remote.filecache.CachedIndexInput;
import org.opensearch.index.store.remote.filecache.FileCache;
import org.opensearch.index.store.remote.utils.FileTypeUtils;
import org.opensearch.storage.indexinput.CachedSwitchableIndexInput;
import org.opensearch.storage.indexinput.SwitchableIndexInput;
import org.opensearch.storage.indexinput.SwitchableIndexInputWrapper;
import org.opensearch.storage.prefetch.TieredStoragePrefetchSettings;
import org.opensearch.storage.utils.DirectoryUtils;
import org.opensearch.storage.warmfullfile.FullFileState;
import org.opensearch.storage.warmfullfile.WarmFullFileManager;
import org.opensearch.storage.warmfullfile.WarmFullFileSettings;
import org.opensearch.threadpool.ThreadPool;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.file.NoSuchFileException;
import java.nio.file.Path;
import java.util.Arrays;
import java.util.Collection;
import java.util.function.Supplier;
import java.util.stream.Stream;

import static org.opensearch.index.store.remote.utils.FileTypeUtils.BLOCK_FILE_IDENTIFIER;
import static org.opensearch.storage.utils.DirectoryUtils.getFilePathSwitchable;
import static org.apache.lucene.index.IndexFileNames.SEGMENTS;

/**
 * Extension of Composite directory to support writable warm and other related features
 *
 * @opensearch.experimental
 */
@ExperimentalApi
public class TieredDirectory extends CompositeDirectory {

    private static final Logger logger = LogManager.getLogger(TieredDirectory.class);

    // A file at or below one block is already served as a single block; a whole-file copy gains nothing.
    static final long ONE_BLOCK_BYTES = AbstractBlockIndexInput.Builder.DEFAULT_BLOCK_SIZE;

    private final Supplier<TieredStoragePrefetchSettings> tieredStoragePrefetchSettingsSupplier;
    private final Supplier<WarmFullFileSettings> warmFullFileSettingsSupplier;
    private final WarmFullFileManager warmFullFileManager;

    public TieredDirectory(
        Directory localDirectory,
        Directory remoteDirectory,
        FileCache fileCache,
        ThreadPool threadPool,
        Supplier<TieredStoragePrefetchSettings> tieredStoragePrefetchSettingsSupplier,
        Supplier<WarmFullFileSettings> warmFullFileSettingsSupplier,
        WarmFullFileManager warmFullFileManager
    ) {
        super(localDirectory, remoteDirectory, fileCache, threadPool);
        this.tieredStoragePrefetchSettingsSupplier = tieredStoragePrefetchSettingsSupplier;
        this.warmFullFileSettingsSupplier = warmFullFileSettingsSupplier;
        this.warmFullFileManager = warmFullFileManager;
    }

    @Override
    public String[] listAll() throws IOException {
        ensureOpen();
        String[] localFiles = localDirectory.listAll();
        String[] remoteFiles;

        // Check if local directory has any segments_n files
        boolean hasLocalSegments = Arrays.stream(localFiles).anyMatch(fileName -> fileName.startsWith(IndexFileNames.SEGMENTS));

        try {
            if (hasLocalSegments) {
                // If local has segments_n, filter out segments_n from remote
                remoteFiles = Arrays.stream(remoteDirectory.listAll())
                    .filter(fileName -> !fileName.startsWith(IndexFileNames.SEGMENTS))
                    .toArray(String[]::new);
            } else {
                // If local doesn't have segments_n, include all remote files
                remoteFiles = remoteDirectory.listAll();
            }
        } catch (NullPointerException e) {
            remoteFiles = new String[] {};
        }

        return Stream.concat(Arrays.stream(localFiles), Arrays.stream(remoteFiles))
            .map(s -> s.contains(BLOCK_FILE_IDENTIFIER) ? s.substring(0, s.indexOf(BLOCK_FILE_IDENTIFIER)) : s)
            .distinct()
            .sorted()
            .toArray(String[]::new);
    }

    @Override
    public void deleteFile(String name) throws IOException {
        super.deleteFile(name);
        // If entry doesn't exist, remove will be a NoOp
        fileCache.remove(getFilePathSwitchable(localDirectory, name));
    }

    @Override
    public void rename(String source, String dest) throws IOException {
        super.rename(source, dest);
        // remove switchable entry of source
        fileCache.remove(getFilePathSwitchable(localDirectory, source));
        // remove block entries of source (along with the full file) if any from file cache
        for (String file : listBlockFiles(source)) {
            fileCache.remove(getFilePath(file));
        }
    }

    /**
     * Whenever we write a file to the directory we add a switchable entry in the FileCache (logic in cacheFile method below)
     *
     * But for replicas where we download the file from remote, this entry would be missing hence we first check if file is present in remote or not
     * and then depending upon if it is present, we add the switchable entry to the FileCache.
     *
     * For reading any file (that is not .tmp) we rely on this switchable entry in the FileCache
     *
     * Detailed step wise flow below:
     *
     * 1. Check if file is .tmp file - read it from the local directory
     * 2. Check if the file cache contains switchable entry of the file (we create an entry when writing the file to the directory) - clone the entry and read it
     * 3. If 2 is false, then check if file exists in remote or not - if not throw NoSuchFileException since file doesn't exist in directory
     * 4. If file is present in remote, then add a corresponding switchable entry in the file cache. Now once file cache has the entry we can follow step 2
     *
     */
    @Override
    public IndexInput openInput(String name, IOContext context) throws IOException {
        ensureOpen();
        // We aren't tracking temporary files (created via createTempOutput) currently in FileCache as these are created and then deleted
        // within a very short span of time
        // We will be reading them directory from the local directory
        if (FileTypeUtils.isTempFile(name) || FileTypeUtils.isCorruptedFile(name)) {
            return localDirectory.openInput(name, context);
        }
        Path key = getFilePathSwitchable(localDirectory, name);
        CachedIndexInput indexInput = fileCache.get(key);
        if (indexInput == null) {
            RemoteSegmentStoreDirectory.UploadedSegmentMetadata uploadedSegmentMetadata = remoteDirectory.getSegmentsUploadedToRemoteStore()
                .get(name);
            if (uploadedSegmentMetadata == null) {
                throw new NoSuchFileException("File " + name + " not found in directory");
            }
            // For pre-created files already in remote, switchable entry would not be cached already locally, we need to download from
            // remote and cache it in FileCache. cacheFile creates the entry only if none is live, so concurrent opens of
            // the same file share one family. The reference cacheFile leaves is released in the finally block below; for
            // a locally written file afterSyncToRemote releases it instead.
            indexInput = cacheFile(name, true);
        }
        try {
            SwitchableIndexInput original = (SwitchableIndexInput) indexInput.getIndexInput();
            // Decide eligibility once per handle, here, because only openInput sees the IOContext.
            boolean eligible = isEligibleForFullFile(name, context, original);
            FullFileState fullFileState = indexInput.fullFileState();
            return new SwitchableIndexInputWrapper(
                "SwitchableIndexInput (path=" + getFilePath(name) + ")",
                (SwitchableIndexInput) original.clone(),
                fullFileState,
                eligible,
                warmFullFileManager
            );
        } finally {
            fileCache.decRef(key);
        }
    }

    /**
     * Decides, once per opened handle, whether this handle may trigger whole-file promotion. All conditions must hold:
     * the file is larger than one block and below the mmap chunk size; the context is a plain DEFAULT read (excludes
     * merges and FLUSH); and it carries no ReadOnce/SEQUENTIAL hint (excludes recovery checksum reads). Temp/corrupted
     * files never reach here (handled earlier in openInput).
     *
     * <p>Neither the extension setting nor remote (block) mode is part of this decision: the searcher opens its
     * handles at refresh time and never reopens them for an unchanged segment, so a handle must be able to become
     * promotable later, when the segment is uploaded or the extension is added to the setting, and must stop being
     * promotable when the extension is removed. The wrapper checks both at trigger time instead; see
     * {@link FullFileState#markRemoteMode(String)} and {@link WarmFullFileManager#isExtensionEligible(String)}.
     */
    // visible for testing
    boolean isEligibleForFullFile(String name, IOContext context, SwitchableIndexInput original) {
        String rejection = ineligibleReason(name, context, original);
        if (rejection == null) {
            return true;
        }
        // A rejection is worth a line only for an extension the setting lists, when debugging a node that never
        // promotes. This runs once per openInput, never on a read.
        WarmFullFileSettings settings = warmFullFileSettingsSupplier == null ? null : warmFullFileSettingsSupplier.get();
        if (settings != null && settings.isExtensionEligible(WarmFullFileSettings.extensionOf(name))) {
            logger.debug("whole-file promotion: [{}] opened with {} is not eligible: {}", name, context, rejection);
        }
        return false;
    }

    /** The rule that rejects a file at open, or null when its handles may trigger a promotion. */
    private static String ineligibleReason(String name, IOContext context, SwitchableIndexInput original) {
        // Not an ordinary Lucene special file.
        if (FileTypeUtils.isSegmentsFile(name) || FileTypeUtils.isLockFile(name) || name.endsWith(".cfs")) {
            return "segments, lock or compound file";
        }
        if (original.length() <= ONE_BLOCK_BYTES) { // original input: offset 0, length == whole file length
            return "length " + original.length() + " is at most one block";
        }
        // The mapping must be a single mmap chunk (Lucene's default chunk is 16 GiB). Above that,
        // closing a view could unmap the master and segmentSliceOrNull returns null (no k-NN SIMD), so do not promote.
        if (original.length() >= WarmFullFileManager.MAX_WHOLE_FILE_BYTES) {
            return "length " + original.length() + " is not below the mmap chunk size";
        }
        // Only plain search reads: DEFAULT context (excludes MERGE/FLUSH) with no ReadOnce/SEQUENTIAL hint
        // (excludes recovery checksum reads via IOContext.READONCE).
        if (context.context() != IOContext.Context.DEFAULT) {
            return "context is " + context.context();
        }
        if (context.hints().contains(ReadOnceHint.INSTANCE) || context.hints().contains(DataAccessHint.SEQUENTIAL)) {
            return "read-once or sequential hint";
        }
        return null;
    }

    @Override
    public void afterSyncToRemote(String file) {
        super.afterSyncToRemote(file);

        // once file is uploaded to remote, we can decrement the ref count of the switchable entry in file cache
        fileCache.decRef(getFilePathSwitchable(localDirectory, file));

        // perform the full file to block switch for the switchable index input (currently called here for testing)
        switchFileToRemote(file);

        // Whole-file promotion may start only now that the local copy is gone; see FullFileState.markRemoteMode.
        markFamilyRemote(file);
    }

    /**
     * Flags the family as remote-mode only if its original input has actually switched to remote. When the switch
     * failed (or the entry is gone), the family keeps serving the local copy and must stay non-promotable, since
     * whole-file promotion removes the family's local cache entry.
     */
    private void markFamilyRemote(String file) {
        withSwitchableEntry(file, (entry, input) -> {
            FullFileState fullFileState = entry.fullFileState();
            if (fullFileState != null && input.hasSwitchedToRemote()) {
                fullFileState.markRemoteMode(FullFileState.uploadedChecksum(remoteDirectory, file));
            }
        }, "Failed to confirm the switch to remote; the family stays on blocks");
    }

    /** An action on the switchable input behind a family's cache entry. */
    @FunctionalInterface
    private interface SwitchableEntryAction {
        void accept(CachedIndexInput entry, SwitchableIndexInput input) throws IOException;
    }

    /**
     * Runs {@code action} on the switchable entry of {@code file}, holding a reference on the entry for the duration and
     * releasing it afterwards. Does nothing when the entry is gone. A failure is logged with {@code failureMessage}.
     */
    private void withSwitchableEntry(String file, SwitchableEntryAction action, String failureMessage) {
        Path key = getFilePathSwitchable(localDirectory, file);
        CachedIndexInput entry = fileCache.get(key);
        if (entry == null) {
            return;
        }
        try {
            if (entry.getIndexInput() instanceof SwitchableIndexInput input) {
                action.accept(entry, input);
            }
        } catch (IOException | IllegalStateException e) {
            logger.warn("{} - {}", failureMessage, file, e);
        } finally {
            fileCache.decRef(key);
        }
    }

    @Override
    public void sync(Collection<String> names) {
        logger.trace("Tiered Directory[{}]: sync() called {}; Skipping sync.", this::toString, () -> names);
    }

    /**
     * Switch the file from local based full-file to remote based blocked-file
     * @param file
     */
    public void switchFileToRemote(String file) {
        withSwitchableEntry(file, (entry, input) -> input.switchToRemote(), "Failed to switch full file to blocked");
    }

    @Override
    public void close() throws IOException {
        ensureOpen();
        logger.trace("Stormborn Directory[{}]: close() called", this::toString);
        String[] localFiles = listLocalFiles();
        for (String localFile : localFiles) {
            // Delete segments_N file with ref count 1 created during index creation on replica shards
            // TODO: https://github.com/opensearch-project/OpenSearch/issues/17534
            if (localFile.startsWith(SEGMENTS)) {
                fileCache.remove(getFilePath(localFile));
            }
            String finalFileName = localFile;
            if (localFile.contains(BLOCK_FILE_IDENTIFIER)) {
                String[] segments = localFile.split(BLOCK_FILE_IDENTIFIER);
                finalFileName = segments[0].strip();
            }
            fileCache.remove(getFilePathSwitchable(localDirectory, finalFileName));
        }
        localDirectory.close();
    }

    public String[] listLocalFiles() throws IOException {
        ensureOpen();
        logger.trace("Stormborn Directory[{}]: listLocalOnly() called", this::toString);
        return localDirectory.listAll();
    }

    @Override
    protected void cacheFile(String fileName) throws IOException {
        cacheFile(fileName, false);
    }

    /**
     * Ensures the file's switchable entry (its family) is in the FileCache, creating it only when the key has no live
     * entry. The cache computes under the key's segment lock, so concurrent callers for the same file get one family
     * and never replace a live one. Returns the entry with one reference taken for the caller, as a put would.
     */
    // protected for testing
    protected CachedIndexInput cacheFile(String fileName, boolean cacheFromRemote) throws IOException {
        Path switchableFilePath = getFilePathSwitchable(localDirectory, fileName);
        try {
            return fileCache.compute(switchableFilePath, (key, existing) -> {
                if (existing != null && existing.isClosed() == false) {
                    return existing;
                }
                try {
                    return new CachedSwitchableIndexInput(
                        fileCache,
                        fileName,
                        DirectoryUtils.unwrapFSDirectory(localDirectory),
                        remoteDirectory,
                        transferManager,
                        cacheFromRemote,
                        threadPool,
                        tieredStoragePrefetchSettingsSupplier
                    );
                } catch (IOException e) {
                    throw new UncheckedIOException(e);
                }
            });
        } catch (UncheckedIOException e) {
            throw e.getCause();
        }
    }
}

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
import org.apache.lucene.store.FSDirectory;
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
import org.opensearch.storage.indexinput.PromotableCachedSwitchableIndexInput;
import org.opensearch.storage.indexinput.PromotableSwitchableIndexInputWrapper;
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
import static org.opensearch.storage.prefetch.TieredStoragePrefetchSettings.CFS_FILE_SUFFIX;
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
    private final WarmFullFileManager warmFullFileManager;

    public TieredDirectory(
        Directory localDirectory,
        Directory remoteDirectory,
        FileCache fileCache,
        ThreadPool threadPool,
        Supplier<TieredStoragePrefetchSettings> tieredStoragePrefetchSettingsSupplier,
        WarmFullFileManager warmFullFileManager
    ) {
        super(localDirectory, remoteDirectory, fileCache, threadPool);
        this.tieredStoragePrefetchSettingsSupplier = tieredStoragePrefetchSettingsSupplier;
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
            String resourceDescription = "SwitchableIndexInput (path=" + getFilePath(name) + ")";
            // A promotable entry hands out the promotable wrapper, and only to a handle that passes the open-time rules,
            // decided here because only openInput sees the IOContext. Every other handle is the existing wrapper: it
            // reads blocks as before, never triggers and never reads the mapping.
            if (indexInput instanceof PromotableCachedSwitchableIndexInput promotable && isEligibleForFullFile(name, context, original)) {
                return new PromotableSwitchableIndexInputWrapper(
                    resourceDescription,
                    (SwitchableIndexInput) original.clone(),
                    promotable.fullFileState(),
                    warmFullFileManager
                );
            }
            return new SwitchableIndexInputWrapper(resourceDescription, (SwitchableIndexInput) original.clone());
        } finally {
            fileCache.decRef(key);
        }
    }

    /**
     * Decides, once per opened handle, whether this handle takes part in whole-file promotion (may trigger it and reads
     * the mapping once FULL) or stays a plain block handle. All conditions must hold:
     * the file is larger than one block and below the mmap chunk size; the context is a plain DEFAULT read (excludes
     * merges and FLUSH); and it carries no ReadOnce/SEQUENTIAL hint (excludes recovery checksum reads). Temp/corrupted
     * files never reach here (handled earlier in openInput).
     *
     * <p>The extension is not part of this decision: it selects the entry type when the family is created (see
     * {@code newSwitchableEntry}), and the wrapper reads the setting again at trigger time so that an extension removed
     * since then stops promoting. Remote (block) mode is not part of it either: the searcher opens its handles at
     * refresh time and never reopens them for an unchanged segment, so a handle must be able to become promotable later,
     * once the segment is uploaded; see {@link FullFileState#markRemoteMode(String)}.
     */
    // visible for testing
    boolean isEligibleForFullFile(String name, IOContext context, SwitchableIndexInput original) {
        return ineligibleReason(name, context, original) == null;
    }

    /** The rule that rejects a file at open, or null when its handles may trigger a promotion. */
    private static String ineligibleReason(String name, IOContext context, SwitchableIndexInput original) {
        // Not an ordinary Lucene special file.
        if (FileTypeUtils.isSegmentsFile(name)
            || FileTypeUtils.isLockFile(name)
            || IndexFileNames.matchesExtension(name, CFS_FILE_SUFFIX)) {
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
        Path filePath = getFilePathSwitchable(localDirectory, file);
        try {
            CachedIndexInput entry = fileCache.get(filePath);
            SwitchableIndexInput switchableIndexInput = ((SwitchableIndexInput) entry.getIndexInput());
            switchableIndexInput.switchToRemote();
            // A promotable family may start promoting only once its local copy is gone, because promotion removes the
            // family's local cache entry. A family whose switch did not happen keeps serving the local copy on blocks.
            if (entry instanceof PromotableCachedSwitchableIndexInput promotable && switchableIndexInput.hasSwitchedToRemote()) {
                promotable.fullFileState().markRemoteMode(FullFileState.uploadedChecksum(remoteDirectory, file));
            }
        } catch (IOException | IllegalStateException e) {
            logger.error("Failed to switch full file to blocked - " + file, e);
        } finally {
            fileCache.decRef(filePath);
        }
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
     * Caches the file's switchable entry (its family) and returns it with one reference taken for the caller. A locally
     * written file is put as is: nothing opens it before its output closes, and its entry opens the local file and
     * calls into the FileCache, which must not happen under a segment lock. A remote-only file is created under the
     * key's segment lock, only when no live entry exists, so concurrent opens share one family; that entry does no IO
     * and makes no cache call while it is built.
     */
    // protected for testing
    protected CachedIndexInput cacheFile(String fileName, boolean cacheFromRemote) throws IOException {
        Path switchableFilePath = getFilePathSwitchable(localDirectory, fileName);
        if (cacheFromRemote == false) {
            CachedSwitchableIndexInput entry = newSwitchableEntry(fileName, false);
            fileCache.put(switchableFilePath, entry);
            return entry;
        }
        try {
            return fileCache.compute(switchableFilePath, (key, existing) -> {
                if (existing != null && existing.isClosed() == false) {
                    return existing;
                }
                try {
                    return newSwitchableEntry(fileName, true);
                } catch (IOException e) {
                    throw new UncheckedIOException(e);
                }
            });
        } catch (UncheckedIOException e) {
            throw e.getCause();
        }
    }

    /**
     * A file whose extension the setting lists gets the promotable entry, which owns the family's promotion state. Every
     * other file, and every file when there is no manager, gets the existing entry. The setting is read when the entry
     * is created, so a change to it applies to families created afterwards.
     */
    private CachedSwitchableIndexInput newSwitchableEntry(String fileName, boolean cacheFromRemote) throws IOException {
        FSDirectory fsDirectory = DirectoryUtils.unwrapFSDirectory(localDirectory);
        if (isCandidate(fileName)) {
            return new PromotableCachedSwitchableIndexInput(
                fileCache,
                fileName,
                fsDirectory,
                remoteDirectory,
                transferManager,
                cacheFromRemote,
                threadPool,
                tieredStoragePrefetchSettingsSupplier
            );
        }
        return new CachedSwitchableIndexInput(
            fileCache,
            fileName,
            fsDirectory,
            remoteDirectory,
            transferManager,
            cacheFromRemote,
            threadPool,
            tieredStoragePrefetchSettingsSupplier
        );
    }

    /** Whether the file's extension is listed in the promotion setting; never without a manager. */
    private boolean isCandidate(String fileName) {
        return warmFullFileManager != null && warmFullFileManager.isExtensionEligible(WarmFullFileSettings.extensionOf(fileName));
    }
}

/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */
package org.opensearch.storage.warmfullfile;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.lucene.codecs.CodecUtil;
import org.apache.lucene.store.DataAccessHint;
import org.apache.lucene.store.FSDirectory;
import org.apache.lucene.store.IOContext;
import org.apache.lucene.store.IndexInput;
import org.apache.lucene.store.MMapDirectory;
import org.apache.lucene.store.MemorySegmentAccessInput;
import org.opensearch.common.annotation.ExperimentalApi;
import org.opensearch.common.lease.Releasable;
import org.opensearch.common.metrics.CounterMetric;
import org.opensearch.common.util.CancellableThreads;
import org.opensearch.index.engine.OpenSearchConcurrentMergeScheduler;
import org.opensearch.index.store.FsDirectoryFactory;
import org.opensearch.index.store.remote.file.AbstractBlockIndexInput;
import org.opensearch.index.store.remote.filecache.CachedFullFileIndexInput;
import org.opensearch.index.store.remote.filecache.CachedIndexInput;
import org.opensearch.index.store.remote.filecache.FileCache;
import org.opensearch.index.store.remote.utils.BlobFetchRequest;
import org.opensearch.index.store.remote.utils.TransferManager;
import org.opensearch.storage.indexinput.CachedSwitchableIndexInput;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.NoSuchFileException;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Collections;
import java.util.EnumMap;
import java.util.EnumSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executor;
import java.util.concurrent.Semaphore;
import java.util.function.LongSupplier;

/**
 * Node-level coordinator for warm-tier whole-file promotion.
 *
 * <p>{@link #promote(FullFileState)} runs on a background executor (never the search thread). It admits a promotion
 * only when a download slot is free and the per-segment whole-file budget allows it, downloads the whole file in
 * parts of {@code partSize} bytes (one block by default) via {@link TransferManager#fetchBlobAsync}, written to a temp
 * file and renamed into place after an fsync so that a whole file at its final path is always complete, then opens the
 * family's own memory mapping and flips the state to {@link FullFileState.State#FULL}. A whole file already on local
 * disk, such as one left by a previous run of the node, is reused instead of downloaded when its length and footer
 * checksum match the remote store's. On any decline or failure the state returns to {@link FullFileState.State#BLOCK} with a
 * retry time set. Every promotion that reaches FULL is logged at INFO with its download time, and {@link #counters()}
 * returns the node's promotion and decline counters.
 *
 * @opensearch.experimental
 */
@ExperimentalApi
public class WarmFullFileManager {

    private static final Logger logger = LogManager.getLogger(WarmFullFileManager.class);

    /** Default download part size: one block, the same size the block path reads. */
    static final long DEFAULT_PART_SIZE = AbstractBlockIndexInput.Builder.DEFAULT_BLOCK_SIZE;

    /**
     * Why a promotion did not reach FULL. Each decline or failure is counted under exactly one reason.
     *
     * @opensearch.experimental
     */
    @ExperimentalApi
    public enum DeclineReason {
        /** Every download slot was in use. */
        NO_SLOT,
        /** The file did not fit the per-segment whole-file budget. */
        BUDGET_EXCEEDED,
        /** The file is not below the local directory's mmap chunk size. */
        TOO_LARGE,
        /** The local directory serves the file with an input that is not Lucene's memory-mapped input. */
        NOT_MMAP,
        /** A remote read failed. */
        DOWNLOAD_FAILED,
        /** The downloaded file does not have the expected length. */
        LENGTH_MISMATCH,
        /** The downloaded file's checksum does not match the one the remote store holds. */
        CHECKSUM_MISMATCH,
        /** The whole-file cache entry was evicted before the family reached FULL. */
        ENTRY_EVICTED,
        /** The family closed while the download or the publish was in progress; a download in flight is cancelled. */
        FAMILY_CLOSED,
        /** An unexpected exception while starting the download or publishing the mapping. */
        ERROR
    }

    /**
     * Declines that are an ordinary outcome of admission or of a close, logged at DEBUG. NOT_MMAP is here because
     * {@link #declinePermanently} logs it at WARN once per extension. Every other reason is logged at WARN.
     */
    private static final Set<DeclineReason> EXPECTED_DECLINES = EnumSet.of(
        DeclineReason.NO_SLOT,
        DeclineReason.BUDGET_EXCEEDED,
        DeclineReason.FAMILY_CLOSED,
        DeclineReason.NOT_MMAP
    );
    /** Retry delay after a failure or any decline other than no-slot. */
    static final long RETRY_DELAY_MILLIS = 60_000L;
    /**
     * Retry delay after a decline for lack of a free download slot. Short against a download, so a slot freed by a
     * finishing download is reused quickly; each retry costs one semaphore attempt on a background thread.
     */
    static final long NO_SLOT_RETRY_DELAY_MILLIS = 5_000L;
    /** Whole-file bytes admitted per cache segment must stay at or below this fraction of the segment's capacity. */
    static final double DEFAULT_BUDGET_FRACTION = 0.5;

    /**
     * Largest file that may be promoted: strictly below Lucene's default mmap chunk size (16 GiB on 64-bit JVMs), so the
     * family mapping is a single chunk. Real .faiss/.veq files are far below this.
     */
    public static final long MAX_WHOLE_FILE_BYTES = MMapDirectory.DEFAULT_MAX_CHUNK_SIZE;

    private final FileCache fileCache;
    private final Executor executor;
    private final LongSupplier clock;
    private volatile int maxConcurrentDownloads;
    private final ResizableSemaphore slots;
    private final SegmentBudget budget;
    private final long partSize;
    private final CounterMetric requested = new CounterMetric();
    private final CounterMetric started = new CounterMetric();
    private final CounterMetric reused = new CounterMetric();
    private final CounterMetric demoted = new CounterMetric();
    private final CounterMetric restored = new CounterMetric();
    private final CounterMetric downloadedBytes = new CounterMetric();
    // NOT_MMAP declines that came after a completed download (a directory type not known at admission).
    private final CounterMetric notMmapAfterDownload = new CounterMetric();
    // The families currently FULL on this node, so a settings change can find the ones to demote.
    private final Set<FullFileState> fullFamilies = ConcurrentHashMap.newKeySet();
    // Demoted families still holding their mapping, so a settings change can restore the ones whose extension returns.
    private final Set<FullFileState> demotedFamilies = ConcurrentHashMap.newKeySet();
    /**
     * FULL families that closed while a reader still held handles. Each keeps its mapping, its whole file and its cache
     * reference until the last handle closes; one that never leaves is a leaked IndexInput somewhere above.
     */
    private final Set<FullFileState> awaitingHandleClose = ConcurrentHashMap.newKeySet();
    // The extensions the setting currently allows; the trigger re-checks this so a change applies to open handles too.
    private volatile Set<String> eligibleExtensions = Set.of();
    // Every extension the setting has listed since this manager was created. A file of such an extension gets the
    // promotable entry even while the extension is off the list, so listing it again applies to families created in
    // between without a reopen; whether a promotion starts is always the current list, checked by the trigger.
    private final Set<String> everEligibleExtensions = ConcurrentHashMap.newKeySet();
    private final CounterMetric promoted = new CounterMetric();
    private final CounterMetric promotedBytes = new CounterMetric();
    private final CounterMetric downloadMillis = new CounterMetric();
    private final Map<DeclineReason, CounterMetric> declinedByReason = new EnumMap<>(DeclineReason.class);
    // Extensions already reported as permanently declined on this node (one WARN per extension, DEBUG per file after).
    private final Set<String> permanentlyDeclinedExtensions = ConcurrentHashMap.newKeySet();
    // Runs on the promotion executor after admission (slot and budget held) and before the first remote read.
    // No-op in production; tests pass a blocking hook to hold a download at a known point.
    private volatile Runnable beforeDownloadHook;

    /**
     * Production constructor.
     *
     * @param fileCache              the node file cache
     * @param executor               the pool to run promote(), the completion handler, and the deferred cleanup on
     *                               (never the search thread, never the FileCache listener thread)
     * @param clock                  a monotonic millis clock (e.g. {@code threadPool::relativeTimeInMillis})
     * @param maxConcurrentDownloads the initial download cap, {@link WarmFullFileSettings#MAX_CONCURRENT_DOWNLOADS};
     *                               later values arrive through {@link #onMaxConcurrentDownloadsChanged}
     */
    public WarmFullFileManager(FileCache fileCache, Executor executor, LongSupplier clock, int maxConcurrentDownloads) {
        this(fileCache, executor, clock, maxConcurrentDownloads, DEFAULT_BUDGET_FRACTION, DEFAULT_PART_SIZE);
    }

    /** Package-private constructor for tests (small slot count, budget fraction, and part size). */
    WarmFullFileManager(
        FileCache fileCache,
        Executor executor,
        LongSupplier clock,
        int maxConcurrentDownloads,
        double budgetFraction,
        long partSize
    ) {
        this(fileCache, executor, clock, maxConcurrentDownloads, budgetFraction, partSize, () -> {});
    }

    /**
     * The constructor every other constructor delegates to. The budget is kept per segment of {@code fileCache}.
     *
     * @param beforeDownloadHook runs once per download, on the promotion executor, after the slot and the budget are
     *                           held and before the first remote read; a no-op in production, a blocking hook in tests
     */
    WarmFullFileManager(
        FileCache fileCache,
        Executor executor,
        LongSupplier clock,
        int maxConcurrentDownloads,
        double budgetFraction,
        long partSize,
        Runnable beforeDownloadHook
    ) {
        this.fileCache = fileCache;
        this.beforeDownloadHook = beforeDownloadHook;
        this.executor = executor;
        this.clock = clock;
        this.maxConcurrentDownloads = Math.max(1, maxConcurrentDownloads);
        this.slots = new ResizableSemaphore(this.maxConcurrentDownloads);
        int segments = fileCache.segmentCount();
        long perSegmentCapacity = (fileCache.capacity() + segments - 1) / segments;
        this.budget = new SegmentBudget(segments, (long) (perSegmentCapacity * budgetFraction));
        this.partSize = partSize > 0 ? partSize : DEFAULT_PART_SIZE;
        for (DeclineReason reason : DeclineReason.values()) {
            declinedByReason.put(reason, new CounterMetric());
        }
    }

    /** @return the executor promote() must be submitted to (never the search thread) */
    public Executor executor() {
        return executor;
    }

    /**
     * Replaces the download hook of a running manager. For the integration tests in this package, which get the
     * manager from the node and so cannot pass the hook to the constructor: a blocking hook holds a download after the
     * slot and the budget are held and before the first remote read, so a test can act (close the shard, delete the
     * index) while a promotion is in flight.
     *
     * @param hook the hook; null restores the no-op
     */
    void setBeforeDownloadHook(Runnable hook) {
        this.beforeDownloadHook = hook == null ? () -> {} : hook;
    }

    /** @return the current monotonic time in millis, the clock {@link #requestPromotion} compares retryAt against */
    public long relativeTimeInMillis() {
        return clock.getAsLong();
    }

    /**
     * Called by a promotable handle on each clone or slice served from blocks. Requests a promotion at most once per
     * BLOCK period: only when the file is in remote (block) mode, only while the setting still lists its extension,
     * never on a Lucene merge thread, and only for the thread whose {@link FullFileState#onAccess} moves the family to
     * PROMOTING (which also enforces BLOCK, the retry time and a pending release). Does that transition and an executor
     * submit and nothing else; {@link #promote} never runs on the caller's thread.
     *
     * @param st the family's whole-file promotion state
     */
    public void requestPromotion(FullFileState st) {
        // Checked on every access, not at open: a handle opened before the upload becomes promotable once the family
        // is in remote mode (see FullFileState.markRemoteMode for when that is).
        if (st.isRemoteMode() == false) {
            return;
        }
        // Read the setting now, not only when the family was created: an extension removed since then must not promote,
        // and one listed again must.
        if (isExtensionEligible(WarmFullFileSettings.extensionOf(st.fileName())) == false) {
            return;
        }
        // A merge reads a source segment once and then deletes it, so promoting for it only spends a slot, budget and
        // a download on a file with minutes to live. Merges usually read through the searcher's already-open reader,
        // whose handles passed the open-time context check, so the merge is recognised here by the thread Lucene runs
        // it on instead.
        if (OpenSearchConcurrentMergeScheduler.isMergeThread(Thread.currentThread())) {
            return;
        }
        if (st.onAccess(clock.getAsLong())) {
            // Once per promotion attempt, not per clone or slice. The thread name shows what started it.
            logger.debug("whole-file promotion requested for {} on thread [{}]", st.fullFilePath(), Thread.currentThread().getName());
            try {
                executor.execute(() -> promote(st));
            } catch (RuntimeException e) {
                // e.g. executor rejected: back to BLOCK with the retry time unchanged, so a later search can retry.
                st.abandonPromotion(st.retryAt());
                logger.debug("whole-file promotion submit rejected for {}", st.fullFilePath(), e);
            }
        }
    }

    /**
     * Attempts to promote the family whose state is given (already moved to PROMOTING by {@link #requestPromotion}).
     * Runs on the background executor. On decline or failure, returns the state to BLOCK and sets retryAt.
     */
    public void promote(FullFileState st) {
        requested.inc();
        // The family closed (shard closed, entry evicted or replaced) between the trigger's CAS and this run: nothing
        // to promote, and no slot or budget to spend on a download nobody will use. (A download already running when the
        // family closes is cancelled by closeFamily; its completion handler releases the slot and the budget.)
        if (st.state() != FullFileState.State.PROMOTING) {
            return;
        }
        new PromotionAttempt(st).start();
    }

    /**
     * One promotion of one family, from admission to FULL or to its decline. What the attempt has taken so far are its
     * fields: the download slot, the reserved budget bytes, a reference on the whole-file cache entry and the mapping.
     * {@link #close()} gives back whatever is still held, so every failure is a close() plus an
     * {@link #abandonPromotion}, and {@link #completePromotion()} hands the bytes, the reference and the mapping to the
     * family together or not at all. {@link #start()} runs on the promotion executor and
     * {@link #onDownloadComplete} on the download's completion thread, one after the other.
     */
    private final class PromotionAttempt {
        private final FullFileState st;
        private final Path path;
        private final long length;
        private final int segment;
        private boolean holdsSlot;
        private Releasable bytes;
        private boolean holdsReference;
        private IndexInput mapping;
        // Set once the family has taken the bytes, the reference and the mapping; only the slot is left to give back.
        private boolean published;
        private boolean reusedLocalFile;
        private long startedAt;

        PromotionAttempt(FullFileState st) {
            this.st = st;
            this.path = st.fullFilePath();
            this.length = st.fileLength();
            this.segment = fileCache.segmentIndexFor(path);
        }

        /** Admission, the local-file reuse check and the start of the download, which completes in {@link #onDownloadComplete}. */
        void start() {
            try {
                // Single-chunk guard. Eligibility already excludes these; this also covers a
                // local directory configured with a smaller chunk size. Checked before taking a slot or reserving bytes.
                if (length >= maxChunkSize(st.localDirectory())) {
                    abandonPromotion(DeclineReason.TOO_LARGE, "file of " + length + " bytes exceeds the mmap chunk size");
                    return;
                }
                // A file the local directory serves with NIO is declined here, before a slot, the budget or a download.
                // Known from the directory type alone for MMapDirectory, hybrid and NIOFSDirectory; any other type is
                // settled after the download, when the mapping is opened.
                if (Boolean.FALSE.equals(FsDirectoryFactory.isMemoryMapped(st.localDirectory(), st.fileName()))) {
                    declinePermanently(st, "the local directory serves it with NIO, not mmap");
                    return;
                }

                // Admission: a free slot AND the per-segment whole-file budget.
                if (slots.tryAcquire() == false) {
                    abandonPromotion(DeclineReason.NO_SLOT, "no free download slot", clock.getAsLong() + NO_SLOT_RETRY_DELAY_MILLIS);
                    return;
                }
                holdsSlot = true;
                bytes = budget.reserve(segment, length);
                if (bytes == null) {
                    close();
                    abandonPromotion(DeclineReason.BUDGET_EXCEEDED, "per-segment whole-file budget exceeded");
                    return;
                }

                // An entry already at the whole-file key: fetchBlobAsync can only drive its own in-flight placeholder (a
                // private type); on any other live entry its asyncLoadIndexInput returns null and leaks the compute
                // reference. A restore placeholder stands for a whole file left on disk by a previous run of the node:
                // fetchBlobAsync replaces it (it reports itself closed) and opens the file without fetching anything, so
                // the file is kept when it validates. Other known entry types are removed; anything else is left for
                // fetchBlobAsync.
                boolean reuse = false;
                CachedIndexInput existing = fileCache.get(path);
                if (existing != null) {
                    holdsReference = true;
                    if (existing instanceof FileCache.RestoredCachedIndexInput) {
                        // Validated under the reference taken above: a referenced entry is not evictable, so the file
                        // cannot be deleted under the validation. The reference is kept until fetchBlobAsync has replaced
                        // the placeholder (the node keeps its count across the replacement) and released after it.
                        reuse = validateExisting(st);
                        if (reuse == false) {
                            dropEntry(); // closes the placeholder and deletes the file
                        }
                    } else {
                        releaseReference();
                        if (isKnownNonReusableEntry(existing)) {
                            fileCache.remove(path);
                        }
                    }
                } else if (Files.exists(path)) {
                    // A whole file with no cache entry (e.g. left by a crash): kept when it validates, else deleted.
                    reuse = validateExisting(st);
                    if (reuse == false) {
                        Files.delete(path);
                    }
                }

                BlobFetchRequest request = buildRequest(st, length);
                beforeDownloadHook.run();
                startedAt = clock.getAsLong();
                reusedLocalFile = reuse;
                if (reuse == false) {
                    started.inc();
                }
                CompletableFuture<IndexInput> future;
                try {
                    future = st.transferManager().fetchBlobAsync(request, st.downloadCancellation());
                } finally {
                    releaseReference();
                }
                if (future == null) {
                    // fetchBlobAsync found a live entry it cannot drive (one put at the key after the check above).
                    throw new IllegalStateException("whole-file entry at " + path + " cannot be downloaded into");
                }
                // The future's value is the cache entry's own (non-clone) input; it must NOT be closed here (closing it
                // would close the entry's master input). The family serves from its own mapping opened on completion.
                future.whenComplete((indexInput, throwable) -> onDownloadComplete(throwable));
            } catch (Exception e) {
                logger.warn("whole-file promotion failed to start for {}", path, e);
                close();
                abandonPromotion(DeclineReason.ERROR, "exception starting download: " + e.getMessage());
            } catch (Error e) {
                // Unwind before the error propagates, or the family would stay PROMOTING with its slot and budget.
                close();
                abandonPromotion(DeclineReason.ERROR, "error starting download: " + e);
                throw e;
            }
        }

        private void onDownloadComplete(Throwable throwable) {
            try {
                if (throwable != null) {
                    close();
                    if (isCancellation(throwable)) {
                        abandonPromotion(DeclineReason.FAMILY_CLOSED, "download cancelled: family closed");
                    } else {
                        abandonPromotion(DeclineReason.DOWNLOAD_FAILED, "download failed: " + throwable.getMessage());
                    }
                    return;
                }
                if (reusedLocalFile == false) {
                    downloadedBytes.inc(length);
                }
                // fetchBlobAsync releases its own reference on success; take the family's reference now.
                if (fileCache.get(path) == null) {
                    close();
                    abandonPromotion(DeclineReason.ENTRY_EVICTED, "entry evicted before FULL");
                    return;
                }
                holdsReference = true;
                FSDirectory localDirectory = st.localDirectory();
                if (lengthOnDisk(localDirectory, st.fileName()) != length) {
                    // Remove the entry (closes it and deletes the truncated file). Without this, the completed
                    // DelayedCreation entry stays live and the next retry reuses the same truncated file and fails
                    // again. The same for an entry whose file is gone: a completed entry joined by this attempt whose
                    // file was deleted on the local directory directly, as a recovery cleanup does.
                    close();
                    dropEntry();
                    abandonPromotion(DeclineReason.LENGTH_MISMATCH, "downloaded length mismatch");
                    return;
                }
                // A reused local file was validated before the entry was created; a fresh download is checked against
                // the remote store's checksum here.
                if (reusedLocalFile == false && matchesRemoteChecksum(st) == false) {
                    close();
                    dropEntry();
                    abandonPromotion(DeclineReason.CHECKSUM_MISMATCH, "downloaded checksum mismatch");
                    return;
                }
                // Open the family's OWN mapping with the RANDOM hint (do not reuse the cache entry's input).
                mapping = localDirectory.openInput(st.fileName(), IOContext.DEFAULT.withHints(DataAccessHint.RANDOM));
                // The master must be Lucene's mmap input. Views of an NIO master would go through
                // BufferedIndexInput (a copy per read, no k-NN SIMD), which is not the feature. Known directory types
                // were settled in start(); this catches any other type, and the answer will not change for this family.
                if ((mapping instanceof MemorySegmentAccessInput) == false) {
                    String servedAs = mapping.getClass().getSimpleName();
                    // Drop the useless whole file so it is not charged against the cache until eviction, and so nothing
                    // reuses it.
                    close();
                    dropEntry();
                    notMmapAfterDownload.inc();
                    declinePermanently(st, "the local directory serves it as " + servedAs + ", not mmap");
                    return;
                }
                if (completePromotion() == false) {
                    close();
                    return;
                }
                close(); // the family owns the rest; this gives back the slot
                final long elapsedMillis = clock.getAsLong() - startedAt;
                promoted.inc();
                promotedBytes.inc(length);
                if (reusedLocalFile) {
                    reused.inc();
                    logger.info(
                        "promoted [{}] to a whole file: {} bytes reused from the local copy (cache segment {})",
                        path,
                        length,
                        segment
                    );
                } else {
                    downloadMillis.inc(elapsedMillis);
                    logger.info(
                        "promoted [{}] to a whole file: {} bytes in {} ms ({} parts of {} bytes, cache segment {})",
                        path,
                        length,
                        elapsedMillis,
                        partCount(length),
                        partSize,
                        segment
                    );
                }
                // The setting may have dropped the extension while the download ran; such a family was not FULL when
                // the change was applied, so it is demoted here instead.
                demoteIfNoLongerEligible(st);
            } catch (Exception e) {
                if (published) {
                    // The family is FULL and serving; its reference, budget and slot are already settled.
                    logger.warn("whole-file bookkeeping after publishing [{}] failed", path, e);
                    return;
                }
                close();
                abandonPromotion(DeclineReason.ERROR, "exception publishing FULL: " + e.getMessage());
            } catch (Error e) {
                // Unwind before the error propagates, or the family would stay PROMOTING with its slot and budget.
                close();
                abandonPromotion(DeclineReason.ERROR, "error completing download: " + e);
                throw e;
            }
        }

        /**
         * Hands the reserved bytes, the cache reference and the mapping to the family, which publishes the mapping and
         * moves to FULL in one step. All three or none: when another completion handler published first, or the family
         * left PROMOTING, this attempt still holds them for its {@link #close()}.
         *
         * @return true if the family is FULL and owns them
         */
        private boolean completePromotion() {
            // Registered before the publish: a close right after it schedules the teardown, which must find the family
            // in the registry to take it out again.
            fullFamilies.add(st);
            Releasable reservation = bytes;
            FullFileState.Completion completion = st.completePromotion(mapping, executor, () -> {
                reservation.close();
                fullFamilies.remove(st);
                demotedFamilies.remove(st);
            }, () -> awaitingHandleClose.add(st), () -> awaitingHandleClose.remove(st));
            switch (completion) {
                case PUBLISHED_BY_OTHER -> {
                    // Concurrent promote() calls join one in-flight download, so each gets a completion. The handler
                    // that published first owns the state and the registration.
                    logger.debug("whole-file mapping for {} was already published by another completion", path);
                    return false;
                }
                case NOT_PROMOTING -> {
                    // The family closed during the download or the publish; nothing was installed.
                    fullFamilies.remove(st);
                    abandonPromotion(DeclineReason.FAMILY_CLOSED, "family closed during publish");
                    return false;
                }
                default -> {
                    bytes = null;
                    holdsReference = false;
                    mapping = null;
                    published = true;
                    return true;
                }
            }
        }

        /** Gives back whatever this attempt still holds: the mapping, the cache reference, the budget bytes, the slot. */
        void close() {
            if (mapping != null) {
                closeQuietly(mapping);
                mapping = null;
            }
            releaseReference();
            if (bytes != null) {
                bytes.close();
                bytes = null;
            }
            if (holdsSlot) {
                holdsSlot = false;
                slots.release();
            }
        }

        private void releaseReference() {
            if (holdsReference) {
                holdsReference = false;
                fileCache.decRef(path);
            }
        }

        /** Releases this attempt's reference on the whole-file entry and removes the entry, closing it and deleting its file. */
        private void dropEntry() {
            releaseReference();
            fileCache.remove(path);
        }

        private void abandonPromotion(DeclineReason reason, String detail) {
            decline(st, reason, detail);
        }

        private void abandonPromotion(DeclineReason reason, String detail, long retryAt) {
            decline(st, reason, detail, retryAt);
        }
    }

    /**
     * Declines for the lifetime of this family: a retry would fail the same way, so a 60 s retry would only take a slot
     * and a budget reservation every minute forever. {@code retryAt = Long.MAX_VALUE} makes the trigger never fire again.
     * A new family for the same file (e.g. after a shard restart) tries once more.
     *
     * <p>Logging: the cause is one operator setting (an extension the local directory serves with NIO), but it would
     * surface once per segment per shard, so only the first file of each extension is logged at WARN on this node;
     * later files of that extension are logged at DEBUG by {@link #decline(FullFileState, DeclineReason, String, long)}.
     */
    private void declinePermanently(FullFileState st, String reason) {
        if (decline(st, DeclineReason.NOT_MMAP, reason, Long.MAX_VALUE)) {
            String extension = WarmFullFileSettings.extensionOf(st.fileName());
            if (extension != null && permanentlyDeclinedExtensions.add(extension)) {
                logger.warn(
                    "whole-file promotion of [.{}] files is permanently declined on this node: {} (first file [{}]). "
                        + "Remove the extension from [{}] or take it off the local directory's NIO list; "
                        + "further [.{}] files are logged at DEBUG",
                    extension,
                    reason,
                    st.fullFilePath(),
                    WarmFullFileSettings.EXTENSIONS.getKey(),
                    extension
                );
            }
        }
    }

    /**
     * Whether the whole file on local disk at the family's path may be served as is. A download reaches the final path
     * only by the rename after its fsync, so a file there is complete; these checks catch a file that is not the one
     * the remote store holds. Cheapest first, each failure meaning the file is deleted and downloaded again: the
     * length, then the footer checksum against the remote store's. A file whose remote checksum is unknown is never
     * reused.
     */
    private boolean validateExisting(FullFileState st) {
        final Path path = st.fullFilePath();
        if (st.expectedChecksum() == null) {
            logger.debug("whole file at {} is not reused: no remote checksum is known for it", path);
            return false;
        }
        try {
            long size = Files.size(path);
            if (size != st.fileLength()) {
                logger.debug("whole file at {} is not reused: {} bytes on disk, {} expected", path, size, st.fileLength());
                return false;
            }
        } catch (IOException e) {
            logger.debug("whole file at {} is not reused: its size could not be read", path, e);
            return false;
        }
        return matchesRemoteChecksum(st);
    }

    /**
     * Compares the local whole file's footer checksum against the remote store's. A file with no known remote checksum
     * passes, there being nothing to compare to; an unreadable or footerless file fails.
     */
    private boolean matchesRemoteChecksum(FullFileState st) {
        final String expected = st.expectedChecksum();
        if (expected == null) {
            return true;
        }
        final Path path = st.fullFilePath();
        try (IndexInput in = st.localDirectory().openInput(st.fileName(), IOContext.READONCE)) {
            final long expectedChecksum = Long.parseLong(expected);
            long footer = CodecUtil.retrieveChecksum(in);
            if (footer != expectedChecksum) {
                logger.debug(
                    "whole file at {} fails validation: footer checksum {} differs from the remote store's {}",
                    path,
                    footer,
                    expected
                );
                return false;
            }
            return true;
        } catch (IOException | NumberFormatException e) {
            logger.debug("whole file at {} fails validation: {}", path, e.toString());
            return false;
        }
    }

    private void decline(FullFileState st, DeclineReason reason, String detail) {
        decline(st, reason, detail, clock.getAsLong() + RETRY_DELAY_MILLIS);
    }

    /**
     * Returns the family to BLOCK with the given retry time and counts the decline under {@code reason}.
     *
     * @return true if the family went back to BLOCK; false if it closed meanwhile. A download cancelled by the close
     *         ends here as {@link DeclineReason#FAMILY_CLOSED} and is logged at DEBUG; any other end after a close is
     *         logged at WARN, since that download ran to completion for a family nobody will use
     */
    private boolean decline(FullFileState st, DeclineReason reason, String detail, long retryAt) {
        declinedByReason.get(reason).inc();
        if (st.abandonPromotion(retryAt)) {
            if (EXPECTED_DECLINES.contains(reason)) {
                // Admission refusals and closes are the normal steady state and retry on their own; a WARN per file
                // per retry across every warm shard would drown the log. The counters carry them.
                logger.debug("whole-file promotion declined ({}) for {}", detail, st.fullFilePath());
            } else {
                logger.warn("whole-file promotion declined ({}) for {}", detail, st.fullFilePath());
            }
            return true;
        }
        if (st.state() == FullFileState.State.CLOSED) {
            if (reason == DeclineReason.FAMILY_CLOSED) {
                logger.debug("whole-file promotion of [{}] ended after its family closed ({})", st.fullFilePath(), detail);
            } else {
                logger.warn("whole-file promotion of [{}] ended after its family closed ({})", st.fullFilePath(), detail);
            }
        }
        return false;
    }

    /** True when the download ended because its family's cancellation handle was cancelled. */
    private static boolean isCancellation(Throwable throwable) {
        for (Throwable t = throwable; t != null; t = t.getCause()) {
            if (t instanceof CancellableThreads.ExecutionCancelledException) {
                return true;
            }
        }
        return false;
    }

    /**
     * Applies a new download cap from {@link WarmFullFileSettings#MAX_CONCURRENT_DOWNLOADS}. Raising it admits more
     * downloads at once; lowering it lets the ones in flight finish and admits no new one until the count is under
     * the new cap.
     *
     * @param newMax the cap, at least one
     */
    public void onMaxConcurrentDownloadsChanged(int newMax) {
        synchronized (slots) {
            int target = Math.max(1, newMax);
            int delta = target - maxConcurrentDownloads;
            if (delta > 0) {
                slots.release(delta);
            } else if (delta < 0) {
                // Permits may go negative: downloads in flight finish, and no new one is admitted until enough have.
                slots.reduce(-delta);
            }
            maxConcurrentDownloads = target;
        }
    }

    /** A {@link Semaphore} whose permit count can also be lowered, for a cap that changes at runtime. */
    private static final class ResizableSemaphore extends Semaphore {
        ResizableSemaphore(int permits) {
            super(permits);
        }

        void reduce(int permits) {
            reducePermits(permits);
        }
    }

    /**
     * Applies a new set of eligible extensions: remembers it for the trigger's check and demotes every FULL family
     * whose extension is no longer in it. A demoted family serves blocks from its next clone or slice; its mapping,
     * cache reference and budget are released once its handles are gone.
     *
     * @param extensions the eligible extensions, normalized as {@link WarmFullFileSettings#getExtensions()} returns them
     */
    public void onExtensionsChanged(Set<String> extensions) {
        everEligibleExtensions.addAll(extensions);
        eligibleExtensions = extensions;
        for (FullFileState st : fullFamilies) {
            if (st.state() == FullFileState.State.CLOSED) {
                // Closed; its teardown takes it out when the deferred cleanup runs, or this does if sooner.
                fullFamilies.remove(st);
                continue;
            }
            demoteIfNoLongerEligible(st);
        }
        for (FullFileState st : demotedFamilies) {
            if (st.state() == FullFileState.State.CLOSED) {
                demotedFamilies.remove(st);
                continue;
            }
            restoreIfEligibleAgain(st);
        }
    }

    /** Demotes a FULL family whose extension the setting no longer lists; a no-op for any other family. */
    private void demoteIfNoLongerEligible(FullFileState st) {
        String extension = WarmFullFileSettings.extensionOf(st.fileName());
        if (isExtensionEligible(extension) || st.demote() == false) {
            return;
        }
        fullFamilies.remove(st);
        demotedFamilies.add(st);
        demoted.inc();
        logger.info(
            "demoted [{}] back to blocks: extension [{}] is no longer configured; its mapping is released once its {} handle(s) close",
            st.fullFilePath(),
            extension,
            st.handleCount()
        );
    }

    /**
     * Takes a demoted family back to FULL when its extension is listed again and its mapping is still held, so the
     * setting is a live switch in both directions: nothing is downloaded or mapped a second time. A family whose
     * release has already run is promoted again the ordinary way by its next trigger.
     */
    private void restoreIfEligibleAgain(FullFileState st) {
        String extension = WarmFullFileSettings.extensionOf(st.fileName());
        if (isExtensionEligible(extension) == false || st.restore() == false) {
            return;
        }
        demotedFamilies.remove(st);
        fullFamilies.add(st);
        restored.inc();
        logger.info(
            "restored [{}] to its whole file: extension [{}] is configured again and the mapping was still held",
            st.fullFilePath(),
            extension
        );
    }

    /**
     * @param extension a file extension without the leading dot
     * @return whether the setting currently allows whole-file promotion for it
     */
    public boolean isExtensionEligible(String extension) {
        return extension != null && eligibleExtensions.contains(extension);
    }

    /**
     * @param extension a file extension without the leading dot
     * @return whether the setting has listed it at any time since this manager was created, whether or not it is
     *         listed now; the directory creates the promotable entry for such files
     */
    public boolean hasBeenEligible(String extension) {
        return extension != null && everEligibleExtensions.contains(extension);
    }

    /**
     * The decline reasons that mean a download or publish was attempted and did not reach FULL (not admission). NOT_MMAP
     * is decided at admission for every known directory type, so it is not listed; the NOT_MMAP declines that followed a
     * download are added to {@code failed} from their own counter.
     */
    private static final Set<DeclineReason> FAILURE_REASONS = EnumSet.of(
        DeclineReason.DOWNLOAD_FAILED,
        DeclineReason.LENGTH_MISMATCH,
        DeclineReason.CHECKSUM_MISMATCH,
        DeclineReason.ENTRY_EVICTED,
        DeclineReason.FAMILY_CLOSED,
        DeclineReason.ERROR
    );

    /**
     * This node's promotion counters, by name. Cumulative counters ({@code requested}, {@code started}, {@code reused},
     * {@code promoted}, {@code demoted}, {@code restored}, {@code downloadedBytes}, {@code promotedBytes},
     * {@code downloadMillis}, {@code declined.<reason>} with the totals {@code declined} and {@code failed}) never
     * decrease for the life of the node; {@code reservedBytes}, {@code activeDownloads},
     * {@code familiesAwaitingHandleClose} and {@code bytesAwaitingHandleClose} describe the current moment. Read by
     * tests and node-local code; it has no wire format and is not part of any API.
     *
     * @return an unmodifiable snapshot
     */
    public Map<String, Long> counters() {
        Map<String, Long> counters = new LinkedHashMap<>();
        counters.put("requested", requested.count());
        counters.put("started", started.count());
        counters.put("reused", reused.count());
        counters.put("promoted", promoted.count());
        counters.put("demoted", demoted.count());
        counters.put("restored", restored.count());
        counters.put("downloadedBytes", downloadedBytes.count());
        counters.put("promotedBytes", promotedBytes.count());
        counters.put("downloadMillis", downloadMillis.count());
        long declined = 0;
        long failed = 0;
        for (DeclineReason reason : DeclineReason.values()) {
            long count = declinedByReason.get(reason).count();
            counters.put("declined." + reason, count);
            declined += count;
            if (FAILURE_REASONS.contains(reason)) {
                failed += count;
            }
        }
        counters.put("declined", declined);
        counters.put("failed", failed + notMmapAfterDownload.count());
        counters.put("reservedBytes", budget.totalReserved());
        counters.put("activeDownloads", (long) (maxConcurrentDownloads - slots.availablePermits()));
        int awaitingFamilies = 0;
        long awaitingBytes = 0;
        for (FullFileState st : awaitingHandleClose) {
            awaitingFamilies++;
            awaitingBytes += st.fileLength();
        }
        counters.put("familiesAwaitingHandleClose", (long) awaitingFamilies);
        counters.put("bytesAwaitingHandleClose", awaitingBytes);
        return Collections.unmodifiableMap(counters);
    }

    /** @return the whole-file bytes currently reserved in each cache segment, by segment index */
    public long[] reservedBytesPerSegment() {
        return budget.reservedPerSegment();
    }

    /** @return the most whole-file bytes one cache segment may hold; a promotion that would exceed it is declined */
    public long perSegmentBudgetBytes() {
        return budget.capPerSegment();
    }

    /** The number of parts a download of {@code length} bytes is split into. */
    private long partCount(long length) {
        return (length + partSize - 1) / partSize;
    }

    /** Builds the download request by hand: ceil(length / partSize) parts at absolute offsets, last clipped, no zero tail. */
    private BlobFetchRequest buildRequest(FullFileState st, long length) {
        List<BlobFetchRequest.BlobPart> parts = buildParts(st.fileName(), length, partSize);
        return BlobFetchRequest.builder().directory(st.localDirectory()).fileName(st.fileName()).blobParts(parts).atomicWrite(true).build();
    }

    static List<BlobFetchRequest.BlobPart> buildParts(String name, long length, long partSize) {
        List<BlobFetchRequest.BlobPart> parts = new ArrayList<>();
        long offset = 0;
        while (offset < length) {
            long partLen = Math.min(partSize, length - offset);
            parts.add(new BlobFetchRequest.BlobPart(name, offset, partLen));
            offset += partLen;
        }
        return parts;
    }

    /** The local directory's mmap chunk size if it is an MMapDirectory, else Lucene's default. */
    static long maxChunkSize(FSDirectory directory) {
        if (directory instanceof MMapDirectory mmap) {
            return Math.min(mmap.getMaxChunkSize(), MAX_WHOLE_FILE_BYTES);
        }
        return MAX_WHOLE_FILE_BYTES;
    }

    /** The file's length in the local directory, or -1 if there is no such file. */
    private static long lengthOnDisk(FSDirectory localDirectory, String fileName) throws IOException {
        try {
            return localDirectory.fileLength(fileName);
        } catch (NoSuchFileException | FileNotFoundException e) {
            return -1;
        }
    }

    /**
     * True for live cache entries that are never reused or served from, and so are removed before re-downloading: a
     * completed whole-file entry or a switchable family entry. A restore placeholder is handled separately (its file
     * may be reused), and fetchBlobAsync's own placeholder (a private type), in flight or complete, is joined, not
     * removed: a complete one hands back its finished download, which is how a family created by an index reopen gets
     * the whole file its predecessor left in the cache without a second download.
     */
    private static boolean isKnownNonReusableEntry(CachedIndexInput entry) {
        return entry instanceof CachedFullFileIndexInput || entry instanceof CachedSwitchableIndexInput;
    }

    private static void closeQuietly(IndexInput input) {
        if (input == null) {
            return;
        }
        try {
            input.close();
        } catch (IOException e) {
            logger.trace("ignored error closing index input");
        }
    }

}

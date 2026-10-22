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
import org.opensearch.common.util.CancellableThreads;
import org.opensearch.index.store.remote.file.AbstractBlockIndexInput;
import org.opensearch.index.store.remote.filecache.CachedFullFileIndexInput;
import org.opensearch.index.store.remote.filecache.CachedIndexInput;
import org.opensearch.index.store.remote.filecache.FileCache;
import org.opensearch.index.store.remote.utils.BlobFetchRequest;
import org.opensearch.index.store.remote.utils.TransferManager;
import org.opensearch.storage.indexinput.CachedSwitchableIndexInput;
import org.opensearch.storage.warmfullfile.WarmFullFileStats.DeclineReason;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.EnumMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executor;
import java.util.concurrent.Semaphore;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicLongArray;
import java.util.function.LongSupplier;

/**
 * Node-level coordinator for warm-tier whole-file promotion.
 *
 * <p>{@link #promote(FullFileState)} runs on a background executor (never the search thread). It admits a promotion
 * only when a download slot is free and the per-segment whole-file budget allows it, downloads the whole file in
 * parts of {@code partSize} bytes (one block by default) via {@link TransferManager#fetchBlobAsync}, then opens the
 * family's own memory mapping and flips the state to {@link FullFileState.State#FULL}. A whole file already on local
 * disk, such as one left by a previous run of the node, is reused instead of downloaded when its length and checksum
 * match the remote store's. On any decline or failure the state returns to {@link FullFileState.State#BLOCK} with a
 * retry time set. Every promotion that reaches FULL is logged at INFO with its download time, and {@link #stats()}
 * returns the node's promotion and decline counters.
 *
 * @opensearch.experimental
 */
@ExperimentalApi
public class WarmFullFileManager {

    private static final Logger logger = LogManager.getLogger(WarmFullFileManager.class);

    /** Default download part size: one block, the same size the block path reads. */
    static final long DEFAULT_PART_SIZE = AbstractBlockIndexInput.Builder.DEFAULT_BLOCK_SIZE;
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
     * Whether a whole file found on local disk is read end to end and checksummed before it is reused. The length and
     * footer checks alone cannot see corruption in the middle of a file whose footer is intact.
     */
    static final boolean DEFAULT_VERIFY_ENTIRE_FILE = true;

    /**
     * Largest file that may be promoted: strictly below Lucene's default mmap chunk size (16 GiB on 64-bit JVMs), so the
     * family mapping is a single chunk. Real .faiss/.veq files are far below this.
     */
    public static final long MAX_WHOLE_FILE_BYTES = MMapDirectory.DEFAULT_MAX_CHUNK_SIZE;

    /** How long a closed FULL family may keep open handles before a WARN is logged (leak detection). */
    static final long STUCK_HANDLE_WARN_DELAY_MILLIS = 5 * 60_000L;

    private final FileCache fileCache;
    private final Executor executor;
    private final LongSupplier clock;
    private final DelayedScheduler scheduler;
    private final int maxConcurrentDownloads;
    private final Semaphore slots;
    private final int segments;
    private final long perSegmentBudgetBytes;
    private final long partSize;
    private final boolean verifyEntireFile;
    private final AtomicLongArray perSegmentWholeFileBytes;
    private final AtomicLong requested = new AtomicLong();
    private final AtomicLong started = new AtomicLong();
    private final AtomicLong reused = new AtomicLong();
    private final AtomicLong downloadedBytes = new AtomicLong();
    private final AtomicLong promoted = new AtomicLong();
    private final AtomicLong promotedBytes = new AtomicLong();
    private final AtomicLong downloadMillis = new AtomicLong();
    private final AtomicLongArray declinedByReason = new AtomicLongArray(DeclineReason.values().length);
    // Extensions already reported as permanently declined on this node (one WARN per extension, DEBUG per file after).
    private final Set<String> permanentlyDeclinedExtensions = ConcurrentHashMap.newKeySet();
    // Runs on the promotion executor after admission (slot and budget held) and before the first remote read.
    // No-op in production; tests install a blocking hook to hold a download at a known point.
    private volatile Runnable beforeDownloadHook = () -> {};

    /**
     * Runs a task once after a delay (injectable for tests; production uses the node ThreadPool).
     *
     * @opensearch.experimental
     */
    @ExperimentalApi
    @FunctionalInterface
    public interface DelayedScheduler {
        void schedule(Runnable task, long delayMillis);
    }

    /** A scheduler that drops tasks; used where leak warnings are not needed. */
    static final DelayedScheduler NO_OP_SCHEDULER = (task, delayMillis) -> {};

    /**
     * Production constructor.
     *
     * @param fileCache            the node file cache
     * @param executor             the pool to run promote(), the completion handler, and the deferred cleanup on
     *                             (never the search thread, never the FileCache listener thread)
     * @param clock                a monotonic millis clock (e.g. {@code threadPool::relativeTimeInMillis})
     * @param allocatedProcessors  the node's allocated processor count
     * @param scheduler            delayed-task scheduler for the stuck-handle warning
     */
    public WarmFullFileManager(
        FileCache fileCache,
        Executor executor,
        LongSupplier clock,
        int allocatedProcessors,
        DelayedScheduler scheduler
    ) {
        this(
            fileCache,
            executor,
            clock,
            // Download slots: one per eight allocated processors, at least one (4 on a 32 vCPU node).
            Math.max(1, allocatedProcessors / 8),
            DEFAULT_BUDGET_FRACTION,
            DEFAULT_PART_SIZE,
            scheduler
        );
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
        this(fileCache, executor, clock, maxConcurrentDownloads, budgetFraction, partSize, NO_OP_SCHEDULER);
    }

    /** Package-private constructor for tests that also need a scheduler. */
    WarmFullFileManager(
        FileCache fileCache,
        Executor executor,
        LongSupplier clock,
        int maxConcurrentDownloads,
        double budgetFraction,
        long partSize,
        DelayedScheduler scheduler
    ) {
        this(fileCache, executor, clock, maxConcurrentDownloads, budgetFraction, partSize, scheduler, DEFAULT_VERIFY_ENTIRE_FILE);
    }

    /** The constructor every other constructor delegates to. The budget is kept per segment of {@code fileCache}. */
    WarmFullFileManager(
        FileCache fileCache,
        Executor executor,
        LongSupplier clock,
        int maxConcurrentDownloads,
        double budgetFraction,
        long partSize,
        DelayedScheduler scheduler,
        boolean verifyEntireFile
    ) {
        this.fileCache = fileCache;
        this.executor = executor;
        this.clock = clock;
        this.scheduler = scheduler == null ? NO_OP_SCHEDULER : scheduler;
        this.maxConcurrentDownloads = Math.max(1, maxConcurrentDownloads);
        this.slots = new Semaphore(this.maxConcurrentDownloads);
        this.segments = fileCache.segmentCount();
        long perSegmentCapacity = (fileCache.capacity() + this.segments - 1) / this.segments;
        this.perSegmentBudgetBytes = (long) (perSegmentCapacity * budgetFraction);
        this.partSize = partSize > 0 ? partSize : DEFAULT_PART_SIZE;
        this.verifyEntireFile = verifyEntireFile;
        this.perSegmentWholeFileBytes = new AtomicLongArray(this.segments);
    }

    /** @return the executor promote() must be submitted to (never the search thread) */
    public Executor executor() {
        return executor;
    }

    /**
     * Installs a hook that {@link #promote} runs once per download, on the promotion executor, after the slot and the
     * budget are held and before the first remote read. Visible for testing: a blocking hook holds a download at that
     * point so a test can act (close the shard, delete the index) while a promotion is in flight. Not for production use.
     *
     * @param hook the hook; null restores the no-op
     */
    public void setBeforeDownloadHook(Runnable hook) {
        this.beforeDownloadHook = hook == null ? () -> {} : hook;
    }

    /** @return the current monotonic time in millis (for the wrapper's retryAt comparison) */
    public long relativeTimeInMillis() {
        return clock.getAsLong();
    }

    /**
     * Attempts to promote the family whose state is given (already CAS'd to PROMOTING by the trigger). Runs on the
     * background executor. On decline or failure, returns the state to BLOCK and sets retryAt.
     */
    public void promote(FullFileState st) {
        requested.incrementAndGet();
        // The family closed (shard closed, entry evicted or replaced) between the trigger's CAS and this run: nothing
        // to promote, and no slot or budget to spend on a download nobody will use. (A download already running when the
        // family closes is cancelled by closeFamily; its completion handler releases the slot and the budget.)
        if (st.state() != FullFileState.State.PROMOTING) {
            return;
        }
        final long length = st.fileLength();
        final Path path = st.fullFilePath();
        final int segment = fileCache.segmentIndexFor(path);

        // Single-chunk guard. Eligibility already excludes these; this also covers a
        // local directory configured with a smaller chunk size. Checked before taking a slot or reserving bytes.
        if (length >= maxChunkSize(st.localDirectory())) {
            decline(st, DeclineReason.TOO_LARGE, "file of " + length + " bytes exceeds the mmap chunk size");
            return;
        }

        // Admission: a free slot AND the per-segment whole-file budget.
        if (slots.tryAcquire() == false) {
            decline(st, DeclineReason.NO_SLOT, "no free download slot", clock.getAsLong() + NO_SLOT_RETRY_DELAY_MILLIS);
            return;
        }
        boolean bytesReserved = false;
        try {
            if (reserveSegmentBytes(segment, length, perSegmentBudgetBytes) == false) {
                slots.release(); // the slot was taken above; give it back or every budget decline leaks one
                decline(st, DeclineReason.BUDGET_EXCEEDED, "per-segment whole-file budget exceeded");
                return;
            }
            bytesReserved = true;

            // An entry already at the whole-file key: fetchBlobAsync can only drive its own in-flight placeholder (a
            // private type); on any other live entry its asyncLoadIndexInput returns null and leaks the compute
            // reference. A restore placeholder stands for a whole file left on disk by a previous run of the node:
            // fetchBlobAsync replaces it (it reports itself closed) and opens the file without fetching anything, so
            // the file is kept when it validates. Other known entry types are removed; anything else is left for
            // fetchBlobAsync.
            boolean reuse = false;
            CachedIndexInput existing = fileCache.get(path);
            if (existing != null) {
                boolean restored = existing instanceof FileCache.RestoredCachedIndexInput;
                boolean nonReusable = isKnownNonReusableEntry(existing);
                fileCache.decRef(path);
                if (restored) {
                    reuse = validateExisting(st);
                    if (reuse == false) {
                        fileCache.remove(path); // closes the placeholder and deletes the file
                    }
                } else if (nonReusable) {
                    fileCache.remove(path);
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
            final long startedAt = clock.getAsLong();
            if (reuse == false) {
                started.incrementAndGet();
            }
            CompletableFuture<IndexInput> future = st.transferManager().fetchBlobAsync(request, st.downloadCancellation());
            if (future == null) {
                // fetchBlobAsync found a live entry it cannot drive (one put at the key after the check above).
                throw new IllegalStateException("whole-file entry at " + path + " cannot be downloaded into");
            }
            // The future's value is the cache entry's own (non-clone) input; it must NOT be closed here (closing it
            // would close the entry's master input). The family serves from its own mapping opened below.
            final boolean reusedLocalFile = reuse;
            future.whenComplete((indexInput, throwable) -> onDownloadComplete(st, segment, length, startedAt, reusedLocalFile, throwable));
            // Ownership of slot + reserved bytes has passed to the completion handler; do not release them here.
            bytesReserved = false;
        } catch (Exception e) {
            logger.warn("whole-file promotion failed to start for {}", path, e);
            if (bytesReserved) {
                releaseSegmentBytes(segment, length);
            }
            slots.release();
            decline(st, DeclineReason.ERROR, "exception starting download: " + e.getMessage());
        }
    }

    private void onDownloadComplete(
        FullFileState st,
        int segment,
        long length,
        long startedAt,
        boolean reusedLocalFile,
        Throwable throwable
    ) {
        final Path path = st.fullFilePath();
        boolean refTaken = false;
        try {
            if (throwable != null) {
                if (isCancellation(throwable)) {
                    failPromotion(st, segment, length, DeclineReason.FAMILY_CLOSED, "download cancelled: family closed");
                } else {
                    failPromotion(st, segment, length, DeclineReason.DOWNLOAD_FAILED, "download failed: " + throwable.getMessage());
                }
                return;
            }
            if (reusedLocalFile == false) {
                downloadedBytes.addAndGet(length);
            }
            // fetchBlobAsync releases its own reference on success; take the family's reference now.
            CachedIndexInput entry = fileCache.get(path);
            if (entry == null) {
                failPromotion(st, segment, length, DeclineReason.ENTRY_EVICTED, "entry evicted before FULL");
                return;
            }
            refTaken = true;
            FSDirectory localDirectory = st.localDirectory();
            if (localDirectory.fileLength(st.fileName()) != length) {
                // Remove the entry (closes it and deletes the truncated file). Without this, the completed
                // DelayedCreation entry stays live and the next retry reuses the same truncated file and fails again.
                dropEntry(path);
                failPromotion(st, segment, length, DeclineReason.LENGTH_MISMATCH, "downloaded length mismatch");
                return;
            }
            // A reused local file was validated before the entry was created; a fresh download is checked against
            // the remote store's checksum here (its footer only, the bytes were just streamed from the store).
            if (reusedLocalFile == false && matchesRemoteChecksum(st, false) == false) {
                dropEntry(path);
                failPromotion(st, segment, length, DeclineReason.CHECKSUM_MISMATCH, "downloaded checksum mismatch");
                return;
            }
            // Open the family's OWN mapping with the RANDOM hint (do not reuse the cache entry's input).
            IndexInput master = localDirectory.openInput(st.fileName(), IOContext.DEFAULT.withHints(DataAccessHint.RANDOM));
            // The master must be Lucene's mmap input. Views of an NIO master would go through
            // BufferedIndexInput (a copy per read, no k-NN SIMD), which is not the feature. This happens only if the
            // extension is on the local directory's NIO list (hybridfs), so it will not change for this family.
            if ((master instanceof MemorySegmentAccessInput) == false) {
                closeQuietly(master);
                // Drop the useless whole file so it is not charged against the cache until eviction, and so nothing
                // reuses it.
                dropEntry(path);
                declinePermanently(
                    st,
                    segment,
                    length,
                    "the local directory serves it as " + master.getClass().getSimpleName() + ", not mmap"
                );
                return;
            }
            // Attach the teardown, then publish localMaster, then CAS to FULL (all three before the CAS). If the family
            // closes after the CAS, closeFamily() releases the budget and the deferred cleanup unmaps + decRefs.
            st.attachCleanup(executor, () -> releaseSegmentBytes(segment, length), () -> scheduleStuckHandleWarning(st));
            if (st.compareAndSetLocalMaster(null, master) == false) {
                // Another completion handler for this family already installed its mapping (two promote() calls joined
                // the same download). Keep theirs and unwind only what this handler holds.
                closeQuietly(master);
                fileCache.decRef(path);
                releaseAdmission(segment, length);
                logger.debug("whole-file mapping for {} was already published by another completion", path);
                return;
            }
            if (st.casState(FullFileState.State.PROMOTING, FullFileState.State.FULL) == false) {
                // The family closed during the publish (closeFamily saw PROMOTING, so it owes no cleanup). Unwind this
                // handler's own mapping only; never touch a mapping installed by another handler.
                closeQuietly(master);
                st.compareAndSetLocalMaster(master, null);
                fileCache.decRef(path);
                releaseAdmission(segment, length);
                decline(st, DeclineReason.FAMILY_CLOSED, "family closed during publish");
                return;
            }
            slots.release();
            final long elapsedMillis = clock.getAsLong() - startedAt;
            promoted.incrementAndGet();
            promotedBytes.addAndGet(length);
            if (reusedLocalFile) {
                reused.incrementAndGet();
                logger.info("promoted [{}] to a whole file: {} bytes reused from the local copy (cache segment {})", path, length, segment);
            } else {
                downloadMillis.addAndGet(elapsedMillis);
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
        } catch (Exception e) {
            if (refTaken) {
                fileCache.decRef(path); // do not leak the family reference taken above
            }
            failPromotion(st, segment, length, DeclineReason.ERROR, "exception publishing FULL: " + e.getMessage());
        }
    }

    /**
     * Leak detection: if the family closed while handles were still open, warn once after a delay if they are still
     * open. The mapping and the whole file stay until those handles close (never unmapped under a live handle).
     */
    private void scheduleStuckHandleWarning(FullFileState st) {
        try {
            scheduler.schedule(() -> {
                if (st.cleanupDone() == false && st.handleCount() > 0) {
                    logger.warn(
                        "whole-file family [{}] closed {} ms ago but still has [{}] open handle(s); its mapping and "
                            + "whole file are kept until they close. A caller may not be closing an IndexInput.",
                        st.fullFilePath(),
                        STUCK_HANDLE_WARN_DELAY_MILLIS,
                        st.handleCount()
                    );
                }
            }, STUCK_HANDLE_WARN_DELAY_MILLIS);
        } catch (RuntimeException e) {
            logger.debug("could not schedule stuck-handle check for {}", st.fullFilePath(), e);
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
    private void declinePermanently(FullFileState st, int segment, long length, String reason) {
        releaseAdmission(segment, length);
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

    private void failPromotion(FullFileState st, int segment, long length, DeclineReason reason, String detail) {
        releaseAdmission(segment, length);
        decline(st, reason, detail);
    }

    /** Gives back what admission granted a promotion: its reserved budget bytes and its download slot. */
    private void releaseAdmission(int segment, long length) {
        releaseSegmentBytes(segment, length);
        slots.release();
    }

    /** Releases this handler's reference on the whole-file entry and removes the entry, closing it and deleting its file. */
    private void dropEntry(Path path) {
        fileCache.decRef(path);
        fileCache.remove(path);
    }

    /**
     * Whether the whole file on local disk at the family's path may be served as is. Cheapest check first, each
     * failure meaning the file is deleted and downloaded again: the length (writes are sequential, so a complete
     * length means the write finished), then the footer checksum against the remote store's, then, when
     * {@code verifyEntireFile} is set, a checksum of every byte. A file whose remote checksum is unknown is never reused.
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
        return matchesRemoteChecksum(st, verifyEntireFile);
    }

    /**
     * Compares the local whole file's footer checksum against the remote store's, and with {@code entireFile} also
     * checks that every byte adds up to that footer. A file with no known remote checksum passes, there being nothing
     * to compare to; an unreadable, footerless or corrupt file fails.
     */
    private boolean matchesRemoteChecksum(FullFileState st, boolean entireFile) {
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
            if (entireFile) {
                // Throws CorruptIndexException when the bytes do not add up to the footer, which already matched.
                CodecUtil.checksumEntireFile(in);
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
     * Returns the family to BLOCK with the given retry time and counts the decline under {@code reason}. Sets
     * {@code retryAt} BEFORE the CAS so a trigger racing this decline cannot start a new promotion in between.
     *
     * @return true if the family went back to BLOCK; false if it closed meanwhile. A download cancelled by the close
     *         ends here as {@link DeclineReason#FAMILY_CLOSED} and is logged at DEBUG; any other end after a close is
     *         logged at WARN, since that download ran to completion for a family nobody will use
     */
    private boolean decline(FullFileState st, DeclineReason reason, String detail, long retryAt) {
        declinedByReason.incrementAndGet(reason.ordinal());
        st.setRetryAt(retryAt);
        if (st.casState(FullFileState.State.PROMOTING, FullFileState.State.BLOCK)) {
            logger.debug("whole-file promotion declined ({}) for {}", detail, st.fullFilePath());
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
     * A snapshot of this node's promotion counters.
     *
     * @return the current counters
     */
    public WarmFullFileStats stats() {
        Map<DeclineReason, Long> declined = new EnumMap<>(DeclineReason.class);
        for (DeclineReason reason : DeclineReason.values()) {
            declined.put(reason, declinedByReason.get(reason.ordinal()));
        }
        long[] reservedPerSegment = new long[segments];
        for (int i = 0; i < segments; i++) {
            reservedPerSegment[i] = perSegmentWholeFileBytes.get(i);
        }
        return new WarmFullFileStats(
            requested.get(),
            started.get(),
            reused.get(),
            promoted.get(),
            downloadedBytes.get(),
            promotedBytes.get(),
            downloadMillis.get(),
            declined,
            reservedPerSegment,
            perSegmentBudgetBytes,
            maxConcurrentDownloads - slots.availablePermits()
        );
    }

    /** The number of parts a download of {@code length} bytes is split into. */
    private long partCount(long length) {
        return (length + partSize - 1) / partSize;
    }

    /** Builds the download request by hand: ceil(length / partSize) parts at absolute offsets, last clipped, no zero tail. */
    private BlobFetchRequest buildRequest(FullFileState st, long length) {
        List<BlobFetchRequest.BlobPart> parts = buildParts(st.fileName(), length, partSize);
        return BlobFetchRequest.builder().directory(st.localDirectory()).fileName(st.fileName()).blobParts(parts).build();
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

    private boolean reserveSegmentBytes(int segment, long length, long cap) {
        while (true) {
            long current = perSegmentWholeFileBytes.get(segment);
            if (current + length > cap) {
                return false;
            }
            if (perSegmentWholeFileBytes.compareAndSet(segment, current, current + length)) {
                return true;
            }
        }
    }

    private void releaseSegmentBytes(int segment, long length) {
        perSegmentWholeFileBytes.addAndGet(segment, -length);
    }

    /** The local directory's mmap chunk size if it is an MMapDirectory, else Lucene's default. */
    static long maxChunkSize(FSDirectory directory) {
        if (directory instanceof MMapDirectory mmap) {
            return Math.min(mmap.getMaxChunkSize(), MAX_WHOLE_FILE_BYTES);
        }
        return MAX_WHOLE_FILE_BYTES;
    }

    /**
     * True for live cache entries that are never reused or served from, and so are removed before re-downloading: a
     * completed whole-file entry or a switchable family entry. A restore placeholder is handled separately (its file
     * may be reused), and fetchBlobAsync's own in-flight placeholder (a private type) is joined, not removed.
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

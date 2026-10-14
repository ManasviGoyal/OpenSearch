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
import org.apache.lucene.store.DataAccessHint;
import org.apache.lucene.store.FSDirectory;
import org.apache.lucene.store.IOContext;
import org.apache.lucene.store.IndexInput;
import org.apache.lucene.store.MMapDirectory;
import org.apache.lucene.store.MemorySegmentAccessInput;
import org.opensearch.common.annotation.ExperimentalApi;
import org.opensearch.index.store.remote.filecache.CachedIndexInput;
import org.opensearch.index.store.remote.filecache.FileCache;
import org.opensearch.index.store.remote.utils.BlobFetchRequest;
import org.opensearch.index.store.remote.utils.TransferManager;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executor;
import java.util.concurrent.Semaphore;
import java.util.concurrent.atomic.AtomicLongArray;

/**
 * Node-level coordinator for warm-tier whole-file promotion.
 *
 * <p>{@link #promote(FullFileState)} runs on a background executor (never the search thread). It admits a promotion
 * only when a download slot is free and the per-segment whole-file budget allows it, downloads the whole file in
 * hand-built 8 MiB parts via {@link TransferManager#fetchBlobAsync}, then opens the family's own memory mapping and
 * flips the state to {@link FullFileState.State#FULL}. On any decline or failure the state returns to
 * {@link FullFileState.State#BLOCK} with a retry time set.
 *
 * @opensearch.experimental
 */
@ExperimentalApi
public class WarmFullFileManager {

    private static final Logger logger = LogManager.getLogger(WarmFullFileManager.class);

    /** Default download/read part size: one block (8 MiB), matching the block path. */
    static final long DEFAULT_PART_SIZE = 8L * 1024 * 1024;
    /** Retry delay after a decline or failure. */
    static final long RETRY_DELAY_MILLIS = 60_000L;
    /** Whole-file bytes admitted per cache segment must stay at or below this fraction of the segment's capacity. */
    static final double DEFAULT_BUDGET_FRACTION = 0.5;

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
    private final Semaphore slots;
    private final double budgetFraction;
    private final int segments;
    private final long perSegmentCapacity;
    private final long partSize;
    private final AtomicLongArray perSegmentWholeFileBytes;
    // Extensions already reported as permanently declined on this node (one WARN per extension, DEBUG per file after).
    private final Set<String> permanentlyDeclinedExtensions = ConcurrentHashMap.newKeySet();

    /** A monotonic millisecond clock (injectable for tests). */
    @FunctionalInterface
    public interface LongSupplier {
        long getAsLong();
    }

    /** Runs a task once after a delay (injectable for tests; production uses the node ThreadPool). */
    @FunctionalInterface
    public interface DelayedScheduler {
        void schedule(Runnable task, long delayMillis);
    }

    /** A scheduler that drops tasks; used where leak warnings are not needed. */
    static final DelayedScheduler NO_OP_SCHEDULER = (task, delayMillis) -> {};

    /** Production constructor without leak warnings (no scheduler). */
    public WarmFullFileManager(FileCache fileCache, Executor executor, LongSupplier clock, int allocatedProcessors) {
        this(fileCache, executor, clock, allocatedProcessors, NO_OP_SCHEDULER);
    }

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
            // Segment count must match the node FileCache's SegmentedCache, which NodeCacheService builds with the
            // default concurrency level = Runtime.availableProcessors() (NOT allocatedProcessors, which differs when
            // node.processors is set). A mismatch would charge whole-file bytes to the wrong segment.
            ceilingNextPowerOfTwo(Runtime.getRuntime().availableProcessors()),
            DEFAULT_PART_SIZE,
            scheduler
        );
    }

    /** Package-private constructor for tests (small slot count, budget fraction, segment count, and part size). */
    WarmFullFileManager(
        FileCache fileCache,
        Executor executor,
        LongSupplier clock,
        int maxConcurrentDownloads,
        double budgetFraction,
        int segments,
        long partSize
    ) {
        this(fileCache, executor, clock, maxConcurrentDownloads, budgetFraction, segments, partSize, NO_OP_SCHEDULER);
    }

    /** Package-private constructor for tests that also inject the scheduler. */
    WarmFullFileManager(
        FileCache fileCache,
        Executor executor,
        LongSupplier clock,
        int maxConcurrentDownloads,
        double budgetFraction,
        int segments,
        long partSize,
        DelayedScheduler scheduler
    ) {
        this.fileCache = fileCache;
        this.executor = executor;
        this.clock = clock;
        this.scheduler = scheduler == null ? NO_OP_SCHEDULER : scheduler;
        this.slots = new Semaphore(Math.max(1, maxConcurrentDownloads));
        this.budgetFraction = budgetFraction;
        this.segments = Math.max(1, segments);
        this.perSegmentCapacity = (fileCache.capacity() + this.segments - 1) / this.segments;
        this.partSize = partSize > 0 ? partSize : DEFAULT_PART_SIZE;
        this.perSegmentWholeFileBytes = new AtomicLongArray(this.segments);
    }

    /** @return the executor promote() must be submitted to (never the search thread) */
    public Executor executor() {
        return executor;
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
        // The family closed (shard closed, entry evicted or replaced) between the trigger's CAS and this run: nothing
        // to promote, and no slot or budget to spend on a download nobody will use. (A download already running when the
        // shard closes cannot be cancelled, fetchBlobAsync has no hook for it; the completion handler tidies that up.)
        if (st.state() != FullFileState.State.PROMOTING) {
            return;
        }
        final long length = st.fileLength();
        final Path path = st.fullFilePath();
        final int segment = segmentIndexFor(path);
        final long cap = (long) (perSegmentCapacity * budgetFraction);

        // Single-chunk guard. Eligibility already excludes these; this also covers a
        // local directory configured with a smaller chunk size. Checked before taking a slot or reserving bytes.
        if (length >= maxChunkSize(st.localDirectory())) {
            decline(st, "file of " + length + " bytes exceeds the mmap chunk size");
            return;
        }

        // Admission: a free slot AND the per-segment whole-file budget.
        if (slots.tryAcquire() == false) {
            decline(st, "no free download slot");
            return;
        }
        boolean bytesReserved = false;
        try {
            if (reserveSegmentBytes(segment, length, cap) == false) {
                slots.release(); // the slot was taken above; give it back or every budget decline leaks one
                decline(st, "per-segment whole-file budget exceeded");
                return;
            }
            bytesReserved = true;

            // Handle any pre-existing entry at the whole-file key before downloading. fetchBlobAsync can only drive
            // its own in-flight placeholder (DelayedCreationCachedIndexInput, a private type); on any other live
            // entry its asyncLoadIndexInput default returns null and leaks the compute ref. We cannot name that
            // private type, so we remove any live entry of a KNOWN non-reusable type and leave anything else (the
            // placeholder) for fetchBlobAsync to reuse.
            CachedIndexInput existing = fileCache.get(path);
            if (existing != null) {
                boolean nonReusable = isKnownNonReusableEntry(existing);
                fileCache.decRef(path);
                if (nonReusable) {
                    fileCache.remove(path);
                }
            } else if (Files.exists(path)) {
                // Stray leftover file with no cache entry (e.g. crash leftover); remove and re-download.
                Files.delete(path);
            }

            BlobFetchRequest request = buildRequest(st, length);
            CompletableFuture<IndexInput> future = st.transferManager().fetchBlobAsync(request);
            // Ownership of slot + reserved bytes passes to the completion handler; do not release them here.
            bytesReserved = false;
            // The future's value is the cache entry's own (non-clone) input; it must NOT be closed here (closing it
            // would close the entry's master input). The family serves from its own mapping opened below.
            future.whenComplete((indexInput, throwable) -> onDownloadComplete(st, segment, length, throwable));
        } catch (Exception e) {
            logger.warn("whole-file promotion failed to start for {}", path, e);
            if (bytesReserved) {
                releaseSegmentBytes(segment, length);
            }
            slots.release();
            decline(st, "exception starting download: " + e.getMessage());
        }
    }

    private void onDownloadComplete(FullFileState st, int segment, long length, Throwable throwable) {
        final Path path = st.fullFilePath();
        boolean refTaken = false;
        try {
            if (throwable != null) {
                failPromotion(st, segment, length, "download failed: " + throwable.getMessage());
                return;
            }
            // fetchBlobAsync releases its own reference on success; take the family's reference now.
            CachedIndexInput entry = fileCache.get(path);
            if (entry == null) {
                failPromotion(st, segment, length, "entry evicted before FULL");
                return;
            }
            refTaken = true;
            FSDirectory localDirectory = st.localDirectory();
            if (localDirectory.fileLength(st.fileName()) != length) {
                fileCache.decRef(path);
                // Remove the entry (closes it and deletes the truncated file). Without this, the completed
                // DelayedCreation entry stays live and the next retry reuses the same truncated file and fails again.
                fileCache.remove(path);
                failPromotion(st, segment, length, "downloaded length mismatch");
                return;
            }
            // Open the family's OWN mapping with the RANDOM hint (do not reuse the cache entry's input).
            IndexInput master = localDirectory.openInput(st.fileName(), IOContext.DEFAULT.withHints(DataAccessHint.RANDOM));
            // The master must be Lucene's mmap input. Views of an NIO master would go through
            // BufferedIndexInput (a copy per read, no k-NN SIMD), which is not the feature. This happens only if the
            // extension is on the local directory's NIO list (hybridfs), so it will not change for this family.
            if ((master instanceof MemorySegmentAccessInput) == false) {
                closeQuietly(master);
                fileCache.decRef(path);
                // Drop the useless whole file so it is not charged against the cache until eviction, and so nothing
                // reuses it.
                fileCache.remove(path);
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
            st.setLocalMaster(master);
            if (st.casState(FullFileState.State.PROMOTING, FullFileState.State.FULL) == false) {
                // Family closed while we were publishing (closeFamily saw PROMOTING, so it owes no cleanup); unwind.
                closeQuietly(master);
                st.setLocalMaster(null);
                fileCache.decRef(path);
                failPromotion(st, segment, length, "family closed during publish");
                return;
            }
            slots.release();
            logger.debug("promoted whole file to FULL: {}", path);
        } catch (Exception e) {
            if (refTaken) {
                fileCache.decRef(path); // do not leak the family reference taken above
            }
            failPromotion(st, segment, length, "exception publishing FULL: " + e.getMessage());
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
     * later files of that extension are logged at DEBUG by {@link #decline(FullFileState, String, long)}.
     */
    private void declinePermanently(FullFileState st, int segment, long length, String reason) {
        releaseSegmentBytes(segment, length);
        slots.release();
        if (decline(st, reason, Long.MAX_VALUE)) {
            String extension = extensionOf(st.fileName());
            if (permanentlyDeclinedExtensions.add(extension)) {
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

    private void failPromotion(FullFileState st, int segment, long length, String reason) {
        releaseSegmentBytes(segment, length);
        slots.release();
        decline(st, reason);
    }

    private void decline(FullFileState st, String reason) {
        decline(st, reason, clock.getAsLong() + RETRY_DELAY_MILLIS);
    }

    /**
     * Returns the family to BLOCK with the given retry time. Sets {@code retryAt} BEFORE the CAS so a trigger racing
     * this decline cannot start a new promotion in between.
     *
     * @return true if the family went back to BLOCK; false if it closed meanwhile (then a WARN is logged:
     *         the slot and bytes were held for the whole download because fetchBlobAsync cannot be cancelled)
     */
    private boolean decline(FullFileState st, String reason, long retryAt) {
        st.setRetryAt(retryAt);
        if (st.casState(FullFileState.State.PROMOTING, FullFileState.State.BLOCK)) {
            logger.debug("whole-file promotion declined ({}) for {}", reason, st.fullFilePath());
            return true;
        }
        if (st.state() == FullFileState.State.CLOSED) {
            logger.warn("whole-file download for [{}] completed after its family closed ({})", st.fullFilePath(), reason);
        }
        return false;
    }

    /** The part of the name after the last dot (no leading dot); the whole name if it has no dot. */
    static String extensionOf(String name) {
        int dot = name.lastIndexOf('.');
        return dot < 0 ? name : name.substring(dot + 1);
    }

    /** Builds the download request by hand: ceil(length / partSize) parts at absolute offsets, last clipped, no zero tail. */
    BlobFetchRequest buildRequest(FullFileState st, long length) {
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

    void releaseSegmentBytes(int segment, long length) {
        perSegmentWholeFileBytes.addAndGet(segment, -length);
    }

    /**
     * Replicates {@code SegmentedCache.segmentFor} for a path key: the same hash mix and mask, with this manager's
     * segment count ({@code ceilingNextPowerOfTwo(availableProcessors())} in production). Kept in sync with
     * SegmentedCache by {@code WarmFullFileSegmentIndexTests} (in the cache's package, where {@code segmentFor} is
     * visible): two paths share a cache segment exactly when they get the same index here, for several segment counts,
     * and the production segment count equals a default-concurrency FileCache's. The budget only needs that bijection
     * (every counter has the same cap), not that index {@code i} is table slot {@code i}.
     */
    int segmentIndexFor(Path path) {
        return segmentIndexFor(path, segments);
    }

    /**
     * The segment index of {@code path} in a {@code SegmentedCache} with {@code segments} segments (a power of two):
     * the same hash mix and mask as {@code SegmentedCache.segmentFor}. Public and static only so the equivalence test
     * can live next to {@code SegmentedCache}.
     *
     * @param path     the FileCache key
     * @param segments the cache's segment count (a power of two)
     * @return the segment index in {@code [0, segments)}
     */
    public static int segmentIndexFor(Path path, int segments) {
        int h = path.hashCode();
        h = ((h >>> 16) ^ h) * 0x45d9f3b;
        h = ((h >>> 16) ^ h) * 0x45d9f3b;
        h = (h >>> 16) ^ h;
        return h & 0x7fffffff & (segments - 1);
    }

    /** @return the number of cache segments this manager budgets over (visible for testing) */
    public int segments() {
        return segments;
    }

    long perSegmentWholeFileBytes(int segment) {
        return perSegmentWholeFileBytes.get(segment);
    }

    /** The local directory's mmap chunk size if it is an MMapDirectory, else Lucene's default. */
    static long maxChunkSize(FSDirectory directory) {
        if (directory instanceof MMapDirectory mmap) {
            return Math.min(mmap.getMaxChunkSize(), MAX_WHOLE_FILE_BYTES);
        }
        return MAX_WHOLE_FILE_BYTES;
    }

    /**
     * True for live cache entries we must never reuse or serve from (and therefore remove before re-downloading): a
     * completed whole-file entry, a restore-placeholder, or a switchable family entry. The one entry we DO reuse is
     * fetchBlobAsync's own in-flight placeholder (a private type), which is not in this set.
     */
    private static boolean isKnownNonReusableEntry(CachedIndexInput entry) {
        return entry instanceof org.opensearch.index.store.remote.filecache.CachedFullFileIndexInput
            || entry instanceof org.opensearch.index.store.remote.filecache.FileCache.RestoredCachedIndexInput
            || entry instanceof org.opensearch.storage.indexinput.CachedSwitchableIndexInput;
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

    private static int ceilingNextPowerOfTwo(int x) {
        return 1 << (Integer.SIZE - Integer.numberOfLeadingZeros(Math.max(1, x) - 1));
    }
}

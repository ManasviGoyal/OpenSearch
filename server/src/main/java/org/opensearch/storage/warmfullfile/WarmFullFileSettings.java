/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */
package org.opensearch.storage.warmfullfile;

import org.opensearch.common.annotation.ExperimentalApi;
import org.opensearch.common.settings.ClusterSettings;
import org.opensearch.common.settings.Setting;
import org.opensearch.common.unit.TimeValue;
import org.opensearch.common.util.concurrent.OpenSearchExecutors;

import java.util.List;
import java.util.Locale;
import java.util.Set;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.function.IntConsumer;
import java.util.function.LongConsumer;
import java.util.stream.Collectors;

/**
 * Node-level settings controlling the warm-tier whole-file feature.
 *
 * When a warm-tier search derives a clone or slice from a file whose extension is listed in
 * {@link #EXTENSIONS}, the node may download the whole file in the background and serve subsequent
 * reads from a single local memory mapping instead of 8 MiB blocks. The list defaults to the k-NN graph and
 * code files; an empty list disables the feature. The feature is gated behind the writable-warm feature flag
 * at the point where it is constructed.
 *
 * <p>Removing an extension is not an emergency lever. It stops new promotions and sends new reads of the already
 * promoted files back to blocks, but it frees nothing on its own: each file's mapping, its whole file in the cache and
 * its budget stay until the searchers holding that file close their handles, which happens when its segment is merged
 * away, the shard relocates or the index is closed. Disk and cache pressure are relieved by those events, not by the
 * setting.
 *
 * @opensearch.experimental
 */
@ExperimentalApi
public class WarmFullFileSettings {

    /**
     * Node-level, dynamic setting listing the file extensions eligible for whole-file promotion.
     * Values are the extension only, without a leading dot (for example {@code faiss}, {@code veq}).
     * Defaults to the k-NN graph and code files. Setting an empty list disables the feature; removing the setting
     * restores the default. Taking an extension off the list frees no disk, cache or budget until the readers holding
     * its files close their handles (see the class comment).
     */
    public static final Setting<List<String>> EXTENSIONS = Setting.listSetting(
        "tiering.service.full_file.extensions",
        List.of("faiss", "veq"),
        Function.identity(),
        Setting.Property.Dynamic,
        Setting.Property.NodeScope
    );

    /**
     * Node-level, dynamic setting for how many whole-file downloads may run at once on this node. Each download holds
     * one {@code remote_recovery} thread for a serial multi-GB read, so this is a node-wide cap, separate from
     * {@code indices.recovery.max_concurrent_remote_store_streams}, which bounds the files one shard recovery downloads
     * at a time. Defaults to one per eight allocated processors, at least one (4 on a 32 vCPU node). Lowering it does
     * not stop a download in flight; no new one is admitted until enough have finished.
     */
    public static final Setting<Integer> MAX_CONCURRENT_DOWNLOADS = new Setting<>(
        "tiering.service.full_file.max_concurrent_downloads",
        s -> Integer.toString(Math.max(1, OpenSearchExecutors.allocatedProcessors(s) / 8)),
        s -> Setting.parseInt(s, 1, "tiering.service.full_file.max_concurrent_downloads"),
        Setting.Property.Dynamic,
        Setting.Property.NodeScope
    );

    /**
     * The extensions whose promoted whole files may be evicted under budget pressure while their segment reader
     * stays open (see {@link #EVICTION_MIN_IDLE}). Fixed in code rather than a setting, because it is a property of
     * the readers, not a tuning choice: eviction releases a mapping under an open reader, which is safe only for
     * files whose readers take a fresh clone or slice of the handle for every query, and would make a later search
     * read a released mapping for files whose readers keep a clone per thread for the reader's life, such as stored
     * fields and term vectors. The k-NN graph and code files and Lucene's doc values files qualify.
     */
    public static final Set<String> EVICTABLE_EXTENSIONS = Set.of("faiss", "veq", "dvd");

    /**
     * Node-level, dynamic setting: a promoted whole file of an {@link #EVICTABLE_EXTENSIONS evictable extension} is
     * evicted under budget pressure only once no whole-file view of it has been cut for this long, so a file searched
     * recently is kept and the promotion is declined instead. When a promotion in a file cache segment is declined
     * for budget, the FULL file in that segment idle longest is demoted, and its mapping, whole file and budget are
     * released once the searches running at that moment have ended, under its open reader.
     */
    public static final Setting<TimeValue> EVICTION_MIN_IDLE = Setting.timeSetting(
        "tiering.service.full_file.eviction.min_idle",
        TimeValue.timeValueMinutes(5),
        TimeValue.ZERO,
        Setting.Property.Dynamic,
        Setting.Property.NodeScope
    );

    private volatile Set<String> extensions;
    private final List<Consumer<Set<String>>> listeners = new CopyOnWriteArrayList<>();
    private volatile int maxConcurrentDownloads;
    private final List<IntConsumer> maxConcurrentDownloadsListeners = new CopyOnWriteArrayList<>();
    private volatile long evictionMinIdleMillis;
    private final List<LongConsumer> evictionMinIdleListeners = new CopyOnWriteArrayList<>();

    /**
     * Creates a new WarmFullFileSettings instance and registers the dynamic update consumers.
     * @param clusterSettings the cluster settings
     */
    public WarmFullFileSettings(ClusterSettings clusterSettings) {
        setExtensions(clusterSettings.get(EXTENSIONS));
        clusterSettings.addSettingsUpdateConsumer(EXTENSIONS, this::setExtensions);
        setMaxConcurrentDownloads(clusterSettings.get(MAX_CONCURRENT_DOWNLOADS));
        clusterSettings.addSettingsUpdateConsumer(MAX_CONCURRENT_DOWNLOADS, this::setMaxConcurrentDownloads);
        setEvictionMinIdle(clusterSettings.get(EVICTION_MIN_IDLE));
        clusterSettings.addSettingsUpdateConsumer(EVICTION_MIN_IDLE, this::setEvictionMinIdle);
    }

    /**
     * Normalizes and stores the eligible extensions (lower-cased, leading dot stripped, blanks removed).
     * @param values the configured extension list
     */
    public void setExtensions(List<String> values) {
        this.extensions = normalize(values);
        for (Consumer<Set<String>> listener : listeners) {
            listener.accept(extensions);
        }
    }

    /**
     * Registers a listener for the eligible extensions, called once now with the current set and again after every
     * update, with the normalized set.
     * @param listener receives the eligible extensions
     */
    /** Lower-cases, strips and drops blanks and leading dots, as {@link #getExtensions()} documents. */
    private static Set<String> normalize(List<String> values) {
        return values.stream()
            .filter(v -> v != null && v.isBlank() == false)
            .map(v -> v.strip().toLowerCase(Locale.ROOT))
            .map(v -> v.startsWith(".") ? v.substring(1) : v)
            .filter(v -> v.isEmpty() == false)
            .collect(Collectors.toUnmodifiableSet());
    }

    public void addListener(Consumer<Set<String>> listener) {
        listeners.add(listener);
        listener.accept(extensions);
    }

    /**
     * Stores the download cap and tells every listener.
     * @param value the configured cap, at least one
     */
    public void setMaxConcurrentDownloads(int value) {
        this.maxConcurrentDownloads = value;
        for (IntConsumer listener : maxConcurrentDownloadsListeners) {
            listener.accept(value);
        }
    }

    /**
     * Registers a listener for the download cap, called once now with the current value and again after every update.
     * @param listener receives the cap
     */
    public void addMaxConcurrentDownloadsListener(IntConsumer listener) {
        maxConcurrentDownloadsListeners.add(listener);
        listener.accept(maxConcurrentDownloads);
    }

    /**
     * @return how many whole-file downloads may run at once on this node
     */
    public int getMaxConcurrentDownloads() {
        return maxConcurrentDownloads;
    }

    /** Applies a new minimum idle time for eviction and tells the listeners, in milliseconds. */
    public void setEvictionMinIdle(TimeValue value) {
        this.evictionMinIdleMillis = value.millis();
        for (LongConsumer listener : evictionMinIdleListeners) {
            listener.accept(evictionMinIdleMillis);
        }
    }

    /** Registers a listener for the eviction minimum idle time and calls it with the current value, in milliseconds. */
    public void addEvictionMinIdleListener(LongConsumer listener) {
        evictionMinIdleListeners.add(listener);
        listener.accept(evictionMinIdleMillis);
    }

    public long getEvictionMinIdleMillis() {
        return evictionMinIdleMillis;
    }

    /**
     * Returns the current set of eligible extensions (normalized, no leading dot).
     * @return the eligible extensions; empty when the feature is disabled
     */
    public Set<String> getExtensions() {
        return extensions;
    }

    /**
     * Returns whether whole-file promotion is enabled for the given extension.
     * @param extension the file extension to check (without a leading dot)
     * @return true if the extension is eligible
     */
    public boolean isExtensionEligible(String extension) {
        if (extension == null) {
            return false;
        }
        return extensions.contains(extension.toLowerCase(Locale.ROOT));
    }

    /**
     * The extension of a file name: the part after the last dot, without the dot.
     * @param name the file name
     * @return the extension, or null when the name has no dot or ends with one
     */
    public static String extensionOf(String name) {
        int dot = name.lastIndexOf('.');
        return dot < 0 || dot == name.length() - 1 ? null : name.substring(dot + 1);
    }
}

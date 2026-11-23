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
import org.opensearch.common.util.concurrent.OpenSearchExecutors;

import java.util.List;
import java.util.Locale;
import java.util.Set;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.function.Consumer;
import java.util.function.DoubleConsumer;
import java.util.function.Function;
import java.util.function.IntConsumer;
import java.util.function.IntSupplier;
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
 * <p>A promoted whole file is an ordinary file cache entry, referenced only while a search reads it and evicted by
 * the cache like a block when it is not. Removing an extension stops new promotions and demotes the files already
 * promoted: new reads go to blocks at once, and each file's mapping, whole file and budget are released once the
 * searches reading it at that moment end.
 *
 * @opensearch.experimental
 */
@ExperimentalApi
public class WarmFullFileSettings {

    /**
     * Node-level, dynamic setting listing the file extensions eligible for whole-file promotion.
     * Values are the extension only, without a leading dot (for example {@code faiss}, {@code veq}).
     * Defaults to the k-NN graph and code files. Setting an empty list disables the feature; removing the setting
     * restores the default. Taking an extension off the list demotes its promoted files (see the class comment).
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
     * at a time. Defaults to one per eight allocated processors, at least one (4 on a 32 vCPU node). A value above the
     * size of the {@code remote_recovery} pool is applied as that size, since no more downloads than that can run.
     * Lowering it does not stop a download in flight; no new one is admitted until enough have finished.
     */
    public static final Setting<Integer> MAX_CONCURRENT_DOWNLOADS = new Setting<>(
        "tiering.service.full_file.max_concurrent_downloads",
        s -> Integer.toString(Math.max(1, OpenSearchExecutors.allocatedProcessors(s) / 8)),
        s -> Setting.parseInt(s, 1, "tiering.service.full_file.max_concurrent_downloads"),
        Setting.Property.Dynamic,
        Setting.Property.NodeScope
    );

    /**
     * Node-level, dynamic setting: the fraction of each file cache segment's capacity that promoted whole files may
     * occupy. A promotion that would take its segment over the fraction is declined and retried later. The cache's own
     * eviction decides which idle whole files leave; this cap only keeps a burst of promotions from crowding the
     * segment's blocks before it does.
     */
    public static final Setting<Double> BUDGET_FRACTION = Setting.doubleSetting(
        "tiering.service.full_file.budget_fraction",
        0.5,
        0.0,
        1.0,
        Setting.Property.Dynamic,
        Setting.Property.NodeScope
    );

    private volatile Set<String> extensions;
    private final List<Consumer<Set<String>>> listeners = new CopyOnWriteArrayList<>();
    private final IntSupplier downloadPoolSize;
    private volatile int maxConcurrentDownloads;
    private final List<IntConsumer> maxConcurrentDownloadsListeners = new CopyOnWriteArrayList<>();
    private volatile double budgetFraction;
    private final List<DoubleConsumer> budgetFractionListeners = new CopyOnWriteArrayList<>();

    /**
     * Creates a new WarmFullFileSettings instance and registers the dynamic update consumers. The download cap is not
     * bounded by a pool size.
     * @param clusterSettings the cluster settings
     */
    public WarmFullFileSettings(ClusterSettings clusterSettings) {
        this(clusterSettings, () -> Integer.MAX_VALUE);
    }

    /**
     * Creates a new WarmFullFileSettings instance and registers the dynamic update consumers.
     * @param clusterSettings  the cluster settings
     * @param downloadPoolSize the size of the pool whole-file downloads run on, which bounds the download cap
     */
    public WarmFullFileSettings(ClusterSettings clusterSettings, IntSupplier downloadPoolSize) {
        this.downloadPoolSize = downloadPoolSize;
        setExtensions(clusterSettings.get(EXTENSIONS));
        clusterSettings.addSettingsUpdateConsumer(EXTENSIONS, this::setExtensions);
        setMaxConcurrentDownloads(clusterSettings.get(MAX_CONCURRENT_DOWNLOADS));
        clusterSettings.addSettingsUpdateConsumer(MAX_CONCURRENT_DOWNLOADS, this::setMaxConcurrentDownloads);
        setBudgetFraction(clusterSettings.get(BUDGET_FRACTION));
        clusterSettings.addSettingsUpdateConsumer(BUDGET_FRACTION, this::setBudgetFraction);
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
     * Stores the download cap, bounded by the size of the download pool, and tells every listener.
     * @param value the configured cap, at least one
     */
    public void setMaxConcurrentDownloads(int value) {
        final int cap = Math.min(value, Math.max(1, downloadPoolSize.getAsInt()));
        this.maxConcurrentDownloads = cap;
        for (IntConsumer listener : maxConcurrentDownloadsListeners) {
            listener.accept(cap);
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
     * @return how many whole-file downloads may run at once on this node: the configured cap, or the size of the
     *         download pool if that is smaller
     */
    public int getMaxConcurrentDownloads() {
        return maxConcurrentDownloads;
    }

    /** Applies a new whole-file budget fraction and tells the listeners. */
    public void setBudgetFraction(Double value) {
        this.budgetFraction = value;
        for (DoubleConsumer listener : budgetFractionListeners) {
            listener.accept(budgetFraction);
        }
    }

    /** Registers a listener for the whole-file budget fraction and calls it with the current value. */
    public void addBudgetFractionListener(DoubleConsumer listener) {
        budgetFractionListeners.add(listener);
        listener.accept(budgetFraction);
    }

    public double getBudgetFraction() {
        return budgetFraction;
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

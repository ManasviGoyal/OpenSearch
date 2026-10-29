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

import java.util.List;
import java.util.Locale;
import java.util.Set;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.stream.Collectors;

/**
 * Node-level settings controlling the warm-tier whole-file feature.
 *
 * When a warm-tier search derives a clone or slice from a file whose extension is listed in
 * {@link #EXTENSIONS}, the node may download the whole file in the background and serve subsequent
 * reads from a single local memory mapping instead of 8 MiB blocks. The feature is off by default
 * (empty extension list) and is gated behind the writable-warm feature flag at the point where it
 * is constructed.
 *
 * @opensearch.experimental
 */
@ExperimentalApi
public class WarmFullFileSettings {

    /**
     * Node-level, dynamic setting listing the file extensions eligible for whole-file promotion.
     * Values are the extension only, without a leading dot (for example {@code faiss}, {@code veq}).
     * Defaults to the k-NN graph files, standalone and compound-segment ({@code faissc}), and the code files. Setting an empty list disables the feature; removing the setting
     * restores the default.
     */
    public static final Setting<List<String>> EXTENSIONS = Setting.listSetting(
        "tiering.service.full_file.extensions",
        List.of("faiss", "faissc", "veq"),
        Function.identity(),
        Setting.Property.Dynamic,
        Setting.Property.NodeScope
    );

    private volatile Set<String> extensions;
    private final List<Consumer<Set<String>>> listeners = new CopyOnWriteArrayList<>();

    /**
     * Creates a new WarmFullFileSettings instance and registers a dynamic update consumer.
     * @param clusterSettings the cluster settings
     */
    public WarmFullFileSettings(ClusterSettings clusterSettings) {
        setExtensions(clusterSettings.get(EXTENSIONS));
        clusterSettings.addSettingsUpdateConsumer(EXTENSIONS, this::setExtensions);
    }

    /**
     * Normalizes and stores the eligible extensions (lower-cased, leading dot stripped, blanks removed).
     * @param values the configured extension list
     */
    public void setExtensions(List<String> values) {
        this.extensions = values.stream()
            .filter(v -> v != null && v.isBlank() == false)
            .map(v -> v.strip().toLowerCase(Locale.ROOT))
            .map(v -> v.startsWith(".") ? v.substring(1) : v)
            .filter(v -> v.isEmpty() == false)
            .collect(Collectors.toUnmodifiableSet());
        for (Consumer<Set<String>> listener : listeners) {
            listener.accept(extensions);
        }
    }

    /**
     * Registers a listener for the eligible extensions, called once now with the current set and again after every
     * update, with the normalized set.
     * @param listener receives the eligible extensions
     */
    public void addListener(Consumer<Set<String>> listener) {
        listeners.add(listener);
        listener.accept(extensions);
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

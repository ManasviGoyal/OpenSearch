/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.storage.warmfullfile;

import java.util.HashSet;
import java.util.Set;

/**
 * Lets a test promote files of extensions other than the k-NN graph and code files, which the server cannot write
 * without the k-NN plugin. The override applies to every node in the JVM, so a test that uses it restores the real
 * extensions when it is done.
 */
public final class PromotableExtensionsOverride {

    private PromotableExtensionsOverride() {}

    /** Makes {@code extensions} promotable as well as the real ones, which the setting lists by default. */
    public static void use(String... extensions) {
        Set<String> all = new HashSet<>(WarmFullFileSettings.PROMOTABLE_EXTENSIONS);
        all.addAll(Set.of(extensions));
        WarmFullFileSettings.setPromotableExtensions(all);
    }

    /** Restores {@link WarmFullFileSettings#PROMOTABLE_EXTENSIONS}. */
    public static void restore() {
        WarmFullFileSettings.setPromotableExtensions(null);
    }
}

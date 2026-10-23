/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */
package org.opensearch.storage.warmfullfile;

import org.opensearch.cluster.service.ClusterService;
import org.opensearch.common.settings.ClusterSettings;
import org.opensearch.common.settings.Settings;
import org.opensearch.test.ClusterServiceUtils;
import org.opensearch.test.OpenSearchTestCase;
import org.opensearch.threadpool.TestThreadPool;
import org.opensearch.threadpool.ThreadPool;
import org.junit.Before;

import java.util.ArrayList;
import java.util.List;
import java.util.Set;

import static org.opensearch.common.settings.ClusterSettings.BUILT_IN_CLUSTER_SETTINGS;

public class WarmFullFileSettingsTests extends OpenSearchTestCase {

    private ClusterService clusterService;
    private ThreadPool threadPool;

    @Before
    public void setUp() throws Exception {
        super.setUp();
        threadPool = new TestThreadPool("WarmFullFileSettingsTests");
        clusterService = ClusterServiceUtils.createClusterService(
            Settings.EMPTY,
            new ClusterSettings(Settings.EMPTY, BUILT_IN_CLUSTER_SETTINGS),
            threadPool
        );
    }

    @Override
    public void tearDown() throws Exception {
        threadPool.shutdownNow();
        super.tearDown();
    }

    public void testGraphAndCodeFilesByDefault() {
        WarmFullFileSettings settings = new WarmFullFileSettings(clusterService.getClusterSettings());
        assertEquals("on by default for the k-NN graph and code files", Set.of("faiss", "veq"), settings.getExtensions());
        assertTrue(settings.isExtensionEligible("faiss"));
        assertTrue(settings.isExtensionEligible("veq"));
        assertFalse("rescoring vectors stay on blocks", settings.isExtensionEligible("vec"));
    }

    public void testRegisteredInBuiltInClusterSettings() {
        assertTrue(
            "WarmFullFileSettings.EXTENSIONS must be registered in BUILT_IN_CLUSTER_SETTINGS",
            BUILT_IN_CLUSTER_SETTINGS.contains(WarmFullFileSettings.EXTENSIONS)
        );
    }

    public void testDynamicUpdateApplies() {
        WarmFullFileSettings settings = new WarmFullFileSettings(clusterService.getClusterSettings());
        assertEquals(Set.of("faiss", "veq"), settings.getExtensions());

        Settings updated = Settings.builder().putList(WarmFullFileSettings.EXTENSIONS.getKey(), List.of("faiss", "veq", "vec")).build();
        clusterService.getClusterSettings().applySettings(updated);

        assertEquals(Set.of("faiss", "veq", "vec"), settings.getExtensions());
        assertTrue(settings.isExtensionEligible("faiss"));
        assertTrue(settings.isExtensionEligible("veq"));
        assertTrue(settings.isExtensionEligible("vec"));
    }

    public void testListenerSeesTheCurrentSetAndEveryUpdate() {
        WarmFullFileSettings settings = new WarmFullFileSettings(clusterService.getClusterSettings());
        List<Set<String>> seen = new ArrayList<>();
        settings.addListener(seen::add);
        assertEquals("called once on registration with the current set", List.of(Set.of("faiss", "veq")), seen);

        clusterService.getClusterSettings()
            .applySettings(Settings.builder().putList(WarmFullFileSettings.EXTENSIONS.getKey(), List.of("faiss", ".VEQ", "vec")).build());
        assertEquals(List.of(Set.of("faiss", "veq"), Set.of("faiss", "veq", "vec")), seen);

        clusterService.getClusterSettings().applySettings(Settings.builder().putNull(WarmFullFileSettings.EXTENSIONS.getKey()).build());
        assertEquals("clearing the setting reverts to the default set", Set.of("faiss", "veq"), seen.get(seen.size() - 1));
    }

    public void testExtensionsAreNormalized() {
        WarmFullFileSettings settings = new WarmFullFileSettings(clusterService.getClusterSettings());
        Settings updated = Settings.builder()
            // leading dots, mixed case, and surrounding whitespace are all normalized away
            .putList(WarmFullFileSettings.EXTENSIONS.getKey(), List.of(".FAISS", "Veq", "  faissc  "))
            .build();
        clusterService.getClusterSettings().applySettings(updated);

        assertEquals(Set.of("faiss", "veq", "faissc"), settings.getExtensions());
        assertTrue(settings.isExtensionEligible("FAISS"));
    }

    public void testExtensionOf() {
        assertEquals("faiss", WarmFullFileSettings.extensionOf("_0.faiss"));
        assertEquals("veq", WarmFullFileSettings.extensionOf("_0_Lucene104_0.veq"));
        assertEquals("only the last extension counts", "vec", WarmFullFileSettings.extensionOf("_0.faiss.vec"));
        assertNull("no dot", WarmFullFileSettings.extensionOf("segments_5"));
        assertNull("trailing dot", WarmFullFileSettings.extensionOf("_0.faiss."));
        assertFalse(
            "a name without an extension is never eligible",
            new WarmFullFileSettings(clusterService.getClusterSettings()).isExtensionEligible(
                WarmFullFileSettings.extensionOf("segments_5")
            )
        );
    }
}

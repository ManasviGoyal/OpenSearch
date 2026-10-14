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
        super.tearDown();
        threadPool.shutdownNow();
    }

    public void testEmptyByDefault() {
        WarmFullFileSettings settings = new WarmFullFileSettings(clusterService.getClusterSettings());
        assertTrue("feature is disabled by default (empty extensions)", settings.getExtensions().isEmpty());
        assertFalse(settings.isExtensionEligible("faiss"));
    }

    public void testRegisteredInBuiltInClusterSettings() {
        assertTrue(
            "WarmFullFileSettings.EXTENSIONS must be registered in BUILT_IN_CLUSTER_SETTINGS",
            BUILT_IN_CLUSTER_SETTINGS.contains(WarmFullFileSettings.EXTENSIONS)
        );
    }

    public void testDynamicUpdateApplies() {
        WarmFullFileSettings settings = new WarmFullFileSettings(clusterService.getClusterSettings());
        assertTrue(settings.getExtensions().isEmpty());

        Settings updated = Settings.builder().putList(WarmFullFileSettings.EXTENSIONS.getKey(), List.of("faiss", "veq")).build();
        clusterService.getClusterSettings().applySettings(updated);

        assertEquals(Set.of("faiss", "veq"), settings.getExtensions());
        assertTrue(settings.isExtensionEligible("faiss"));
        assertTrue(settings.isExtensionEligible("veq"));
        assertFalse(settings.isExtensionEligible("vec"));
    }

    public void testExtensionsAreNormalized() {
        WarmFullFileSettings settings = new WarmFullFileSettings(clusterService.getClusterSettings());
        Settings updated = Settings.builder()
            // leading dots, mixed case, and a blank entry should all be normalized away
            .putList(WarmFullFileSettings.EXTENSIONS.getKey(), List.of(".FAISS", "Veq", "  faissc  "))
            .build();
        clusterService.getClusterSettings().applySettings(updated);

        assertEquals(Set.of("faiss", "veq", "faissc"), settings.getExtensions());
        assertTrue(settings.isExtensionEligible("FAISS"));
    }
}

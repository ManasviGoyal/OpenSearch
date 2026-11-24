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
import org.opensearch.common.settings.Setting;
import org.opensearch.common.settings.Settings;
import org.opensearch.common.unit.TimeValue;
import org.opensearch.common.util.concurrent.OpenSearchExecutors;
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
        assertTrue(
            "WarmFullFileSettings.MAX_CONCURRENT_DOWNLOADS must be registered in BUILT_IN_CLUSTER_SETTINGS",
            BUILT_IN_CLUSTER_SETTINGS.contains(WarmFullFileSettings.MAX_CONCURRENT_DOWNLOADS)
        );
    }

    public void testMaxConcurrentDownloadsDefaultsToOnePerEightProcessorsAtLeastOne() {
        Setting<Integer> setting = WarmFullFileSettings.MAX_CONCURRENT_DOWNLOADS;
        // node.processors above the machine's count logs a deprecation warning, so the many-processor case uses the
        // machine's own count and the formula.
        int allocated = OpenSearchExecutors.allocatedProcessors(Settings.EMPTY);
        assertEquals(Math.max(1, allocated / 8), setting.get(Settings.EMPTY).intValue());
        assertEquals(1, setting.get(Settings.builder().put("node.processors", Math.min(8, allocated)).build()).intValue());
        assertEquals("never zero", 1, setting.get(Settings.builder().put("node.processors", 1).build()).intValue());
        assertEquals("explicit value wins", 7, setting.get(Settings.builder().put(setting.getKey(), 7).build()).intValue());
        IllegalArgumentException e = expectThrows(
            IllegalArgumentException.class,
            () -> setting.get(Settings.builder().put(setting.getKey(), 0).build())
        );
        assertTrue(e.getMessage(), e.getMessage().contains("must be >= 1"));
    }

    public void testMaxConcurrentDownloadsListenerSeesTheCurrentValueAndEveryUpdate() {
        WarmFullFileSettings settings = new WarmFullFileSettings(clusterService.getClusterSettings());
        int initial = settings.getMaxConcurrentDownloads();
        assertTrue(initial >= 1);
        List<Integer> seen = new ArrayList<>();
        settings.addMaxConcurrentDownloadsListener(seen::add);
        assertEquals("called once on registration with the current value", List.of(initial), seen);

        String key = WarmFullFileSettings.MAX_CONCURRENT_DOWNLOADS.getKey();
        clusterService.getClusterSettings().applySettings(Settings.builder().put(key, 2).build());
        clusterService.getClusterSettings().applySettings(Settings.builder().put(key, 9).build());
        assertEquals(List.of(initial, 2, 9), seen);
        assertEquals(9, settings.getMaxConcurrentDownloads());

        clusterService.getClusterSettings().applySettings(Settings.builder().putNull(key).build());
        assertEquals("clearing the setting reverts to the default", initial, settings.getMaxConcurrentDownloads());
    }

    public void testMaxConcurrentDownloadsIsCappedAtTheDownloadPoolSize() {
        // A cap above the pool's size cannot run, so the pool's size is what the listeners and the getter see. A cap
        // at or below it is applied as configured.
        String key = WarmFullFileSettings.MAX_CONCURRENT_DOWNLOADS.getKey();
        clusterService.getClusterSettings().applySettings(Settings.builder().put(key, 50).build());
        WarmFullFileSettings settings = new WarmFullFileSettings(clusterService.getClusterSettings(), () -> 6);
        assertEquals("capped at construction", 6, settings.getMaxConcurrentDownloads());
        List<Integer> seen = new ArrayList<>();
        settings.addMaxConcurrentDownloadsListener(seen::add);

        clusterService.getClusterSettings().applySettings(Settings.builder().put(key, 3).build());
        clusterService.getClusterSettings().applySettings(Settings.builder().put(key, 6).build());
        clusterService.getClusterSettings().applySettings(Settings.builder().put(key, 7).build());
        assertEquals(List.of(6, 3, 6, 6), seen);
        assertEquals(6, settings.getMaxConcurrentDownloads());
    }

    public void testDynamicUpdateApplies() {
        WarmFullFileSettings settings = new WarmFullFileSettings(clusterService.getClusterSettings());
        assertEquals(Set.of("faiss", "veq"), settings.getExtensions());

        Settings off = Settings.builder().putList(WarmFullFileSettings.EXTENSIONS.getKey(), List.of()).build();
        clusterService.getClusterSettings().applySettings(off);
        assertEquals(Set.of(), settings.getExtensions());
        assertFalse(settings.isExtensionEligible("faiss"));
        assertFalse(settings.isExtensionEligible("veq"));

        Settings on = Settings.builder().putList(WarmFullFileSettings.EXTENSIONS.getKey(), List.of("faiss", "veq")).build();
        clusterService.getClusterSettings().applySettings(on);
        assertEquals(Set.of("faiss", "veq"), settings.getExtensions());
        assertTrue(settings.isExtensionEligible("faiss"));
        assertTrue(settings.isExtensionEligible("veq"));
    }

    public void testListenerSeesTheCurrentSetAndEveryUpdate() {
        WarmFullFileSettings settings = new WarmFullFileSettings(clusterService.getClusterSettings());
        List<Set<String>> seen = new ArrayList<>();
        settings.addListener(seen::add);
        assertEquals("called once on registration with the current set", List.of(Set.of("faiss", "veq")), seen);

        clusterService.getClusterSettings()
            .applySettings(Settings.builder().putList(WarmFullFileSettings.EXTENSIONS.getKey(), List.of()).build());
        clusterService.getClusterSettings()
            .applySettings(Settings.builder().putList(WarmFullFileSettings.EXTENSIONS.getKey(), List.of("faiss", ".VEQ")).build());
        assertEquals(List.of(Set.of("faiss", "veq"), Set.of(), Set.of("faiss", "veq")), seen);

        clusterService.getClusterSettings().applySettings(Settings.builder().putNull(WarmFullFileSettings.EXTENSIONS.getKey()).build());
        assertEquals("clearing the setting reverts to the default set", Set.of("faiss", "veq"), seen.get(seen.size() - 1));
    }

    public void testBudgetFractionDefaultsToHalfAndIsRegistered() {
        WarmFullFileSettings settings = new WarmFullFileSettings(clusterService.getClusterSettings());
        assertEquals(0.5, settings.getBudgetFraction(), 0.0);
        assertTrue(BUILT_IN_CLUSTER_SETTINGS.contains(WarmFullFileSettings.BUDGET_FRACTION));
        assertTrue(WarmFullFileSettings.BUDGET_FRACTION.isDynamic());
    }

    public void testBudgetFractionListenerSeesTheCurrentValueAndEveryUpdate() {
        WarmFullFileSettings settings = new WarmFullFileSettings(clusterService.getClusterSettings());
        List<Double> seen = new ArrayList<>();
        settings.addBudgetFractionListener(seen::add);
        assertEquals(List.of(0.5), seen);

        clusterService.getClusterSettings()
            .applySettings(Settings.builder().put(WarmFullFileSettings.BUDGET_FRACTION.getKey(), 0.25).build());
        assertEquals(List.of(0.5, 0.25), seen);
        assertEquals(0.25, settings.getBudgetFraction(), 0.0);

        clusterService.getClusterSettings()
            .applySettings(Settings.builder().putNull(WarmFullFileSettings.BUDGET_FRACTION.getKey()).build());
        assertEquals("clearing the setting reverts to the default", 0.5, settings.getBudgetFraction(), 0.0);
    }

    public void testEvictionBackoffDefaultsToThirtySecondsAndIsRegistered() {
        WarmFullFileSettings settings = new WarmFullFileSettings(clusterService.getClusterSettings());
        assertEquals(TimeValue.timeValueSeconds(30), settings.getEvictionBackoff());
        assertTrue(BUILT_IN_CLUSTER_SETTINGS.contains(WarmFullFileSettings.EVICTION_BACKOFF));
        assertTrue(WarmFullFileSettings.EVICTION_BACKOFF.isDynamic());
    }

    public void testEvictionBackoffListenerSeesTheCurrentValueAndEveryUpdate() {
        WarmFullFileSettings settings = new WarmFullFileSettings(clusterService.getClusterSettings());
        List<TimeValue> seen = new ArrayList<>();
        settings.addEvictionBackoffListener(seen::add);
        assertEquals(List.of(TimeValue.timeValueSeconds(30)), seen);

        String key = WarmFullFileSettings.EVICTION_BACKOFF.getKey();
        clusterService.getClusterSettings().applySettings(Settings.builder().put(key, "0s").build());
        clusterService.getClusterSettings().applySettings(Settings.builder().put(key, "2m").build());
        assertEquals(List.of(TimeValue.timeValueSeconds(30), TimeValue.ZERO, TimeValue.timeValueMinutes(2)), seen);
        assertEquals(TimeValue.timeValueMinutes(2), settings.getEvictionBackoff());

        clusterService.getClusterSettings().applySettings(Settings.builder().putNull(key).build());
        assertEquals("clearing the setting reverts to the default", TimeValue.timeValueSeconds(30), settings.getEvictionBackoff());
    }

    public void testEvictionBackoffCannotBeNegative() {
        expectThrows(
            IllegalArgumentException.class,
            () -> WarmFullFileSettings.EVICTION_BACKOFF.get(
                Settings.builder().put(WarmFullFileSettings.EVICTION_BACKOFF.getKey(), "-1s").build()
            )
        );
    }

    public void testBudgetFractionIsBoundedToTheUnitInterval() {
        expectThrows(
            IllegalArgumentException.class,
            () -> WarmFullFileSettings.BUDGET_FRACTION.get(
                Settings.builder().put(WarmFullFileSettings.BUDGET_FRACTION.getKey(), 1.5).build()
            )
        );
        expectThrows(
            IllegalArgumentException.class,
            () -> WarmFullFileSettings.BUDGET_FRACTION.get(
                Settings.builder().put(WarmFullFileSettings.BUDGET_FRACTION.getKey(), -0.1).build()
            )
        );
    }

    public void testExtensionsAreNormalized() {
        WarmFullFileSettings settings = new WarmFullFileSettings(clusterService.getClusterSettings());
        Settings updated = Settings.builder()
            // leading dots, mixed case, and surrounding whitespace are all normalized away
            .putList(WarmFullFileSettings.EXTENSIONS.getKey(), List.of(".FAISS", "  Veq  "))
            .build();
        clusterService.getClusterSettings().applySettings(updated);

        assertEquals(Set.of("faiss", "veq"), settings.getExtensions());
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

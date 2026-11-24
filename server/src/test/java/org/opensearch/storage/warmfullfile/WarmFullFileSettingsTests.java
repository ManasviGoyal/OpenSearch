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

    public void testOnlyTheKnnGraphAndCodeFilesCanBePromoted() {
        assertEquals(Set.of("faiss", "veq"), WarmFullFileSettings.PROMOTABLE_EXTENSIONS);
        assertTrue(WarmFullFileSettings.isPromotable("faiss"));
        assertTrue(WarmFullFileSettings.isPromotable("veq"));
        assertFalse(WarmFullFileSettings.isPromotable("fdt"));
        assertFalse(WarmFullFileSettings.isPromotable(null));
    }

    public void testTheSettingCanNarrowThePromotableExtensionsAndNotAddToThem() {
        // A list that names an extension which cannot be promoted is rejected as a whole, and what was in force stays.
        WarmFullFileSettings settings = new WarmFullFileSettings(clusterService.getClusterSettings());
        String key = WarmFullFileSettings.EXTENSIONS.getKey();

        clusterService.getClusterSettings().applySettings(Settings.builder().putList(key, List.of("faiss")).build());
        assertEquals("narrowed", Set.of("faiss"), settings.getExtensions());

        IllegalArgumentException e = expectThrows(
            IllegalArgumentException.class,
            () -> WarmFullFileSettings.EXTENSIONS.get(Settings.builder().putList(key, List.of("veq", ".FDT", "dvd")).build())
        );
        assertTrue(e.getMessage(), e.getMessage().contains("[dvd, fdt]"));
        assertTrue(e.getMessage(), e.getMessage().contains("[faiss, veq]"));
        expectThrows(
            IllegalArgumentException.class,
            () -> clusterService.getClusterSettings().applySettings(Settings.builder().putList(key, List.of("fdt")).build())
        );
        assertEquals("the rejected list changed nothing", Set.of("faiss"), settings.getExtensions());
    }

    public void testATestCanAddPromotableExtensionsAndRestoreTheRealOnes() {
        try {
            PromotableExtensionsOverride.use("fdt");
            assertTrue(WarmFullFileSettings.isPromotable("fdt"));
            assertTrue("the real ones stay", WarmFullFileSettings.isPromotable("faiss"));
            WarmFullFileSettings settings = new WarmFullFileSettings(clusterService.getClusterSettings());
            clusterService.getClusterSettings()
                .applySettings(Settings.builder().putList(WarmFullFileSettings.EXTENSIONS.getKey(), List.of("fdt", "faiss")).build());
            assertEquals(Set.of("fdt", "faiss"), settings.getExtensions());
        } finally {
            PromotableExtensionsOverride.restore();
        }
        assertFalse(WarmFullFileSettings.isPromotable("fdt"));
        expectThrows(
            IllegalArgumentException.class,
            () -> WarmFullFileSettings.EXTENSIONS.get(
                Settings.builder().putList(WarmFullFileSettings.EXTENSIONS.getKey(), List.of("fdt")).build()
            )
        );
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

    public void testPromotionRuleDefaultsAndRegistration() {
        WarmFullFileSettings settings = new WarmFullFileSettings(clusterService.getClusterSettings());
        assertEquals(3, settings.getPromotionUses());
        assertEquals(TimeValue.timeValueSeconds(5), settings.getPromotionUseInterval());
        assertEquals(TimeValue.timeValueMinutes(10), settings.getPromotionUseWindow());
        for (Setting<?> setting : List.of(
            WarmFullFileSettings.PROMOTION_USES,
            WarmFullFileSettings.PROMOTION_USE_INTERVAL,
            WarmFullFileSettings.PROMOTION_USE_WINDOW
        )) {
            assertTrue(setting.getKey(), BUILT_IN_CLUSTER_SETTINGS.contains(setting));
            assertTrue(setting.getKey(), setting.isDynamic());
        }
    }

    public void testPromotionRuleListenerSeesTheCurrentRuleAndEveryUpdate() {
        WarmFullFileSettings settings = new WarmFullFileSettings(clusterService.getClusterSettings());
        List<String> seen = new ArrayList<>();
        settings.addPromotionRuleListener((uses, interval, window) -> seen.add(uses + "/" + interval + "/" + window));
        assertEquals("called once on registration with the defaults", List.of("3/5s/10m"), seen);

        clusterService.getClusterSettings().applySettings(Settings.builder().put(WarmFullFileSettings.PROMOTION_USES.getKey(), 1).build());
        clusterService.getClusterSettings()
            .applySettings(
                Settings.builder()
                    .put(WarmFullFileSettings.PROMOTION_USES.getKey(), 1)
                    .put(WarmFullFileSettings.PROMOTION_USE_INTERVAL.getKey(), "1s")
                    .build()
            );
        assertEquals(List.of("3/5s/10m", "1/5s/10m", "1/1s/10m"), seen);
        assertEquals(1, settings.getPromotionUses());
        assertEquals(TimeValue.timeValueSeconds(1), settings.getPromotionUseInterval());
    }

    public void testPromotionUsesIsAtLeastOne() {
        expectThrows(
            IllegalArgumentException.class,
            () -> WarmFullFileSettings.PROMOTION_USES.get(Settings.builder().put(WarmFullFileSettings.PROMOTION_USES.getKey(), 0).build())
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

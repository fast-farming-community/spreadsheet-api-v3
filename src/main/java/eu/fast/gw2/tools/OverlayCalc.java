package eu.fast.gw2.tools;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;

import eu.fast.gw2.dao.CalculationsDao;

public class OverlayCalc {

    /** Cache for calculations by (category|key). Allows null values. */
    private static final Map<String, CalculationsDao.Config> CALC_CACHE = new HashMap<>();
    private static volatile boolean CALC_ALL_PRELOADED = false;
    private static final ThreadLocal<java.util.Set<Integer>> ACTIVE_IGNORES = new ThreadLocal<>();

    static java.util.Set<Integer> activeIgnores() {
        java.util.Set<Integer> s = ACTIVE_IGNORES.get();
        return (s == null) ? java.util.Set.of() : s;
    }

    /** Run a block with a temporary ignore set; always clears after. */
    public static <T> T withIgnoredIds(java.util.Set<Integer> ids, java.util.function.Supplier<T> body) {
        ACTIVE_IGNORES.set((ids == null) ? java.util.Set.of() : java.util.Set.copyOf(ids));
        try {
            return body.get();
        } finally {
            ACTIVE_IGNORES.remove();
        }
    }

    /** Alias so callers can just use preloadAll() */
    public static void preloadAll() {
        preloadAllCalcsIfNeeded();
    }

    /** One-time bulk preload of the latest rows for all (category,key). */
    public static void preloadAllCalcsIfNeeded() {
        if (CALC_ALL_PRELOADED)
            return;
        synchronized (OverlayCalc.class) {
            if (CALC_ALL_PRELOADED)
                return;
            long t0 = System.currentTimeMillis();
            Map<String, CalculationsDao.Config> all = CalculationsDao.findAllLatest();
            CALC_CACHE.putAll(all);
            CALC_ALL_PRELOADED = true;
            System.out.printf(java.util.Locale.ROOT,
                    "Preloaded calculations: %d entries in %.1fs%n",
                    all.size(), (System.currentTimeMillis() - t0) / 1000.0);
        }
    }

    public static CalculationsDao.Config getCalcCfg(String category, String key) {
        String ck = ((category == null ? "" : category.trim().toUpperCase())
                + "|" + (key == null ? "" : key.trim()));

        // Fast path: cache hit
        CalculationsDao.Config cfg = CALC_CACHE.get(ck);
        if (cfg != null)
            return cfg;

        // If we fully preloaded already, absence means "no config".
        if (CALC_ALL_PRELOADED)
            return null;

        // Try direct fetch, then fall back to one-time preload.
        cfg = CalculationsDao.find(category, key);
        if (cfg != null) {
            CALC_CACHE.put(ck, cfg);
            return cfg;
        }

        preloadAllCalcsIfNeeded();
        return CALC_CACHE.get(ck);
    }

    public static int pickTaxesPercent(String category, String key, CalculationsDao.Config tableCfg) {
        // NEW: NEGATIVE rows are never taxed (exact, case-sensitive match)
        if ("NEGATIVE".equals(category))
            return 0;

        if (OverlayHelper.isInternal(category))
            return 0;

        if (category != null && !category.isBlank() && key != null && !key.isBlank())
            return 0; // composite ref

        var rowCfg = getCalcCfg(category, key);
        if (rowCfg != null)
            return OverlayHelper.clampPercent(rowCfg.taxes());

        if (tableCfg != null)
            return OverlayHelper.clampPercent(tableCfg.taxes());

        return 15;
    }

    public static void ensurePricesForIds(Map<Integer, int[]> priceMap, Set<Integer> ids, String tierKey) {
        if (ids == null || ids.isEmpty())
            return;
        java.util.List<Integer> missing = null;
        for (Integer id : ids) {
            if (id == null || id <= 0)
                continue; // guard bad IDs
            if (!priceMap.containsKey(id)) {
                if (missing == null)
                    missing = new ArrayList<>();
                missing.add(id);
            }
        }
        if (missing != null && !missing.isEmpty()) {
            priceMap.putAll(eu.fast.gw2.dao.Gw2PricesDao.loadTier(missing, tierKey));
        }
    }

    /**
     * Get EV for a detail key using LRU cache; ensures prices exist in priceMap.
     */
    public static int[] evForDetail(String refKey, Map<Integer, int[]> priceMap, int taxesPercent, String tierKey) {
        if (refKey == null || refKey.isBlank())
            return new int[] { 0, 0 };
        String ck = tierKey + "|" + taxesPercent + "|" + refKey;
        int[] cached = OverlayCache.getEv(ck);
        if (cached != null)
            return cached;

        var drops = OverlayCache.getDetailRowsCached(refKey);
        ensurePricesForIds(priceMap, OverlayHelper.extractIds(drops), tierKey);

        int[] ev = OverlayHelper.bagEV(drops, priceMap, taxesPercent);
        OverlayCache.putEv(ck, ev);
        return ev;
    }
}

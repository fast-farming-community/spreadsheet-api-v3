package eu.fast.gw2.tools;

import java.util.*;
import eu.fast.gw2.dao.CalculationsDao;

public class OverlayCalc {

    /** Cache for calculations by (category|key). Allows null values. */
    private static final Map<String, CalculationsDao.Config> CALC_CACHE = new HashMap<>();

    public static CalculationsDao.Config getCalcCfg(String category, String key) {
        String ck = ((category == null ? "" : category.trim().toUpperCase()) + "|" + (key == null ? "" : key.trim()));
        if (CALC_CACHE.containsKey(ck))
            return CALC_CACHE.get(ck);
        var cfg = CalculationsDao.find(category, key);
        CALC_CACHE.put(ck, cfg); // may be null
        return cfg;
    }

    public static int pickTaxesPercent(String category, String key, CalculationsDao.Config tableCfg) {
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
        List<Integer> missing = null;
        for (Integer id : ids) {
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

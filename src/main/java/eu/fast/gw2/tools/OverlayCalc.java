package eu.fast.gw2.tools;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import eu.fast.gw2.dao.CalculationsDao;
import eu.fast.gw2.dao.Gw2PricesDao;

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

    /**
     * Resolve aggregation op from public.calculations.operation with safe defaults.
     * - INTERNAL -> MAX (hard rule)
     * - If DB op is "MAX" -> MAX
     * - If DB op is "SUM" (or blank/unknown) -> SUM
     */
    public static String pickAggregationOp(String category, String key) {
        if ("INTERNAL".equalsIgnoreCase(category))
            return "MAX";

        CalculationsDao.Config cfg = getCalcCfg(category, key);
        String op = (cfg == null) ? null : cfg.operation();
        if (op != null) {
            op = op.trim().toUpperCase(java.util.Locale.ROOT);
            if ("MAX".equals(op))
                return "MAX";
            if ("SUM".equals(op))
                return "SUM";
        }
        return "SUM";
    }

    /**
     * Taxes selection (deterministic):
     * - INTERNAL / NEGATIVE / UNCHECKED -> 0%
     * - Else prefer row calc (category|key), else table-level, else 15%
     */
    public static int pickTaxesPercent(String category, String key, CalculationsDao.Config tableCfg) {
        if (category != null) {
            if ("INTERNAL".equalsIgnoreCase(category))
                return 0;
            if ("NEGATIVE".equalsIgnoreCase(category))
                return 0;
            if ("UNCHECKED".equalsIgnoreCase(category))
                return 0; // will be skipped, but safe
        }

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
            priceMap.putAll(Gw2PricesDao.loadTier(missing, tierKey));
        }
    }

    // =====================================================================
    // Recursive EV with caller-provided aggregation (SUM / MAX)
    // =====================================================================

    /** Back-compat overload: defaults to SUM aggregation. */
    public static int[] evForDetail(String refKey, Map<Integer, int[]> priceMap, int taxesPercent, String tierKey) {
        return evForDetail(refKey, priceMap, taxesPercent, tierKey, "SUM");
    }

    /**
     * Compute EV for a referenced detail table:
     * - Walk its rows recursively (composites call back using DB-seeded op).
     * - Collapse THIS table's rows using the provided {@code aggOp}: "SUM" | "MAX".
     * - Returns int[2] = { buyEV, sellEV }.
     */
    public static int[] evForDetail(String refKey,
            Map<Integer, int[]> priceMap,
            int taxesPercent,
            String tierKey,
            String aggOp) {
        if (refKey == null || refKey.isBlank())
            return new int[] { 0, 0 };

        final String op = (aggOp == null || aggOp.isBlank()) ? "SUM" : aggOp.trim().toUpperCase(java.util.Locale.ROOT);
        String ck = tierKey + "|" + taxesPercent + "|" + op + "|" + refKey;
        int[] cached = OverlayCache.getEv(ck);
        if (cached != null)
            return cached;

        List<Map<String, Object>> rows = OverlayCache.getDetailRowsCached(refKey);
        if (rows == null || rows.isEmpty()) {
            int[] ev = new int[] { 0, 0 };
            OverlayCache.putEv(ck, ev);
            return ev;
        }

        // Ensure prices for the immediate leaf items in this table
        ensurePricesForIds(priceMap, OverlayHelper.extractIds(rows), tierKey);

        // Collect per-row EV pairs (already quantized by row's AverageAmount)
        List<int[]> perRow = new ArrayList<>(rows.size());
        for (Map<String, Object> r : rows) {
            int[] pair = evalRowEVPair(r, priceMap, taxesPercent, tierKey);
            if (pair != null)
                perRow.add(pair);
        }

        // Aggregate this table's rows according to aggOp
        int[] result = aggregatePairs(perRow, op);
        OverlayCache.putEv(ck, result);
        return result;
    }

    /** Compute one row's EV pair (buy,sell), including AverageAmount scaling. */
    private static int[] evalRowEVPair(Map<String, Object> row,
            Map<Integer, int[]> priceMap,
            int parentTaxesPercent,
            String tierKey) {
        if (row == null)
            return new int[] { 0, 0 };

        String rawCategory = OverlayHelper.str(row.get(OverlayHelper.COL_CAT));
        String rawKey = OverlayHelper.str(row.get(OverlayHelper.COL_KEY));
        int itemId = OverlayHelper.toInt(row.get(OverlayHelper.COL_ID), -1);

        // Skip UNCHECKED silently
        if ("UNCHECKED".equalsIgnoreCase(rawCategory))
            return new int[] { 0, 0 };

        // NEGATIVE: untaxed negatives × qty
        if ("NEGATIVE".equalsIgnoreCase(rawCategory)) {
            double qty = OverlayHelper.toDouble(row.get(OverlayHelper.COL_AVG), 1.0);
            int[] ps = (itemId > 0) ? priceMap.getOrDefault(itemId, new int[] { 0, 0 }) : new int[] { 0, 0 };
            int unitBuy = (ps.length > 0 ? ps[0] : 0);
            int unitSell = (ps.length > 1 ? ps[1] : 0);
            int buy = (int) Math.round(-qty * unitBuy);
            int sell = (int) Math.round(-qty * unitSell);
            return new int[] { buy, sell };
        }

        // Determine effective taxes for THIS row
        int taxesForRow;
        String effectiveCategory;
        String effectiveKey;

        if ("INTERNAL".equalsIgnoreCase(rawCategory)) {
            effectiveCategory = "INTERNAL";
            effectiveKey = (rawKey == null ? "" : rawKey);
            taxesForRow = 0; // internal always 0
        } else if (rawCategory != null && !rawCategory.isBlank() && rawKey != null && !rawKey.isBlank()) {
            effectiveCategory = rawCategory;
            effectiveKey = rawKey;
            taxesForRow = pickTaxesPercent(effectiveCategory, effectiveKey, null);
        } else {
            effectiveCategory = (rawCategory == null ? "" : rawCategory);
            effectiveKey = (rawKey == null ? "" : rawKey);
            taxesForRow = parentTaxesPercent;
        }

        // Composite ref?
        boolean isComposite = (rawCategory != null && !rawCategory.isBlank() && rawKey != null && !rawKey.isBlank());
        if (isComposite || "INTERNAL".equalsIgnoreCase(rawCategory)) {
            // Use DB-seeded operation for the referenced table
            String opForChild = pickAggregationOp(effectiveCategory, effectiveKey);

            int[] child = evForDetail(effectiveKey, priceMap, taxesForRow, tierKey, opForChild);
            int evBuy = (child.length > 0 ? child[0] : 0);
            int evSell = (child.length > 1 ? child[1] : 0);

            double qty = OverlayHelper.toDouble(row.get(OverlayHelper.COL_AVG), 1.0);
            return new int[] {
                    (int) Math.round(evBuy * qty),
                    (int) Math.round(evSell * qty)
            };
        }

        // Plain item (leaf)
        if (itemId > 0) {
            int[] ps = priceMap.getOrDefault(itemId, new int[] { 0, 0 });

            int unitBuy = ps.length > 0 ? Math.max(0, ps[0]) : 0;
            int unitSell = ps.length > 1 ? Math.max(0, ps[1]) : 0;

            int buyNet = OverlayHelper.net(unitBuy, taxesForRow);
            int sellNet = OverlayHelper.net(unitSell, taxesForRow);

            if (buyNet == 0 && sellNet == 0) {
                Integer vv = OverlayCache.vendorValueCached(itemId);
                if (vv != null && vv > 0) {
                    sellNet = vv;
                }
            }

            double qty = OverlayHelper.toDouble(row.get(OverlayHelper.COL_AVG), 1.0);
            return new int[] {
                    (int) Math.round(buyNet * qty),
                    (int) Math.round(sellNet * qty)
            };
        }

        // Nothing to contribute
        return new int[] { 0, 0 };
    }

    /** Aggregate a list of {buy,sell} pairs with SUM or MAX. */
    private static int[] aggregatePairs(List<int[]> pairs, String op) {
        if (pairs == null || pairs.isEmpty())
            return new int[] { 0, 0 };

        long bSum = 0, sSum = 0;
        int bMax = Integer.MIN_VALUE, sMax = Integer.MIN_VALUE;
        for (int[] p : pairs) {
            int b = (p == null || p.length < 1) ? 0 : p[0];
            int s = (p == null || p.length < 2) ? 0 : p[1];
            bSum += b;
            sSum += s;
            if (b > bMax)
                bMax = b;
            if (s > sMax)
                sMax = s;
        }

        if ("MAX".equalsIgnoreCase(op)) {
            return new int[] { Math.max(0, bMax), Math.max(0, sMax) };
        }
        // default SUM
        return new int[] { (int) Math.min(bSum, Integer.MAX_VALUE), (int) Math.min(sSum, Integer.MAX_VALUE) };
    }
}

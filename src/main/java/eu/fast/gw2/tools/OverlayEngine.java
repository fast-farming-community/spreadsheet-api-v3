package eu.fast.gw2.tools;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import eu.fast.gw2.dao.CalculationsDao;
import eu.fast.gw2.dao.Gw2PricesDao;
import eu.fast.gw2.dao.OverlayDao;
import eu.fast.gw2.enums.Tier;

public class OverlayEngine {

    /** Hard cap for profiling runs (override with -Doverlay.limit=50). 0 disables the cap. */
    private static final int RUN_LIMIT = Math.max(0, Integer.getInteger("overlay.limit", 50));

    // ---------------- public API ----------------

    public static void recomputeMain(String tableKey, Tier tier, boolean persist) {
        long start = System.currentTimeMillis();
        System.out.printf(java.util.Locale.ROOT, "Overlay: recomputeMain name='%s' tier=%s%n", tableKey, tier.columnKey());

        String rowsJson = OverlayDBAccess.getMainRowsJson(tableKey);
        if (rowsJson == null || rowsJson.isBlank()) {
            System.out.println("  -> no rows found, skipping.");
            return;
        }

        List<Map<String, Object>> rows = OverlayJson.parseRows(rowsJson);

        String tableCategory = OverlayHelper.dominantCategory(rows);
        var tableCfg = CalculationsDao.find(tableCategory, tableKey);

        // batch preload detail rows for all referenced composite/internal refs
        Set<String> detailKeys = collectDetailKeysForMain(rows);
        OverlayCache.preloadDetailRows(detailKeys);

        // Collect IDs including composite/internal referenced detail tables
        Set<Integer> needed = collectAllNeededIdsForMain(rows, detailKeys);
        System.out.printf(java.util.Locale.ROOT, "  -> rows=%d, ids=%d%n", rows.size(), needed.size());

        Map<Integer, int[]> priceMap = needed.isEmpty()
                ? new HashMap<>()
                : Gw2PricesDao.loadTier(new ArrayList<>(needed), tier.columnKey());

        long sumBuy = 0, sumSell = 0;

        // Per-row compute (qty = 1 for all)
        for (var row : rows) {
            String cat  = OverlayHelper.str(row.get(OverlayHelper.COL_CAT));
            String rkey = OverlayHelper.str(row.get(OverlayHelper.COL_KEY));
            int taxesPercent = OverlayCalc.pickTaxesPercent(cat, rkey, tableCfg);

            if (OverlayHelper.isInternal(cat) && rkey != null && !rkey.isBlank()) {
                var cfg = OverlayCalc.getCalcCfg(cat, rkey);
                String refKey = (cfg != null && cfg.sourceTableKey() != null && !cfg.sourceTableKey().isBlank())
                        ? cfg.sourceTableKey() : rkey;

                int[] ev = OverlayCalc.evForDetail(refKey, priceMap, 0, tier.columnKey());
                OverlayHelper.writeProfitWithHour(row, ev[0], ev[1]);

            } else if (OverlayHelper.isCompositeRef(cat, rkey)) {
                int[] ev = OverlayCalc.evForDetail(rkey, priceMap, taxesPercent, tier.columnKey());
                OverlayHelper.writeProfitWithHour(row, ev[0], ev[1]);

            } else {
                int id = OverlayHelper.toInt(row.get(OverlayHelper.COL_ID), -1);
                if (id > 0) {
                    int[] ps = priceMap.getOrDefault(id, new int[] { 0, 0 });
                    int buyNet = OverlayHelper.net(ps[0], taxesPercent);
                    int sellNet = OverlayHelper.net(ps[1], taxesPercent);
                    if (buyNet == 0 && sellNet == 0) {
                        Integer vendor = OverlayCache.vendorValueCached(id);
                        if (vendor != null) {
                            buyNet = vendor; sellNet = vendor;
                        }
                    }
                    OverlayHelper.writeProfitWithHour(row, buyNet, sellNet);
                }
            }

            Integer outBuy  = OverlayHelper.toIntBoxed(row.get(OverlayHelper.COL_TPB));
            Integer outSell = OverlayHelper.toIntBoxed(row.get(OverlayHelper.COL_TPS));
            sumBuy  += (outBuy  == null ? 0 : outBuy);
            sumSell += (outSell == null ? 0 : outSell);
        }

        if (tableCfg != null)
            OverlayHelper.applyAggregation(rows, tableCfg.operation());

        if (persist) {
            String out = OverlayJson.toJson(rows);
            OverlayDBAccess.updateMain(tableKey, out);
        }

        long dur = System.currentTimeMillis() - start;
        System.out.printf(java.util.Locale.ROOT,
                "  -> recomputeMain done: sumBuy=%d sumSell=%d persisted=%s (%.1fs)%n",
                sumBuy, sumSell, persist ? "yes" : "no", dur / 1000.0);
    }

    /** Recompute one table (detail_tables) identified by (detailFeatureId, tableKey). */
    public static void recompute(long detailFeatureId, String tableKey, Tier tier, boolean persist) {
        long start = System.currentTimeMillis();
        System.out.printf(java.util.Locale.ROOT, "Overlay: recompute detail fid=%d key='%s' tier=%s%n",
                detailFeatureId, tableKey, tier.columnKey());

        String rowsJson = OverlayDBAccess.getDetailRowsJson(detailFeatureId, tableKey);
        if (rowsJson == null || rowsJson.isBlank()) {
            System.out.println("  -> no rows found, skipping.");
            return;
        }

        List<Map<String, Object>> rows = OverlayJson.parseRows(rowsJson);

        var tableCategory = OverlayHelper.dominantCategory(rows);
        var tableCfg = CalculationsDao.find(tableCategory, tableKey);

        // batch preload detail rows for all referenced composite/internal refs
        Set<String> detailKeys = collectDetailKeysForDetail(rows);
        OverlayCache.preloadDetailRows(detailKeys);

        // Collect IDs including composite/internal referenced detail tables
        Set<Integer> needed = collectAllNeededIdsForDetail(rows, detailKeys);
        System.out.printf(java.util.Locale.ROOT, "  -> rows=%d, ids=%d%n", rows.size(), needed.size());

        Map<Integer, int[]> priceMap = needed.isEmpty()
                ? new HashMap<>()
                : Gw2PricesDao.loadTier(new ArrayList<>(needed), tier.columnKey());

        // Recompute rows
        for (var row : rows) {
            String cat  = OverlayHelper.str(row.get(OverlayHelper.COL_CAT));
            String rkey = OverlayHelper.str(row.get(OverlayHelper.COL_KEY));
            int taxesPercent = OverlayCalc.pickTaxesPercent(cat, rkey, tableCfg);

            if (OverlayHelper.isInternal(cat) && rkey != null && !rkey.isBlank()) {
                var cfg = OverlayCalc.getCalcCfg(cat, rkey);
                String refKey = (cfg != null && cfg.sourceTableKey() != null && !cfg.sourceTableKey().isBlank())
                        ? cfg.sourceTableKey() : rkey;

                int[] ev = OverlayCalc.evForDetail(refKey, priceMap, 0, tier.columnKey());
                double qty = OverlayHelper.toDouble(row.get(OverlayHelper.COL_AVG), 1.0);
                OverlayHelper.writeProfit(row, (int) Math.round(ev[0] * qty), (int) Math.round(ev[1] * qty));

            } else if (OverlayHelper.isCompositeRef(cat, rkey)) {
                int[] ev = OverlayCalc.evForDetail(rkey, priceMap, taxesPercent, tier.columnKey());
                double qty = OverlayHelper.toDouble(row.get(OverlayHelper.COL_AVG), 1.0);
                OverlayHelper.writeProfit(row, (int) Math.round(ev[0] * qty), (int) Math.round(ev[1] * qty));

            } else {
                int id = OverlayHelper.toInt(row.get(OverlayHelper.COL_ID), -1);
                if (id > 0) {
                    int[] ps = priceMap.getOrDefault(id, new int[] { 0, 0 });
                    int buyNet = OverlayHelper.net(ps[0], taxesPercent);
                    int sellNet = OverlayHelper.net(ps[1], taxesPercent);
                    if (buyNet == 0 && sellNet == 0) {
                        Integer vendor = OverlayCache.vendorValueCached(id);
                        if (vendor != null) {
                            buyNet = vendor; sellNet = vendor;
                        }
                    }
                    double qty = OverlayHelper.toDouble(row.get(OverlayHelper.COL_AVG), 1.0);
                    OverlayHelper.writeProfit(row, (int) Math.round(buyNet * qty), (int) Math.round(sellNet * qty));
                } else if (OverlayHelper.isCoinRow(row)) {
                    int amt = OverlayHelper.numericTotalAmount(row);
                    OverlayHelper.writeProfit(row, amt, amt);
                }
            }
        }

        if (tableCfg != null)
            OverlayHelper.applyAggregation(rows, tableCfg.operation());

        if (persist) {
            String out = OverlayJson.toJson(rows);
            OverlayDBAccess.updateDetail(detailFeatureId, tableKey, out);
        }

        long dur = System.currentTimeMillis() - start;
        System.out.printf(java.util.Locale.ROOT, "  -> recompute detail done: persisted=%s (%.1fs)%n",
                persist ? "yes" : "no", dur / 1000.0);
    }

    // ---------- private cores (timed wrappers) ----------

    private static List<Map<String, Object>> recomputeDetailCore(long fid, String key, Tier tier) {
        long start = System.currentTimeMillis();
        String rowsJson = OverlayDBAccess.getDetailRowsJson(fid, key);
        if (rowsJson == null || rowsJson.isBlank()) return List.of();

        List<Map<String, Object>> rows = OverlayJson.parseRows(rowsJson);
        var tableCategory = OverlayHelper.dominantCategory(rows);
        var tableCfg = CalculationsDao.find(tableCategory, key);

        Set<String> detailKeys = collectDetailKeysForDetail(rows);
        OverlayCache.preloadDetailRows(detailKeys);

        Set<Integer> needed = collectAllNeededIdsForDetail(rows, detailKeys);

        Map<Integer, int[]> priceMap = needed.isEmpty()
                ? new HashMap<>()
                : Gw2PricesDao.loadTier(new ArrayList<>(needed), tier.columnKey());

        for (var row : rows) {
            String cat  = OverlayHelper.str(row.get(OverlayHelper.COL_CAT));
            String rkey = OverlayHelper.str(row.get(OverlayHelper.COL_KEY));
            int taxesPercent = OverlayCalc.pickTaxesPercent(cat, rkey, tableCfg);

            if (OverlayHelper.isCompositeRef(cat, rkey)) {
                int[] ev = OverlayCalc.evForDetail(rkey, priceMap, taxesPercent, tier.columnKey());
                double qty = OverlayHelper.toDouble(row.get(OverlayHelper.COL_AVG), 1.0);
                OverlayHelper.writeProfit(row, (int) Math.round(ev[0] * qty), (int) Math.round(ev[1] * qty));

            } else if (OverlayHelper.isInternal(cat) && rkey != null && !rkey.isBlank()) {
                var cfg = OverlayCalc.getCalcCfg(cat, rkey);
                String refKey = (cfg != null && cfg.sourceTableKey() != null && !cfg.sourceTableKey().isBlank())
                        ? cfg.sourceTableKey() : rkey;

                int[] ev = OverlayCalc.evForDetail(refKey, priceMap, 0, tier.columnKey());
                double qty = OverlayHelper.toDouble(row.get(OverlayHelper.COL_AVG), 1.0);
                OverlayHelper.writeProfit(row, (int) Math.round(ev[0] * qty), (int) Math.round(ev[1] * qty));

            } else {
                int id = OverlayHelper.toInt(row.get(OverlayHelper.COL_ID), -1);
                if (id > 0) {
                    int[] ps = priceMap.getOrDefault(id, new int[] { 0, 0 });
                    int buyNet = OverlayHelper.net(ps[0], taxesPercent);
                    int sellNet = OverlayHelper.net(ps[1], taxesPercent);
                    if (buyNet == 0 && sellNet == 0) {
                        Integer vendor = OverlayCache.vendorValueCached(id);
                        if (vendor != null) { buyNet = vendor; sellNet = vendor; }
                    }
                    double qty = OverlayHelper.toDouble(row.get(OverlayHelper.COL_AVG), 1.0);
                    OverlayHelper.writeProfit(row, (int) Math.round(buyNet * qty), (int) Math.round(sellNet * qty));
                } else if (OverlayHelper.isCoinRow(row)) {
                    int amt = OverlayHelper.numericTotalAmount(row);
                    OverlayHelper.writeProfit(row, amt, amt);
                }
            }
        }

        if (tableCfg != null) OverlayHelper.applyAggregation(rows, tableCfg.operation());

        long dur = System.currentTimeMillis() - start;
        System.out.printf(java.util.Locale.ROOT, "    recomputeDetailCore key='%s' tier=%s rows=%d (%.1fs)%n",
                key, tier.columnKey(), rows.size(), dur / 1000.0);

        return rows;
    }

    private static List<Map<String, Object>> recomputeMainCore(String key, Tier tier) {
        long start = System.currentTimeMillis();
        String rowsJson = OverlayDBAccess.getMainRowsJson(key);
        if (rowsJson == null || rowsJson.isBlank()) return List.of();

        List<Map<String, Object>> rows = OverlayJson.parseRows(rowsJson);
        String tableCategory = OverlayHelper.dominantCategory(rows);
        var tableCfg = CalculationsDao.find(tableCategory, key);

        Set<String> detailKeys = collectDetailKeysForMain(rows);
        OverlayCache.preloadDetailRows(detailKeys);

        Set<Integer> needed = collectAllNeededIdsForMain(rows, detailKeys);

        Map<Integer, int[]> priceMap = needed.isEmpty()
                ? new HashMap<>()
                : Gw2PricesDao.loadTier(new ArrayList<>(needed), tier.columnKey());

        for (var row : rows) {
            String cat  = OverlayHelper.str(row.get(OverlayHelper.COL_CAT));
            String rkey = OverlayHelper.str(row.get(OverlayHelper.COL_KEY));
            int taxesPercent = OverlayCalc.pickTaxesPercent(cat, rkey, tableCfg);

            if (OverlayHelper.isCompositeRef(cat, rkey)) {
                int[] ev = OverlayCalc.evForDetail(rkey, priceMap, taxesPercent, tier.columnKey());
                OverlayHelper.writeProfitWithHour(row, ev[0], ev[1]);

            } else if (OverlayHelper.isInternal(cat) && rkey != null && !rkey.isBlank()) {
                var cfg = OverlayCalc.getCalcCfg(cat, rkey);
                String refKey = (cfg != null && cfg.sourceTableKey() != null && !cfg.sourceTableKey().isBlank())
                        ? cfg.sourceTableKey() : rkey;

                int[] ev = OverlayCalc.evForDetail(refKey, priceMap, 0, tier.columnKey());
                OverlayHelper.writeProfitWithHour(row, ev[0], ev[1]);

            } else {
                int id = OverlayHelper.toInt(row.get(OverlayHelper.COL_ID), -1);
                if (id > 0) {
                    int[] ps = priceMap.getOrDefault(id, new int[] { 0, 0 });
                    int buyNet = OverlayHelper.net(ps[0], taxesPercent);
                    int sellNet = OverlayHelper.net(ps[1], taxesPercent);
                    if (buyNet == 0 && sellNet == 0) {
                        Integer vendor = OverlayCache.vendorValueCached(id);
                        if (vendor != null) { buyNet = vendor; sellNet = vendor; }
                    }
                    OverlayHelper.writeProfitWithHour(row, buyNet, sellNet);
                }
            }
        }

        if (tableCfg != null) OverlayHelper.applyAggregation(rows, tableCfg.operation());

        long dur = System.currentTimeMillis() - start;
        System.out.printf(java.util.Locale.ROOT, "    recomputeMainCore key='%s' tier=%s rows=%d (%.1fs)%n",
                key, tier.columnKey(), rows.size(), dur / 1000.0);

        return rows;
    }

    // -------- public helpers (unchanged signatures) --------

    public static String recomputeDetailJson(long fid, String key, Tier tier) {
        var rows = recomputeDetailCore(fid, key, tier);
        return OverlayJson.toJson(rows);
    }

    public static String recomputeMainJson(String key, Tier tier) {
        var rows = recomputeMainCore(key, tier);
        return OverlayJson.toJson(rows);
    }

    public static void recomputeMainPersist(String key, Tier tier) {
        var rows = recomputeMainCore(key, tier);
        String out = OverlayJson.toJson(rows);
        OverlayDBAccess.updateMain(key, out);
    }

    /** Convenience overload using the common tier set. */
    public static void recomputeAndPersistAllOverlays(int sleepMs) {
        Tier[] tiers = { Tier.T5M, Tier.T10M, Tier.T15M, Tier.T60M };
        recomputeAndPersistAllOverlays(tiers, sleepMs);
    }

    @SuppressWarnings("unchecked")
    public static void recomputeAndPersistAllOverlays(Tier[] tiers, int sleepMs) {
        var detailTargets = OverlayDBAccess.listDetailTargets();
        var mainTargets   = OverlayDBAccess.listMainTargets();

        // limiter (optional)
        if (RUN_LIMIT > 0 && detailTargets.size() > RUN_LIMIT) {
            detailTargets = detailTargets.subList(0, RUN_LIMIT);
            System.out.printf(java.util.Locale.ROOT, "Limiter: detail targets capped to %d (overlay.limit=%d)%n",
                    detailTargets.size(), RUN_LIMIT);
        }
        if (RUN_LIMIT > 0 && mainTargets.size() > RUN_LIMIT) {
            mainTargets = mainTargets.subList(0, RUN_LIMIT);
            System.out.printf(java.util.Locale.ROOT, "Limiter: main targets capped to %d (overlay.limit=%d)%n",
                    mainTargets.size(), RUN_LIMIT);
        }

        for (Tier t : tiers) {
            long tierStart = System.currentTimeMillis();
            int ok = 0, fail = 0;
            System.out.println("Overlay: recompute detail_tables for tier " + t.name());
            for (Object[] row : detailTargets) {
                long fid = ((Number) row[0]).longValue();
                String key = (String) row[1];
                try {
                    String json = recomputeDetailJson(fid, key, t);
                    if (json != null) {
                        OverlayDao.upsertDetail(fid, key, t.label, json);
                    }
                    ok++;
                } catch (Exception e) {
                    fail++;
                    System.err.printf("! detail tier=%s fid=%d key='%s' -> %s: %s%n",
                            t.name(), fid, key, e.getClass().getSimpleName(),
                            (e.getMessage() == null ? "<no message>" : e.getMessage()));
                }
                sleepQuiet(sleepMs);
            }
            System.out.printf(java.util.Locale.ROOT, "  detail %s ok=%d fail=%d%n", t.name(), ok, fail);
            long tierDur = System.currentTimeMillis() - tierStart;
            System.out.printf(java.util.Locale.ROOT, "Tier %s (detail) finished in %.1fs%n", t.name(), tierDur / 1000.0);
        }

        for (Tier t : tiers) {
            long tierStart = System.currentTimeMillis();
            int ok = 0, fail = 0;
            System.out.println("Overlay: recompute main tables for tier " + t.name());
            for (String name : mainTargets) {
                try {
                    String json = recomputeMainJson(name, t);
                    if (json != null) {
                        OverlayDao.upsertMain(name, t.label, json);
                    }
                    ok++;
                } catch (Exception e) {
                    fail++;
                    System.err.printf("! main tier=%s name='%s' -> %s: %s%n",
                            t.name(), name, e.getClass().getSimpleName(),
                            (e.getMessage() == null ? "<no message>" : e.getMessage()));
                }
                sleepQuiet(sleepMs);
            }
            System.out.printf(java.util.Locale.ROOT, "  main %s ok=%d fail=%d%n", t.name(), ok, fail);
            long tierDur = System.currentTimeMillis() - tierStart;
            System.out.printf(java.util.Locale.ROOT, "Tier %s (main) finished in %.1fs%n", t.name(), tierDur / 1000.0);
        }
    }

    private static void sleepQuiet(int ms) {
        if (ms <= 0) return;
        try { Thread.sleep(ms); }
        catch (InterruptedException ignored) { Thread.currentThread().interrupt(); }
    }

    // --------- local helpers (keys/ids) ---------

    private static Set<String> collectDetailKeysForMain(List<Map<String, Object>> rows) {
        Set<String> keys = new HashSet<>();
        for (var row : rows) {
            String cat = OverlayHelper.str(row.get(OverlayHelper.COL_CAT));
            String rk  = OverlayHelper.str(row.get(OverlayHelper.COL_KEY));
            if (OverlayHelper.isCompositeRef(cat, rk)) {
                keys.add(rk);
            } else if (OverlayHelper.isInternal(cat) && rk != null && !rk.isBlank()) {
                var cfg = OverlayCalc.getCalcCfg(cat, rk);
                keys.add((cfg != null && cfg.sourceTableKey() != null && !cfg.sourceTableKey().isBlank())
                        ? cfg.sourceTableKey() : rk);
            }
        }
        return keys;
    }

    private static Set<String> collectDetailKeysForDetail(List<Map<String, Object>> rows) {
        return collectDetailKeysForMain(rows);
    }

    private static Set<Integer> collectAllNeededIdsForMain(List<Map<String, Object>> rows, Set<String> preloadedKeys) {
        Set<Integer> needed = new HashSet<>(Math.max(16, rows.size() / 2));
        // leaf ids
        for (var row : rows) {
            String cat  = OverlayHelper.str(row.get(OverlayHelper.COL_CAT));
            String rkey = OverlayHelper.str(row.get(OverlayHelper.COL_KEY));
            if (OverlayHelper.isCompositeRef(cat, rkey) || OverlayHelper.isInternal(cat)) continue;
            int id = OverlayHelper.toInt(row.get(OverlayHelper.COL_ID), -1);
            if (id > 0) needed.add(id);
        }
        // ids from preloaded detail rows
        if (preloadedKeys != null) {
            for (String k : preloadedKeys) {
                var drops = OverlayCache.getDetailRowsCached(k);
                needed.addAll(OverlayHelper.extractIds(drops));
            }
        }
        return needed;
    }

    private static Set<Integer> collectAllNeededIdsForDetail(List<Map<String, Object>> rows, Set<String> preloadedKeys) {
        return collectAllNeededIdsForMain(rows, preloadedKeys);
    }
}

package eu.fast.gw2.tools;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;

import eu.fast.gw2.dao.CalculationsDao;
import eu.fast.gw2.dao.Gw2PricesDao;
import eu.fast.gw2.dao.OverlayDao;
import eu.fast.gw2.enums.Tier;

public class OverlayEngine {

    private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();

    // Column names (detail_tables)
    private static final String COL_ID = "Id";
    private static final String COL_CAT = "Category";
    private static final String COL_KEY = "Key";
    private static final String COL_NAME = "Name";
    private static final String COL_AVG = "AverageAmount";
    private static final String COL_TPB = "TPBuyProfit";
    private static final String COL_TPS = "TPSellProfit";
    private static final String COL_TOTAL_AMOUNT = "TotalAmount";

    // Column names (tables - main)
    private static final String COL_TPB_HR = "TPBuyProfitHr";
    private static final String COL_TPS_HR = "TPSellProfitHr";
    private static final String COL_HOURS = "Duration";

    // ---- lightweight caches (per run) ----
    private static final int DETAIL_CACHE_MAX = 256;

    /** LRU cache for detail_tables.rows by key. */
    private static final LinkedHashMap<String, List<Map<String, Object>>> DETAIL_ROWS_CACHE = new LinkedHashMap<>(64,
            0.75f, true) {
        @Override
        protected boolean removeEldestEntry(Map.Entry<String, List<Map<String, Object>>> eldest) {
            return size() > DETAIL_CACHE_MAX;
        }
    };

    /** Cache for calculations by (category|key). Allows null values. */
    private static final Map<String, CalculationsDao.Config> CALC_CACHE = new HashMap<>();

    /** Cache for vendor values by item id. Allows null values. */
    private static final Map<Integer, Integer> VENDOR_CACHE = new HashMap<>();

    // ---------------- public API ----------------

    public static void recomputeMain(String tableKey, Tier tier, boolean persist) {
        System.out.printf(Locale.ROOT, "Overlay: recomputeMain name='%s' tier=%s%n", tableKey, tier.columnKey());

        String rowsJson = Jpa.tx(em -> {
            var query = em.createNativeQuery("""
                        SELECT rows
                          FROM public.tables
                         WHERE name = :k
                         ORDER BY id DESC
                         LIMIT 1
                    """).setParameter("k", tableKey).getResultList();
            return query.isEmpty() ? null : (String) query.get(0);
        });
        if (rowsJson == null || rowsJson.isBlank()) {
            System.out.println("  -> no rows found, skipping.");
            return;
        }

        List<Map<String, Object>> rows = parseRows(rowsJson);
        System.out.printf(Locale.ROOT, "  -> rows=%d%n", rows.size());

        String tableCategory = dominantCategory(rows);
        var tableCfg = CalculationsDao.find(tableCategory, tableKey);

        // Collect leaf ids
        Set<Integer> needed = new HashSet<>(Math.max(16, rows.size() / 2));
        for (var row : rows) {
            String cat = str(row.get(COL_CAT));
            String key = str(row.get(COL_KEY));
            if (isCompositeRef(cat, key) || isInternal(cat))
                continue;
            int id = toInt(row.get(COL_ID), -1);
            if (id > 0)
                needed.add(id);
        }

        Map<Integer, int[]> priceMap = Gw2PricesDao.loadTier(new ArrayList<>(needed), tier.columnKey());

        long sumBuy = 0, sumSell = 0;

        // Per-row compute (qty = 1 for all)
        for (int i = 0; i < rows.size(); i++) {
            var row = rows.get(i);
            String cat = str(row.get(COL_CAT));
            String rkey = str(row.get(COL_KEY));
            int taxesPercent = pickTaxesPercent(cat, rkey, tableCfg);

            if (isInternal(cat) && rkey != null && !rkey.isBlank()) {
                var cfg = getCalcCfg(cat, rkey);
                String refKey = (cfg != null && cfg.sourceTableKey() != null && !cfg.sourceTableKey().isBlank())
                        ? cfg.sourceTableKey()
                        : rkey;

                var src = loadDetailRowsCached(refKey);
                ensurePricesForIds(priceMap, extractIds(src), tier.columnKey());
                int[] ev = bagEV(src, priceMap, 0);
                writeProfitWithHour(row, ev[0], ev[1]);

            } else if (isCompositeRef(cat, rkey)) {
                var drops = loadDetailRowsCached(rkey);
                ensurePricesForIds(priceMap, extractIds(drops), tier.columnKey());
                int[] ev = bagEV(drops, priceMap, taxesPercent);
                writeProfitWithHour(row, ev[0], ev[1]);

            } else {
                int id = toInt(row.get(COL_ID), -1);
                if (id > 0) {
                    int[] ps = priceMap.getOrDefault(id, new int[] { 0, 0 });
                    int buyNet = net(ps[0], taxesPercent);
                    int sellNet = net(ps[1], taxesPercent);
                    if (buyNet == 0 && sellNet == 0) {
                        Integer vendor = vendorValueCached(id);
                        if (vendor != null) {
                            buyNet = vendor;
                            sellNet = vendor;
                        }
                    }
                    writeProfitWithHour(row, buyNet, sellNet);
                }
            }

            Integer outBuy = toIntBoxed(row.get(COL_TPB));
            Integer outSell = toIntBoxed(row.get(COL_TPS));
            sumBuy += (outBuy == null ? 0 : outBuy);
            sumSell += (outSell == null ? 0 : outSell);
        }

        if (tableCfg != null)
            applyAggregation(rows, tableCfg.operation());

        if (persist) {
            String out = toJson(rows);
            Jpa.txVoid(em -> em.createNativeQuery("""
                        UPDATE public.tables
                           SET rows = :rows, updated_at = now()
                         WHERE name = :k
                    """).setParameter("rows", out)
                    .setParameter("k", tableKey)
                    .executeUpdate());
        }

        System.out.printf(Locale.ROOT,
                "  -> recomputeMain done: sumBuy=%d sumSell=%d persisted=%s%n",
                sumBuy, sumSell, persist ? "yes" : "no");
    }

    /**
     * Recompute one table (detail_tables) identified by (detailFeatureId,
     * tableKey).
     */
    public static void recompute(long detailFeatureId, String tableKey, Tier tier, boolean persist) {
        System.out.printf(Locale.ROOT, "Overlay: recompute detail fid=%d key='%s' tier=%s%n",
                detailFeatureId, tableKey, tier.columnKey());

        String rowsJson = Jpa.tx(em -> {
            var query = em.createNativeQuery("""
                        SELECT rows FROM public.detail_tables
                         WHERE detail_feature_id = :fid AND key = :k
                    """).setParameter("fid", detailFeatureId).setParameter("k", tableKey).getResultList();
            return query.isEmpty() ? null : (String) query.get(0);
        });
        if (rowsJson == null || rowsJson.isBlank()) {
            System.out.println("  -> no rows found, skipping.");
            return;
        }

        List<Map<String, Object>> rows = parseRows(rowsJson);
        System.out.printf(Locale.ROOT, "  -> rows=%d%n", rows.size());

        var tableCategory = dominantCategory(rows);
        var tableCfg = CalculationsDao.find(tableCategory, tableKey);

        // Collect leaf IDs
        Set<Integer> needed = new HashSet<>(Math.max(16, rows.size() / 2));
        for (var row : rows) {
            String cat = str(row.get(COL_CAT));
            String key = str(row.get(COL_KEY));
            if (isCompositeRef(cat, key) || isInternal(cat))
                continue;
            int id = toInt(row.get(COL_ID), -1);
            if (id > 0)
                needed.add(id);
        }

        Map<Integer, int[]> priceMap = Gw2PricesDao.loadTier(new ArrayList<>(needed), tier.columnKey());

        // Recompute rows
        for (int i = 0; i < rows.size(); i++) {
            var row = rows.get(i);
            String cat = str(row.get(COL_CAT));
            String rkey = str(row.get(COL_KEY));
            int taxesPercent = pickTaxesPercent(cat, rkey, tableCfg);

            if (isInternal(cat) && rkey != null && !rkey.isBlank()) {
                var cfg = getCalcCfg(cat, rkey);
                String refKey = (cfg != null && cfg.sourceTableKey() != null && !cfg.sourceTableKey().isBlank())
                        ? cfg.sourceTableKey()
                        : rkey;

                var src = loadDetailRowsCached(refKey);
                ensurePricesForIds(priceMap, extractIds(src), tier.columnKey());
                int[] ev = bagEV(src, priceMap, 0);
                double qty = toDouble(row.get(COL_AVG), 1.0);
                writeProfit(row, (int) Math.round(ev[0] * qty), (int) Math.round(ev[1] * qty));

            } else if (isCompositeRef(cat, rkey)) {
                var drops = loadDetailRowsCached(rkey);
                ensurePricesForIds(priceMap, extractIds(drops), tier.columnKey());
                int[] ev = bagEV(drops, priceMap, taxesPercent);
                double qty = toDouble(row.get(COL_AVG), 1.0);
                writeProfit(row, (int) Math.round(ev[0] * qty), (int) Math.round(ev[1] * qty));

            } else {
                int id = toInt(row.get(COL_ID), -1);
                if (id > 0) {
                    int[] ps = priceMap.getOrDefault(id, new int[] { 0, 0 });
                    int buyNet = net(ps[0], taxesPercent);
                    int sellNet = net(ps[1], taxesPercent);
                    if (buyNet == 0 && sellNet == 0) {
                        Integer vendor = vendorValueCached(id);
                        if (vendor != null) {
                            buyNet = vendor;
                            sellNet = vendor;
                        }
                    }
                    double qty = toDouble(row.get(COL_AVG), 1.0);
                    writeProfit(row, (int) Math.round(buyNet * qty), (int) Math.round(sellNet * qty));

                } else if (isCoinRow(row)) {
                    int amt = numericTotalAmount(row);
                    writeProfit(row, amt, amt);
                }
            }
        }

        if (tableCfg != null)
            applyAggregation(rows, tableCfg.operation());

        if (persist) {
            String out = toJson(rows);
            Jpa.txVoid(em -> em.createNativeQuery("""
                        UPDATE public.detail_tables
                           SET rows = :rows, updated_at = now()
                         WHERE detail_feature_id = :fid AND key = :k
                    """).setParameter("rows", out)
                    .setParameter("fid", detailFeatureId)
                    .setParameter("k", tableKey)
                    .executeUpdate());
        }

        System.out.printf(Locale.ROOT, "  -> recompute detail done: persisted=%s%n", persist ? "yes" : "no");
    }

    // ---------- private cores (no logging) ----------

    private static List<Map<String, Object>> recomputeDetailCore(long fid, String key, Tier tier) {
        String rowsJson = Jpa.tx(em -> {
            var r = em.createNativeQuery("""
                        SELECT rows FROM public.detail_tables
                         WHERE detail_feature_id = :fid AND key = :k
                    """).setParameter("fid", fid).setParameter("k", key).getResultList();
            return r.isEmpty() ? null : (String) r.get(0);
        });
        if (rowsJson == null || rowsJson.isBlank())
            return List.of();

        List<Map<String, Object>> rows = parseRows(rowsJson);

        var tableCategory = dominantCategory(rows);
        var tableCfg = CalculationsDao.find(tableCategory, key);

        Set<Integer> needed = new HashSet<>(Math.max(16, rows.size() / 2));
        for (var row : rows) {
            String cat = str(row.get(COL_CAT));
            String rkey = str(row.get(COL_KEY));
            if (isCompositeRef(cat, rkey) || isInternal(cat))
                continue;
            int id = toInt(row.get(COL_ID), -1);
            if (id > 0)
                needed.add(id);
        }
        Map<Integer, int[]> priceMap = Gw2PricesDao.loadTier(new ArrayList<>(needed), tier.columnKey());

        for (int i = 0; i < rows.size(); i++) {
            var row = rows.get(i);
            String cat = str(row.get(COL_CAT));
            String rkey = str(row.get(COL_KEY));
            int taxesPercent = pickTaxesPercent(cat, rkey, tableCfg);

            if (isCompositeRef(cat, rkey)) {
                var drops = loadDetailRowsCached(rkey);
                ensurePricesForIds(priceMap, extractIds(drops), tier.columnKey());
                int[] ev = bagEV(drops, priceMap, taxesPercent);
                double qty = toDouble(row.get(COL_AVG), 1.0);
                writeProfit(row, (int) Math.round(ev[0] * qty), (int) Math.round(ev[1] * qty));

            } else if (isInternal(cat) && rkey != null && !rkey.isBlank()) {
                var cfg = getCalcCfg(cat, rkey);
                String refKey = (cfg != null && cfg.sourceTableKey() != null && !cfg.sourceTableKey().isBlank())
                        ? cfg.sourceTableKey()
                        : rkey;

                var src = loadDetailRowsCached(refKey);
                ensurePricesForIds(priceMap, extractIds(src), tier.columnKey());
                int[] ev = bagEV(src, priceMap, 0);
                double qty = toDouble(row.get(COL_AVG), 1.0);
                writeProfit(row, (int) Math.round(ev[0] * qty), (int) Math.round(ev[1] * qty));

            } else {
                int id = toInt(row.get(COL_ID), -1);
                if (id > 0) {
                    int[] ps = priceMap.getOrDefault(id, new int[] { 0, 0 });
                    int buyNet = net(ps[0], taxesPercent);
                    int sellNet = net(ps[1], taxesPercent);
                    if (buyNet == 0 && sellNet == 0) {
                        Integer vendor = vendorValueCached(id);
                        if (vendor != null) {
                            buyNet = vendor;
                            sellNet = vendor;
                        }
                    }
                    double qty = toDouble(row.get(COL_AVG), 1.0);
                    writeProfit(row, (int) Math.round(buyNet * qty), (int) Math.round(sellNet * qty));

                } else if (isCoinRow(row)) {
                    int amt = numericTotalAmount(row);
                    writeProfit(row, amt, amt);
                }
            }
        }

        tableCategory = dominantCategory(rows);
        tableCfg = CalculationsDao.find(tableCategory, key);
        if (tableCfg != null)
            applyAggregation(rows, tableCfg.operation());

        return rows;
    }

    private static List<Map<String, Object>> recomputeMainCore(String key, Tier tier) {
        String rowsJson = Jpa.tx(em -> {
            var r = em.createNativeQuery("""
                        SELECT rows FROM public.tables
                         WHERE name = :k
                         ORDER BY id DESC
                         LIMIT 1
                    """).setParameter("k", key).getResultList();
            return r.isEmpty() ? null : (String) r.get(0);
        });
        if (rowsJson == null || rowsJson.isBlank())
            return List.of();

        List<Map<String, Object>> rows = parseRows(rowsJson);

        String tableCategory = dominantCategory(rows);
        var tableCfg = CalculationsDao.find(tableCategory, key);

        Set<Integer> needed = new HashSet<>(Math.max(16, rows.size() / 2));
        for (var row : rows) {
            String cat = str(row.get(COL_CAT));
            String rkey = str(row.get(COL_KEY));
            if (isCompositeRef(cat, rkey) || isInternal(cat))
                continue;
            int id = toInt(row.get(COL_ID), -1);
            if (id > 0)
                needed.add(id);
        }
        Map<Integer, int[]> priceMap = Gw2PricesDao.loadTier(new ArrayList<>(needed), tier.columnKey());

        for (int i = 0; i < rows.size(); i++) {
            var row = rows.get(i);
            String cat = str(row.get(COL_CAT));
            String rkey = str(row.get(COL_KEY));
            int taxesPercent = pickTaxesPercent(cat, rkey, tableCfg);

            if (isCompositeRef(cat, rkey)) {
                var drops = loadDetailRowsCached(rkey);
                ensurePricesForIds(priceMap, extractIds(drops), tier.columnKey());
                int[] ev = bagEV(drops, priceMap, taxesPercent);
                writeProfitWithHour(row, ev[0], ev[1]);

            } else if (isInternal(cat) && rkey != null && !rkey.isBlank()) {
                var cfg = getCalcCfg(cat, rkey);
                String refKey = (cfg != null && cfg.sourceTableKey() != null && !cfg.sourceTableKey().isBlank())
                        ? cfg.sourceTableKey()
                        : rkey;

                var src = loadDetailRowsCached(refKey);
                ensurePricesForIds(priceMap, extractIds(src), tier.columnKey());
                int[] ev = bagEV(src, priceMap, 0);
                writeProfitWithHour(row, ev[0], ev[1]);

            } else {
                int id = toInt(row.get(COL_ID), -1);
                if (id > 0) {
                    int[] ps = priceMap.getOrDefault(id, new int[] { 0, 0 });
                    int buyNet = net(ps[0], taxesPercent);
                    int sellNet = net(ps[1], taxesPercent);
                    if (buyNet == 0 && sellNet == 0) {
                        Integer vendor = vendorValueCached(id);
                        if (vendor != null) {
                            buyNet = vendor;
                            sellNet = vendor;
                        }
                    }
                    writeProfitWithHour(row, buyNet, sellNet);
                }
            }
        }

        if (tableCfg != null)
            applyAggregation(rows, tableCfg.operation());
        return rows;
    }

    // -------- public helpers (unchanged signatures) --------

    public static String recomputeDetailJson(long fid, String key, Tier tier) {
        var rows = recomputeDetailCore(fid, key, tier);
        return toJson(rows);
    }

    public static String recomputeMainJson(String key, Tier tier) {
        var rows = recomputeMainCore(key, tier);
        return toJson(rows);
    }

    public static void recomputeMainPersist(String key, Tier tier) {
        var rows = recomputeMainCore(key, tier);
        String out = toJson(rows);
        Jpa.txVoid(em -> em.createNativeQuery("""
                    UPDATE public.tables
                       SET rows = :rows, updated_at = now()
                     WHERE name = :k
                """).setParameter("rows", out)
                .setParameter("k", key)
                .executeUpdate());
    }

    // ---------------- small utils ----------------

    private static boolean isCompositeRef(String category, String key) {
        return category != null && !category.isBlank() && key != null && !key.isBlank();
    }

    private static boolean isInternal(String category) {
        return eq(category, "INTERNAL");
    }

    private static String dominantCategory(List<Map<String, Object>> rows) {
        Map<String, Integer> freq = new HashMap<>();
        for (var r : rows) {
            String c = str(r.get(COL_CAT));
            if (c != null && !c.isBlank())
                freq.merge(c.trim(), 1, Integer::sum);
        }
        if (freq.isEmpty())
            return "";
        return freq.entrySet().stream().max(Map.Entry.comparingByValue()).map(Map.Entry::getKey).orElse("");
    }

    private static int pickTaxesPercent(String category, String key, CalculationsDao.Config tableCfg) {
        if (isInternal(category))
            return 0;
        if (category != null && !category.isBlank() && key != null && !key.isBlank())
            return 0;

        var rowCfg = getCalcCfg(category, key);
        if (rowCfg != null)
            return clampPercent(rowCfg.taxes());
        if (tableCfg != null)
            return clampPercent(tableCfg.taxes());
        return 15;
    }

    private static CalculationsDao.Config getCalcCfg(String category, String key) {
        String ck = ((category == null ? "" : category.trim().toUpperCase()) + "|" + (key == null ? "" : key.trim()));
        if (CALC_CACHE.containsKey(ck))
            return CALC_CACHE.get(ck);
        var cfg = CalculationsDao.find(category, key);
        CALC_CACHE.put(ck, cfg); // may be null
        return cfg;
    }

    private static Integer vendorValueCached(int itemId) {
        if (VENDOR_CACHE.containsKey(itemId))
            return VENDOR_CACHE.get(itemId);
        Integer v = Gw2PricesDao.vendorValueById(itemId);
        VENDOR_CACHE.put(itemId, v); // may be null
        return v;
    }

    private static void ensurePricesForIds(Map<Integer, int[]> priceMap, Set<Integer> ids, String tierKey) {
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
            priceMap.putAll(Gw2PricesDao.loadTier(missing, tierKey));
        }
    }

    private static Set<Integer> extractIds(List<Map<String, Object>> rows) {
        if (rows == null || rows.isEmpty())
            return Set.of();
        Set<Integer> ids = new HashSet<>(Math.max(16, rows.size() / 2));
        for (var d : rows) {
            int id = toInt(d.get(COL_ID), -1);
            if (id > 0)
                ids.add(id);
        }
        return ids;
    }

    private static int clampPercent(int p) {
        if (p < 0)
            return 0;
        if (p > 100)
            return 100;
        return p;
    }

    private static int net(int value, int taxesPercent) {
        if (value <= 0)
            return 0;
        if (taxesPercent <= 0)
            return value;
        double f = 1.0 - (taxesPercent / 100.0);
        return (int) Math.floor(value * f);
    }

    private static int[] bagEV(List<Map<String, Object>> drops, Map<Integer, int[]> priceMap, int taxesPercent) {
        long sumBuy = 0, sumSell = 0;
        for (var d : drops) {
            int id = toInt(d.get(COL_ID), -1);
            if (id <= 0)
                continue;
            double avgQty = toDouble(d.get(COL_AVG), 0.0);
            int[] ps = priceMap.getOrDefault(id, new int[] { 0, 0 });
            int buyNet = net(ps[0], taxesPercent);
            int sellNet = net(ps[1], taxesPercent);
            sumBuy += Math.round(avgQty * buyNet);
            sumSell += Math.round(avgQty * sellNet);
        }
        return new int[] { (int) Math.min(sumBuy, Integer.MAX_VALUE),
                (int) Math.min(sumSell, Integer.MAX_VALUE) };
    }

    private static void writeProfit(Map<String, Object> row, int buy, int sell) {
        row.put(COL_TPB, buy);
        row.put(COL_TPS, sell);
    }

    private static void writeProfitWithHour(Map<String, Object> row, int buy, int sell) {
        writeProfit(row, buy, sell);
        double hours = toDouble(row.get(COL_HOURS), 0.0);
        int buyHr = (hours > 0.0) ? (int) Math.floor(buy / hours) : buy;
        int sellHr = (hours > 0.0) ? (int) Math.floor(sell / hours) : sell;
        row.put(COL_TPB_HR, buyHr);
        row.put(COL_TPS_HR, sellHr);
    }

    private static void applyAggregation(List<Map<String, Object>> rows, String op) {
        var buyBase = new ArrayList<Integer>();
        var sellBase = new ArrayList<Integer>();
        var buyHr = new ArrayList<Integer>();
        var sellHr = new ArrayList<Integer>();

        for (var query : rows) {
            Integer b = toIntBoxed(query.get(COL_TPB));
            Integer s = toIntBoxed(query.get(COL_TPS));
            if (b != null)
                buyBase.add(b);
            if (s != null)
                sellBase.add(s);

            Integer bh = query.containsKey(COL_TPB_HR) ? toIntBoxed(query.get(COL_TPB_HR)) : null;
            Integer sh = query.containsKey(COL_TPS_HR) ? toIntBoxed(query.get(COL_TPS_HR)) : null;

            if (bh == null || sh == null) {
                double hours = toDouble(query.get(COL_HOURS), 0.0);
                if (hours > 0.0) {
                    if (bh == null && b != null)
                        bh = (int) Math.floor(b / hours);
                    if (sh == null && s != null)
                        sh = (int) Math.floor(s / hours);
                }
            }
            if (bh != null)
                buyHr.add(bh);
            if (sh != null)
                sellHr.add(sh);
        }

        if (buyBase.isEmpty() && sellBase.isEmpty() && buyHr.isEmpty() && sellHr.isEmpty())
            return;

        String agg = (op == null ? "SUM" : op.toUpperCase());
        java.util.function.Function<List<Integer>, Integer> AGG = xs -> {
            if (xs == null || xs.isEmpty())
                return 0;
            return switch (agg) {
                case "AVG" -> avg(xs);
                case "MIN" -> xs.stream().min(Integer::compare).orElse(0);
                case "MAX" -> xs.stream().max(Integer::compare).orElse(0);
                default -> sum(xs);
            };
        };

        int aggBuyBase = AGG.apply(buyBase);
        int aggSellBase = AGG.apply(sellBase);
        Integer aggBuyHr = buyHr.isEmpty() ? null : AGG.apply(buyHr);
        Integer aggSellHr = sellHr.isEmpty() ? null : AGG.apply(sellHr);

        Map<String, Object> total = rows.stream()
                .filter(q -> "TOTAL".equalsIgnoreCase(str(q.get(COL_KEY)))
                        || "TOTAL".equalsIgnoreCase(str(q.get(COL_NAME))))
                .findFirst()
                .orElseGet(() -> {
                    var t = new LinkedHashMap<String, Object>();
                    t.put(COL_KEY, "TOTAL");
                    t.put(COL_NAME, "TOTAL");
                    rows.add(t);
                    return t;
                });

        total.put(COL_TPB, aggBuyBase);
        total.put(COL_TPS, aggSellBase);
        if (aggBuyHr != null)
            total.put(COL_TPB_HR, aggBuyHr);
        if (aggSellHr != null)
            total.put(COL_TPS_HR, aggSellHr);
    }

    // JSON load/save and utils

    private static int sum(List<Integer> xs) {
        long s = 0L;
        for (int v : xs)
            s += v;
        return (int) Math.min(s, Integer.MAX_VALUE);
    }

    private static int avg(List<Integer> xs) {
        if (xs == null || xs.isEmpty())
            return 0;
        long s = 0L;
        for (int v : xs)
            s += v;
        return (int) Math.floor(s / (double) xs.size());
    }

    private static boolean isCoinRow(Map<String, Object> row) {
        String name = str(row.get(COL_NAME));
        return "Coin".equalsIgnoreCase(name);
    }

    private static int numericTotalAmount(Map<String, Object> row) {
        Object v = row.get(COL_TOTAL_AMOUNT);
        if (v == null)
            return 0;
        if (v instanceof Number n)
            return (int) Math.floor(n.doubleValue());
        try {
            return (int) Math.floor(Double.parseDouble(String.valueOf(v).replace(',', '.')));
        } catch (Exception e) {
            return 0;
        }
    }

    private static List<Map<String, Object>> parseRows(String json) {
        try {
            return OBJECT_MAPPER.readValue(json, new TypeReference<List<Map<String, Object>>>() {
            });
        } catch (Exception e) {
            throw new RuntimeException("Failed to parse rows JSON", e);
        }
    }

    private static String toJson(List<Map<String, Object>> rows) {
        try {
            return OBJECT_MAPPER.writeValueAsString(rows);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    private static List<Map<String, Object>> loadDetailRowsCached(String key) {
        if (key == null || key.isBlank())
            return List.of();
        var cached = DETAIL_ROWS_CACHE.get(key);
        if (cached != null)
            return cached;

        String json = Jpa.tx(em -> {
            var query = em.createNativeQuery("""
                        SELECT rows FROM public.detail_tables
                         WHERE key = :k
                         ORDER BY id DESC
                         LIMIT 1
                    """).setParameter("k", key).getResultList();
            return query.isEmpty() ? null : (String) query.get(0);
        });
        if (json == null || json.isBlank()) {
            DETAIL_ROWS_CACHE.put(key, List.of());
            return List.of();
        }
        var rows = parseRows(json);
        DETAIL_ROWS_CACHE.put(key, rows);
        return rows;
    }

    private static String str(Object o) {
        return o == null ? null : String.valueOf(o);
    }

    private static boolean eq(String a, String b) {
        return a != null && b != null && a.equalsIgnoreCase(b);
    }

    private static int toInt(Object o, int def) {
        if (o == null)
            return def;
        if (o instanceof Number n)
            return n.intValue();
        try {
            return (int) Math.floor(Double.parseDouble(String.valueOf(o).replace(',', '.')));
        } catch (Exception e) {
            return def;
        }
    }

    private static Integer toIntBoxed(Object o) {
        if (o == null)
            return null;
        if (o instanceof Number n)
            return n.intValue();
        try {
            return (int) Math.floor(Double.parseDouble(String.valueOf(o).replace(',', '.')));
        } catch (Exception e) {
            return null;
        }
    }

    private static Double toDouble(Object o, double def) {
        if (o == null)
            return def;
        if (o instanceof Number n)
            return n.doubleValue();
        try {
            return Double.parseDouble(String.valueOf(o).replace(',', '.'));
        } catch (Exception e) {
            return def;
        }
    }

    /** Convenience overload using the common tier set. */
    public static void recomputeAndPersistAllOverlays(int sleepMs) {
        Tier[] tiers = { Tier.T5M, Tier.T10M, Tier.T15M, Tier.T60M };
        recomputeAndPersistAllOverlays(tiers, sleepMs);
    }

    @SuppressWarnings("unchecked")
    public static void recomputeAndPersistAllOverlays(Tier[] tiers, int sleepMs) {
        var detailTargets = Jpa.tx(em -> em.createNativeQuery("""
                    SELECT detail_feature_id, key
                      FROM public.detail_tables
                     ORDER BY id DESC
                """).getResultList());

        var mainTargets = Jpa.tx(em -> (java.util.List<String>) em.createNativeQuery("""
                    SELECT DISTINCT name
                      FROM public.tables
                     ORDER BY name
                """).getResultList());

        for (Tier t : tiers) {
            int ok = 0, fail = 0;
            System.out.println("Overlay: recompute detail_tables for tier " + t.name());
            for (Object rowObj : detailTargets) {
                Object[] row = (Object[]) rowObj;
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
                    System.err
                            .println("! detail " + t.name() + " fid=" + fid + " key=" + key + " -> " + e.getMessage());
                }
                sleepQuiet(sleepMs);
            }
            System.out.printf(java.util.Locale.ROOT, "  detail %s ok=%d fail=%d%n", t.name(), ok, fail);
        }

        for (Tier t : tiers) {
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
                    System.err.println("! main " + t.name() + " name=" + name + " -> " + e.getMessage());
                }
                sleepQuiet(sleepMs);
            }
            System.out.printf(java.util.Locale.ROOT, "  main %s ok=%d fail=%d%n", t.name(), ok, fail);
        }
    }

    private static void sleepQuiet(int ms) {
        if (ms <= 0)
            return;
        try {
            Thread.sleep(ms);
        } catch (InterruptedException ignored) {
            Thread.currentThread().interrupt();
        }
    }
}

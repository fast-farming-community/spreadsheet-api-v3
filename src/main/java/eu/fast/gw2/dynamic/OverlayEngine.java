package eu.fast.gw2.dynamic;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;

import eu.fast.gw2.dao.CalculationsDao;
import eu.fast.gw2.dao.Gw2PricesDao;
import eu.fast.gw2.dao.ItemsDao;
import eu.fast.gw2.enums.Tier;
import eu.fast.gw2.jpa.Jpa;
import eu.fast.gw2.main.DebugTrace;

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

    public static void recomputeMain(String tableKey, Tier tier, boolean persist) {
        // 1) load rows JSON from main tables
        String rowsJson = Jpa.tx(em -> {
            var query = em.createNativeQuery("""
                        SELECT rows
                          FROM public.tables
                         WHERE key = :k
                         ORDER BY id DESC
                         LIMIT 1
                    """).setParameter("k", tableKey).getResultList();
            return query.isEmpty() ? null : (String) query.get(0);
        });
        if (rowsJson == null || rowsJson.isBlank())
            return;

        List<Map<String, Object>> rows = parseRows(rowsJson);

        // table-level config (tax/op) – same logic as detail tables
        String tableCategory = dominantCategory(rows);
        var tableCfg = CalculationsDao.find(tableCategory, tableKey);

        // collect leaf ids first
        Set<Integer> needed = new HashSet<>();
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

        // optional debug header
        if (DebugTrace.on()) {
            DebugTrace.rowHeader(-1L, tableKey, rows.size(), rows.size());
        }

        long sumBuy = 0, sumSell = 0;
        int printed = 0;

        // 2) per-row compute (qty = 1 for all)
        for (int i = 0; i < rows.size(); i++) {
            var row = rows.get(i);
            Integer idIn = toIntBoxed(row.get(COL_ID));
            String name = str(row.get(COL_NAME));
            String cat = str(row.get(COL_CAT));
            String rkey = str(row.get(COL_KEY));
            Long tpbIn = toIntBoxed(row.get(COL_TPB)) == null ? null : Long.valueOf(toIntBoxed(row.get(COL_TPB)));
            Long tpsIn = toIntBoxed(row.get(COL_TPS)) == null ? null : Long.valueOf(toIntBoxed(row.get(COL_TPS)));

            int taxesPercent = pickTaxesPercent(cat, rkey, tableCfg);
            String source = "tier:" + tier.columnKey();
            Integer priceBuyRaw = null, priceSellRaw = null;
            if (isInternal(cat) && rkey != null && !rkey.isBlank()) {
                var cfg = CalculationsDao.find(cat, rkey);
                String refKey = (cfg != null && cfg.sourceTableKey() != null && !cfg.sourceTableKey().isBlank())
                        ? cfg.sourceTableKey()
                        : rkey;

                var src = loadDetailRows(refKey);
                var ids = src.stream().map(d -> toInt(d.get(COL_ID), -1)).filter(x -> x > 0)
                        .collect(Collectors.toSet());
                if (!ids.isEmpty())
                    priceMap.putAll(Gw2PricesDao.loadTier(new ArrayList<>(ids), tier.columnKey()));

                int[] ev = bagEV(src, priceMap, 0);
                // main tables have no AverageAmount → qty = 1
                writeProfitWithHour(row, ev[0], ev[1]);

                if (DebugTrace.on()) {
                    DebugTrace.refSummary(refKey, "SUM", 0, ev[0], ev[1], src.size());
                }

            } else if (isCompositeRef(cat, rkey)) {
                // compute EV from referenced detail table rows with tax rule applied
                var drops = loadDetailRows(rkey);
                var dropIds = drops.stream().map(d -> toInt(d.get(COL_ID), -1))
                        .filter(x -> x > 0).collect(Collectors.toSet());
                if (!dropIds.isEmpty())
                    priceMap.putAll(Gw2PricesDao.loadTier(new ArrayList<>(dropIds), tier.columnKey()));

                int[] ev = bagEV(drops, priceMap, taxesPercent); // qty = 1 for main tables
                writeProfitWithHour(row, ev[0], ev[1]);

                if (DebugTrace.on()) {
                    DebugTrace.refSummary(rkey, "SUM", taxesPercent, ev[0], ev[1], drops.size());
                }
            } else {
                // leaf item (Id only)
                int id = toInt(row.get(COL_ID), -1);
                if (id > 0) {
                    int[] ps = priceMap.getOrDefault(id, new int[] { 0, 0 });
                    priceBuyRaw = ps[0];
                    priceSellRaw = ps[1];

                    int buyNet = net(ps[0], taxesPercent);
                    int sellNet = net(ps[1], taxesPercent);

                    if (buyNet == 0 && sellNet == 0) {
                        Integer vendor = ItemsDao.vendorValueById(id);
                        if (vendor != null) {
                            buyNet = vendor; // vendor → NO TAX
                            sellNet = vendor;
                            source = "vendor";
                        }
                    }
                    writeProfitWithHour(row, buyNet, sellNet);
                }
            }

            int outBuy = toIntBoxed(row.get(COL_TPB)) == null ? 0 : toIntBoxed(row.get(COL_TPB));
            int outSell = toIntBoxed(row.get(COL_TPS)) == null ? 0 : toIntBoxed(row.get(COL_TPS));
            sumBuy += outBuy;
            sumSell += outSell;

            if (DebugTrace.on() && printed < DebugTrace.limit()
                    && DebugTrace.allow(idIn, name)) {
                DebugTrace.rowLine(
                        i + 1, idIn, name, cat, rkey,
                        null, null, // avg/total not used for main tables
                        tpbIn, tpsIn,
                        source, priceBuyRaw, priceSellRaw, taxesPercent,
                        (long) outBuy, (long) outSell);
                printed++;
            }
        }

        if (tableCfg != null) {
            // keep your “TOTAL” summary logic for main tables as well
            applyAggregation(rows, tableCfg.operation());
        }
        if (DebugTrace.on()) {
            DebugTrace.totalLine(sumBuy, sumSell);
        }

        if (persist) {
            String out = toJson(rows);
            Jpa.txVoid(em -> em.createNativeQuery("""
                        UPDATE public.tables
                           SET rows = :rows, updated_at = now()
                         WHERE key = :k
                    """).setParameter("rows", out)
                    .setParameter("k", tableKey)
                    .executeUpdate());
        }
    }

    /**
     * Recompute one table (detail_tables) identified by (detailFeatureId,
     * tableKey).
     * Uses public.calculations to pick tax% and aggregation op per (category,key).
     */
    public static void recompute(long detailFeatureId, String tableKey, Tier tier, boolean persist) {
        String rowsJson = Jpa.tx(em -> {
            var query = em.createNativeQuery("""
                        SELECT rows FROM public.detail_tables
                         WHERE detail_feature_id = :fid AND key = :k
                    """).setParameter("fid", detailFeatureId).setParameter("k", tableKey).getResultList();
            return query.isEmpty() ? null : (String) query.get(0);
        });
        if (rowsJson == null || rowsJson.isBlank())
            return;

        List<Map<String, Object>> rows = parseRows(rowsJson);

        // Table-level config (tax/op)
        var tableCategory = dominantCategory(rows);
        var tableCfg = CalculationsDao.find(tableCategory, tableKey);

        // Collect leaf IDs (bags/internal handled later)
        Set<Integer> needed = new HashSet<>();
        for (var row : rows) {
            String cat = str(row.get(COL_CAT));
            String key = str(row.get(COL_KEY));
            if (isCompositeRef(cat, key) || isInternal(cat))
                continue;
            int id = toInt(row.get(COL_ID), -1);
            if (id > 0)
                needed.add(id);
        }

        // Load tiered prices for leaves
        Map<Integer, int[]> priceMap = Gw2PricesDao.loadTier(new ArrayList<>(needed), tier.columnKey());

        // --- DEBUG HEADER
        if (DebugTrace.on()) {
            int tpRows = rows.size(); // or tighten if you prefer
            DebugTrace.rowHeader(detailFeatureId, tableKey, rows.size(), tpRows);
        }

        long sumBuy = 0, sumSell = 0;
        int printed = 0;

        // Recompute rows
        for (int i = 0; i < rows.size(); i++) {
            var row = rows.get(i);

            // snapshot of incoming fields (for debug print)
            Integer idIn = toIntBoxed(row.get(COL_ID));
            String name = str(row.get(COL_NAME));
            String cat = str(row.get(COL_CAT));
            String rkey = str(row.get(COL_KEY));
            Double avg = toDouble(row.get(COL_AVG), 0.0);
            Double total = toDouble(
                    row.get(COL_TOTAL_AMOUNT), 0.0);
            Long tpbIn = toIntBoxed(row.get(COL_TPB)) == null ? null : Long.valueOf(toIntBoxed(row.get(COL_TPB)));
            Long tpsIn = toIntBoxed(row.get(COL_TPS)) == null ? null : Long.valueOf(toIntBoxed(row.get(COL_TPS)));

            int taxesPercent = pickTaxesPercent(cat, rkey, tableCfg);
            String source = "tier:" + tier.columnKey();
            Integer priceBuyRaw = null, priceSellRaw = null;

            if (isInternal(cat) && rkey != null && !rkey.isBlank()) {
                var cfg = CalculationsDao.find(cat, rkey);
                String refKey = (cfg != null && cfg.sourceTableKey() != null && !cfg.sourceTableKey().isBlank())
                        ? cfg.sourceTableKey()
                        : rkey;

                var src = loadDetailRows(refKey);
                var ids = src.stream().map(d -> toInt(d.get(COL_ID), -1)).filter(x -> x > 0)
                        .collect(Collectors.toSet());
                if (!ids.isEmpty())
                    priceMap.putAll(Gw2PricesDao.loadTier(new ArrayList<>(ids), tier.columnKey()));

                int[] ev = bagEV(src, priceMap, 0);
                double qty = toDouble(row.get(COL_AVG), 1.0);
                int outBuy = (int) Math.round(ev[0] * qty);
                int outSell = (int) Math.round(ev[1] * qty);
                writeProfit(row, outBuy, outSell);

                if (DebugTrace.on()) {
                    DebugTrace.refSummary(refKey, "SUM", 0, ev[0], ev[1], src.size());
                }
            } else if (isCompositeRef(cat, rkey)) {
                var drops = loadDetailRows(rkey);
                var dropIds = drops.stream().map(d -> toInt(d.get(COL_ID), -1)).filter(x -> x > 0)
                        .collect(Collectors.toSet());
                if (!dropIds.isEmpty())
                    priceMap.putAll(Gw2PricesDao.loadTier(new ArrayList<>(dropIds), tier.columnKey()));

                int[] ev = bagEV(drops, priceMap, taxesPercent);
                double qty = toDouble(row.get(COL_AVG), 1.0);
                int outBuy = (int) Math.round(ev[0] * qty);
                int outSell = (int) Math.round(ev[1] * qty);
                writeProfit(row, outBuy, outSell);

                if (DebugTrace.on()) {
                    DebugTrace.refSummary(rkey, "SUM", taxesPercent, ev[0], ev[1], drops.size());
                }
            } else {
                int id = toInt(row.get(COL_ID), -1);
                if (id > 0) {
                    int[] ps = priceMap.getOrDefault(id, new int[] { 0, 0 });
                    priceBuyRaw = ps[0];
                    priceSellRaw = ps[1];

                    int buyNet = net(ps[0], taxesPercent);
                    int sellNet = net(ps[1], taxesPercent);

                    if (buyNet == 0 && sellNet == 0) {
                        Integer vendor = ItemsDao.vendorValueById(id);
                        if (vendor != null) {
                            buyNet = vendor; // NO TAX on vendor
                            sellNet = vendor;
                            source = "vendor";
                        }
                    }
                    double qty = toDouble(row.get(COL_AVG), 1.0);
                    int outBuy = (int) Math.round(buyNet * qty);
                    int outSell = (int) Math.round(sellNet * qty);
                    writeProfit(row, outBuy, outSell);
                } else if (isCoinRow(row)) {
                    int amt = numericTotalAmount(row);
                    writeProfit(row, amt, amt);
                    source = "coin-total";
                }
            }

            int outBuy = toIntBoxed(row.get(COL_TPB)) == null ? 0 : toIntBoxed(row.get(COL_TPB));
            int outSell = toIntBoxed(row.get(COL_TPS)) == null ? 0 : toIntBoxed(row.get(COL_TPS));
            sumBuy += outBuy;
            sumSell += outSell;

            if (DebugTrace.on() && printed < DebugTrace.limit() && DebugTrace.allow(idIn, name)) {
                DebugTrace.rowLine(
                        i + 1,
                        idIn,
                        name,
                        cat,
                        rkey,
                        avg,
                        total,
                        tpbIn,
                        tpsIn,
                        source,
                        priceBuyRaw,
                        priceSellRaw,
                        taxesPercent,
                        (long) outBuy,
                        (long) outSell);
                printed++;
            }
        }

        // Optional table aggregation rule
        if (tableCfg != null)

        {
            applyAggregation(rows, tableCfg.operation());
        }

        if (DebugTrace.on()) {
            DebugTrace.totalLine(sumBuy, sumSell);
        }

        if (persist) {
            String out = toJson(rows);
            Jpa.txVoid(em -> em.createNativeQuery("""
                        UPDATE public.detail_tables
                           SET rows = :rows, updated_at = now()
                         WHERE detail_feature_id = :fid AND key = :k
                    """).setParameter("rows", out).setParameter("fid", detailFeatureId).setParameter("k", tableKey)
                    .executeUpdate());
        }
    }

    // ===== Helpers =====

    private static boolean isCompositeRef(String category, String key) {
        return category != null && !category.isBlank()
                && key != null && !key.isBlank();
    }

    private static boolean isInternal(String category) {
        return eq(category, "INTERNAL");
    }

    private static String dominantCategory(List<Map<String, Object>> rows) {
        Map<String, Integer> freq = new HashMap<>();
        for (var query : rows) {
            String c = str(query.get(COL_CAT));
            if (c != null && !c.isBlank()) {
                freq.merge(c.trim(), 1, Integer::sum);
            }
        }
        if (freq.isEmpty())
            return "";
        return freq.entrySet().stream()
                .max(Map.Entry.comparingByValue())
                .map(Map.Entry::getKey)
                .orElse("");
    }

    private static int pickTaxesPercent(String category, String key, CalculationsDao.Config tableCfg) {
        // INTERNAL = never tax here
        if (isInternal(category))
            return 0;

        // any row that has both Category and Key => no tax
        if (category != null && !category.isBlank() && key != null && !key.isBlank()) {
            return 0;
        }

        // otherwise (leaf items with empty category/key) fall back to rules/defaults
        var rowCfg = CalculationsDao.find(category, key);
        if (rowCfg != null)
            return clampPercent(rowCfg.taxes());
        if (tableCfg != null)
            return clampPercent(tableCfg.taxes());
        return 15; // default for plain TP leaf rows
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
        return new int[] {
                (int) Math.min(sumBuy, Integer.MAX_VALUE),
                (int) Math.min(sumSell, Integer.MAX_VALUE)
        };
    }

    private static void writeProfit(Map<String, Object> row, int buy, int sell) {
        // integers only
        row.put(COL_TPB, buy);
        row.put(COL_TPS, sell);
    }

    private static void writeProfitWithHour(Map<String, Object> row, int buy, int sell) {
        // write base
        writeProfit(row, buy, sell);

        // derive hours if available
        double hours = toDouble(row.get(COL_HOURS), 0.0);

        int buyHr = (hours > 0.0) ? (int) Math.floor(buy / hours) : buy;
        int sellHr = (hours > 0.0) ? (int) Math.floor(sell / hours) : sell;

        row.put(COL_TPB_HR, buyHr);
        row.put(COL_TPS_HR, sellHr);
    }

    private static void applyAggregation(List<Map<String, Object>> rows, String op) {
        // buckets
        var buyBase = new ArrayList<Integer>();
        var sellBase = new ArrayList<Integer>();
        var buyHr = new ArrayList<Integer>();
        var sellHr = new ArrayList<Integer>();

        for (var query : rows) {
            // base profits
            Integer b = toIntBoxed(query.get(COL_TPB));
            Integer s = toIntBoxed(query.get(COL_TPS));
            if (b != null)
                buyBase.add(b);
            if (s != null)
                sellBase.add(s);

            // prefer existing hourly if present
            Integer bh = query.containsKey(COL_TPB_HR) ? toIntBoxed(query.get(COL_TPB_HR)) : null;
            Integer sh = query.containsKey(COL_TPS_HR) ? toIntBoxed(query.get(COL_TPS_HR)) : null;

            // fallback: derive hourly from base and Duration if needed
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

        if (buyBase.isEmpty() && sellBase.isEmpty() && buyHr.isEmpty() && sellHr.isEmpty()) {
            return;
        }

        String agg = (op == null ? "SUM" : op.toUpperCase());
        java.util.function.Function<List<Integer>, Integer> AGG = xs -> {
            if (xs == null || xs.isEmpty())
                return 0;
            return switch (agg) {
                case "AVG" -> avg(xs);
                case "MIN" -> xs.stream().min(Integer::compare).orElse(0);
                case "MAX" -> xs.stream().max(Integer::compare).orElse(0);
                default -> sum(xs); // SUM
            };
        };

        int aggBuyBase = AGG.apply(buyBase);
        int aggSellBase = AGG.apply(sellBase);
        Integer aggBuyHr = buyHr.isEmpty() ? null : AGG.apply(buyHr);
        Integer aggSellHr = sellHr.isEmpty() ? null : AGG.apply(sellHr);

        // upsert TOTAL row
        Map<String, Object> total = rows.stream()
                .filter(query -> "TOTAL".equalsIgnoreCase(str(query.get(COL_KEY)))
                        || "TOTAL".equalsIgnoreCase(str(query.get(COL_NAME))))
                .findFirst()
                .orElseGet(() -> {
                    var t = new LinkedHashMap<String, Object>();
                    t.put(COL_KEY, "TOTAL");
                    t.put(COL_NAME, "TOTAL");
                    rows.add(t);
                    return t;
                });

        // write base totals
        total.put(COL_TPB, aggBuyBase);
        total.put(COL_TPS, aggSellBase);

        // write hourly totals if we managed to compute any
        if (aggBuyHr != null)
            total.put(COL_TPB_HR, aggBuyHr);
        if (aggSellHr != null)
            total.put(COL_TPS_HR, aggSellHr);
    }

    // DETAIL: core compute → rows
    private static List<Map<String, Object>> recomputeDetailCore(long fid, String key, Tier tier) {
        // this is your existing recompute(...) logic up to BEFORE writing to DB,
        // but instead of writing, return the computed `rows`.
        // Easiest path: copy your current recompute(...) body, remove the final UPDATE,
        // and return `rows` at the end.

        // 1) load rows JSON
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

        // --- paste your current detail-table compute loop here ---
        // re-use your existing helpers (dominantCategory, pickTaxesPercent, bagEV,
        // writeProfit, etc.)
        // NOTE: remove the final DB UPDATE; we’re returning rows instead.

        // (BEGIN your current body)
        var tableCategory = dominantCategory(rows);
        var tableCfg = CalculationsDao.find(tableCategory, key);

        Set<Integer> needed = new HashSet<>();
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

        if (DebugTrace.on()) {
            DebugTrace.rowHeader(fid, key, rows.size(), rows.size());
        }

        long sumBuy = 0, sumSell = 0;
        int printed = 0;

        for (int i = 0; i < rows.size(); i++) {
            var row = rows.get(i);

            Integer idIn = toIntBoxed(row.get(COL_ID));
            String name = str(row.get(COL_NAME));
            String cat = str(row.get(COL_CAT));
            String rkey = str(row.get(COL_KEY));
            Double avg = toDouble(row.get(COL_AVG), 0.0);
            Double total = toDouble(row.get(COL_TOTAL_AMOUNT), 0.0);
            Long tpbIn = toIntBoxed(row.get(COL_TPB)) == null ? null : Long.valueOf(toIntBoxed(row.get(COL_TPB)));
            Long tpsIn = toIntBoxed(row.get(COL_TPS)) == null ? null : Long.valueOf(toIntBoxed(row.get(COL_TPS)));

            int taxesPercent = pickTaxesPercent(cat, rkey, tableCfg);
            String source = "tier:" + tier.columnKey();
            Integer priceBuyRaw = null, priceSellRaw = null;

            if (isCompositeRef(cat, rkey)) {
                var drops = loadDetailRows(rkey);
                var dropIds = drops.stream().map(d -> toInt(d.get(COL_ID), -1)).filter(x -> x > 0)
                        .collect(Collectors.toSet());
                if (!dropIds.isEmpty())
                    priceMap.putAll(Gw2PricesDao.loadTier(new ArrayList<>(dropIds), tier.columnKey()));

                int[] ev = bagEV(drops, priceMap, taxesPercent);
                double qty = toDouble(row.get(COL_AVG), 1.0);
                int outBuy = (int) Math.round(ev[0] * qty);
                int outSell = (int) Math.round(ev[1] * qty);
                writeProfit(row, outBuy, outSell);
                if (DebugTrace.on())
                    DebugTrace.refSummary(rkey, "SUM", taxesPercent, ev[0], ev[1], drops.size());

            } else if (isInternal(cat) && rkey != null && !rkey.isBlank()) {
                var cfg = CalculationsDao.find(cat, rkey);
                String refKey = (cfg != null && cfg.sourceTableKey() != null && !cfg.sourceTableKey().isBlank())
                        ? cfg.sourceTableKey()
                        : rkey;

                var src = loadDetailRows(refKey);
                var ids = src.stream().map(d -> toInt(d.get(COL_ID), -1)).filter(x -> x > 0)
                        .collect(Collectors.toSet());
                if (!ids.isEmpty())
                    priceMap.putAll(Gw2PricesDao.loadTier(new ArrayList<>(ids), tier.columnKey()));

                int[] ev = bagEV(src, priceMap, 0);
                double qty = toDouble(row.get(COL_AVG), 1.0);
                int outBuy = (int) Math.round(ev[0] * qty);
                int outSell = (int) Math.round(ev[1] * qty);
                writeProfit(row, outBuy, outSell);
                if (DebugTrace.on())
                    DebugTrace.refSummary(refKey, "SUM", 0, ev[0], ev[1], src.size());

            } else {
                int id = toInt(row.get(COL_ID), -1);
                if (id > 0) {
                    int[] ps = priceMap.getOrDefault(id, new int[] { 0, 0 });
                    priceBuyRaw = ps[0];
                    priceSellRaw = ps[1];

                    int buyNet = net(ps[0], taxesPercent);
                    int sellNet = net(ps[1], taxesPercent);

                    if (buyNet == 0 && sellNet == 0) {
                        Integer vendor = ItemsDao.vendorValueById(id);
                        if (vendor != null) {
                            buyNet = vendor;
                            sellNet = vendor;
                            source = "vendor";
                        }
                    }
                    double qty = toDouble(row.get(COL_AVG), 1.0);
                    writeProfit(row, (int) Math.round(buyNet * qty), (int) Math.round(sellNet * qty));

                } else if (isCoinRow(row)) {
                    int amt = numericTotalAmount(row);
                    writeProfit(row, amt, amt);
                    source = "coin-total";
                }
            }

            int outBuy = toIntBoxed(row.get(COL_TPB)) == null ? 0 : toIntBoxed(row.get(COL_TPB));
            int outSell = toIntBoxed(row.get(COL_TPS)) == null ? 0 : toIntBoxed(row.get(COL_TPS));
            sumBuy += outBuy;
            sumSell += outSell;

            if (DebugTrace.on() && printed < DebugTrace.limit() && DebugTrace.allow(idIn, name)) {
                DebugTrace.rowLine(i + 1, idIn, name, cat, rkey, avg, total, tpbIn, tpsIn, source, priceBuyRaw,
                        priceSellRaw, taxesPercent, (long) outBuy, (long) outSell);
                printed++;
            }
        }

        // aggregation (TOTAL row)
        tableCategory = dominantCategory(rows);
        tableCfg = CalculationsDao.find(tableCategory, key);
        if (tableCfg != null)
            applyAggregation(rows, tableCfg.operation());

        if (DebugTrace.on())
            DebugTrace.totalLine(sumBuy, sumSell);

        // (END your current body)

        return rows;
    }

    // DETAIL: public helpers
    public static String recomputeDetailJson(long fid, String key, Tier tier) {
        var rows = recomputeDetailCore(fid, key, tier);
        return toJson(rows);
    }

    public static void recomputeDetailPersist(long fid, String key, Tier tier) {
        var rows = recomputeDetailCore(fid, key, tier);
        String out = toJson(rows);
        Jpa.txVoid(em -> em.createNativeQuery("""
                    UPDATE public.detail_tables
                       SET rows = :rows, updated_at = now()
                     WHERE detail_feature_id = :fid AND key = :k
                """).setParameter("rows", out)
                .setParameter("fid", fid)
                .setParameter("k", key)
                .executeUpdate());
    }

    // MAIN: core compute → rows
    private static List<Map<String, Object>> recomputeMainCore(String key, Tier tier) {
        String rowsJson = Jpa.tx(em -> {
            var r = em.createNativeQuery("""
                        SELECT rows FROM public.tables
                         WHERE key = :k
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

        Set<Integer> needed = new HashSet<>();
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

        if (DebugTrace.on())
            DebugTrace.rowHeader(-1L, key, rows.size(), rows.size());

        long sumBuy = 0, sumSell = 0;
        int printed = 0;

        for (int i = 0; i < rows.size(); i++) {
            var row = rows.get(i);
            Integer idIn = toIntBoxed(row.get(COL_ID));
            String name = str(row.get(COL_NAME));
            String cat = str(row.get(COL_CAT));
            String rkey = str(row.get(COL_KEY));
            Long tpbIn = toIntBoxed(row.get(COL_TPB)) == null ? null : Long.valueOf(toIntBoxed(row.get(COL_TPB)));
            Long tpsIn = toIntBoxed(row.get(COL_TPS)) == null ? null : Long.valueOf(toIntBoxed(row.get(COL_TPS)));

            int taxesPercent = pickTaxesPercent(cat, rkey, tableCfg);
            String source = "tier:" + tier.columnKey();
            Integer priceBuyRaw = null, priceSellRaw = null;

            if (isCompositeRef(cat, rkey)) {
                var drops = loadDetailRows(rkey);
                var dropIds = drops.stream().map(d -> toInt(d.get(COL_ID), -1)).filter(x -> x > 0)
                        .collect(Collectors.toSet());
                if (!dropIds.isEmpty())
                    priceMap.putAll(Gw2PricesDao.loadTier(new ArrayList<>(dropIds), tier.columnKey()));
                int[] ev = bagEV(drops, priceMap, taxesPercent);
                writeProfitWithHour(row, ev[0], ev[1]);
                if (DebugTrace.on())
                    DebugTrace.refSummary(rkey, "SUM", taxesPercent, ev[0], ev[1], drops.size());

            } else if (isInternal(cat) && rkey != null && !rkey.isBlank()) {
                var cfg = CalculationsDao.find(cat, rkey);
                String refKey = (cfg != null && cfg.sourceTableKey() != null && !cfg.sourceTableKey().isBlank())
                        ? cfg.sourceTableKey()
                        : rkey;

                var src = loadDetailRows(refKey);
                var ids = src.stream().map(d -> toInt(d.get(COL_ID), -1)).filter(x -> x > 0)
                        .collect(Collectors.toSet());
                if (!ids.isEmpty())
                    priceMap.putAll(Gw2PricesDao.loadTier(new ArrayList<>(ids), tier.columnKey()));
                int[] ev = bagEV(src, priceMap, 0);
                writeProfitWithHour(row, ev[0], ev[1]);
                if (DebugTrace.on())
                    DebugTrace.refSummary(refKey, "SUM", 0, ev[0], ev[1], src.size());

            } else {
                int id = toInt(row.get(COL_ID), -1);
                if (id > 0) {
                    int[] ps = priceMap.getOrDefault(id, new int[] { 0, 0 });
                    priceBuyRaw = ps[0];
                    priceSellRaw = ps[1];
                    int buyNet = net(ps[0], taxesPercent);
                    int sellNet = net(ps[1], taxesPercent);
                    if (buyNet == 0 && sellNet == 0) {
                        Integer vendor = ItemsDao.vendorValueById(id);
                        if (vendor != null) {
                            buyNet = vendor;
                            sellNet = vendor;
                            source = "vendor";
                        }
                    }
                    writeProfitWithHour(row, buyNet, sellNet);
                }
            }

            int outBuy = toIntBoxed(row.get(COL_TPB)) == null ? 0 : toIntBoxed(row.get(COL_TPB));
            int outSell = toIntBoxed(row.get(COL_TPS)) == null ? 0 : toIntBoxed(row.get(COL_TPS));
            sumBuy += outBuy;
            sumSell += outSell;

            if (DebugTrace.on() && printed < DebugTrace.limit() && DebugTrace.allow(idIn, name)) {
                DebugTrace.rowLine(i + 1, idIn, name, cat, rkey, null, null, tpbIn, tpsIn, source, priceBuyRaw,
                        priceSellRaw, taxesPercent, (long) outBuy, (long) outSell);
                printed++;
            }
        }

        if (tableCfg != null)
            applyAggregation(rows, tableCfg.operation());
        if (DebugTrace.on())
            DebugTrace.totalLine(sumBuy, sumSell);

        return rows;
    }

    // MAIN: public helpers
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
                     WHERE key = :k
                """).setParameter("rows", out)
                .setParameter("k", key)
                .executeUpdate());
    }

    private static int sum(List<Integer> xs) {
        long s = 0;
        for (int v : xs)
            s += v;
        return (int) Math.min(s, Integer.MAX_VALUE);
    }

    private static int avg(List<Integer> xs) {
        if (xs.isEmpty())
            return 0;
        long s = 0;
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

    // JSON load/save and utils

    private static List<Map<String, Object>> parseRows(String json) {
        try {
            return OBJECT_MAPPER.readValue(json, new TypeReference<List<Map<String, Object>>>() {
            });
        } catch (Exception e) {
            throw new RuntimeException("Failed to parse detail_tables.rows JSON", e);
        }
    }

    private static String toJson(List<Map<String, Object>> rows) {
        try {
            return OBJECT_MAPPER.writeValueAsString(rows);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    private static List<Map<String, Object>> loadDetailRows(String key) {
        if (key == null || key.isBlank())
            return List.of();
        String json = Jpa.tx(em -> {
            var query = em.createNativeQuery("""
                        SELECT rows FROM public.detail_tables
                         WHERE key = :k
                         ORDER BY id DESC
                         LIMIT 1
                    """).setParameter("k", key).getResultList();
            return query.isEmpty() ? null : (String) query.get(0);
        });
        if (json == null || json.isBlank())
            return List.of();
        return parseRows(json);
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
}

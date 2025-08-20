package eu.fast.gw2.dynamic;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import eu.fast.gw2.dao.CalculationsDao;
import eu.fast.gw2.dao.Gw2PricesDao;
import eu.fast.gw2.dao.ItemsDao;
import eu.fast.gw2.jpa.Jpa;
import eu.fast.gw2.main.DebugTrace;

import java.util.*;
import java.util.stream.Collectors;

public class OverlayEngine {

    private static final ObjectMapper M = new ObjectMapper();

    // Column names (canonical)
    private static final String COL_ID = "Id";
    private static final String COL_CAT = "Category";
    private static final String COL_KEY = "Key";
    private static final String COL_NAME = "Name";
    private static final String COL_AVG = "AverageAmount";
    private static final String COL_TPB = "TPBuyProfit";
    private static final String COL_TPS = "TPSellProfit";
    private static final String COL_TOTAL_AMOUNT = "Total Amount";
    private static final String COL_TOTAL_AMOUNT2 = "TotalAmount"; // seen in some sheets

    /**
     * Recompute one table (detail_tables) identified by (detailFeatureId,
     * tableKey).
     * Uses public.calculations to pick tax% and aggregation op per (category,key).
     */
    public static void recompute(long detailFeatureId, String tableKey) {
        // 1) Load parent table JSON
        String rowsJson = Jpa.tx(em -> {
            var r = em.createNativeQuery("""
                        SELECT rows FROM public.detail_tables
                         WHERE detail_feature_id = :fid AND key = :k
                    """).setParameter("fid", detailFeatureId)
                    .setParameter("k", tableKey)
                    .getResultList();
            return r.isEmpty() ? null : (String) r.get(0);
        });
        if (rowsJson == null || rowsJson.isBlank())
            return;

        List<Map<String, Object>> rows = parseRows(rowsJson);

        // Determine this table's canonical category (most common non-empty)
        String tableCategory = dominantCategory(rows);

        // Find per-table config (taxes/op) if defined
        var tableCfg = CalculationsDao.find(tableCategory, tableKey);

        // 2) Collect all item ids needed for leaf computations
        Set<Integer> needed = new HashSet<>();
        for (var row : rows) {
            var id = toInt(row.get(COL_ID), -1);
            var cat = str(row.get(COL_CAT));
            var key = str(row.get(COL_KEY));
            if (isBag(cat, key)) {
                // bag EV from child table; ids will be collected after loading drops
                continue;
            }
            if (isInternal(cat)) {
                // INTERNAL handled via source_table_key or zero—no ids here
                continue;
            }
            if (id > 0)
                needed.add(id);
        }

        // Load base price cache for leaf ids
        Map<Integer, int[]> priceMap = Gw2PricesDao.loadLatest(needed); // id -> [buy,sell] in copper

        // ---- TRACE header ----
        int tpRows = countRowsWithTpFields(rows);
        DebugTrace.rowHeader(detailFeatureId, tableKey, rows.size(), tpRows);
        long sumBuy = 0, sumSell = 0;
        int printed = 0;

        // 3) Recompute each row
        for (int i = 0; i < rows.size(); i++) {
            var row = rows.get(i);

            // Extract common fields (for trace & logic)
            Integer id = toIntBoxed(row.get(COL_ID));
            String name = str(row.get(COL_NAME));
            String cat = str(row.get(COL_CAT));
            String key = str(row.get(COL_KEY));

            Double avg = toDoubleBoxed(row.get(COL_AVG));
            Double total = totalAmountboxed(row); // handles 2 variants

            Long tpbIn = toLongBoxed(row.get(COL_TPB));
            Long tpsIn = toLongBoxed(row.get(COL_TPS));

            // pick taxes%: INTERNAL always 0, else row-specific cfg or table cfg or default
            // 15
            int taxesPercent = pickTaxesPercent(cat, key, tableCfg);

            // Values for trace:
            String sourcePath = "n/a";
            Integer priceBuy = null, priceSell = null;
            int taxPctUsed = taxesPercent;

            Long tpbOut = null, tpsOut = null;

            if (isBag(cat, key)) {
                // Compute EV from child table = the SAME key
                List<Map<String, Object>> drops = loadDetailRows(key);
                var dropIds = drops.stream()
                        .map(d -> toInt(d.get(COL_ID), -1))
                        .filter(x -> x > 0)
                        .collect(Collectors.toSet());
                if (!dropIds.isEmpty()) {
                    priceMap.putAll(Gw2PricesDao.loadLatest(dropIds));
                }
                int[] ev = bagEV(drops, priceMap, taxesPercent);
                tpbOut = (long) ev[0];
                tpsOut = (long) ev[1];
                sourcePath = "ref(bag/" + key + ")";
                // Optional: trace summary of the referenced table EV
                if (DebugTrace.on() && DebugTrace.allow(id, name)) {
                    DebugTrace.refSummary(key, "SUM", taxesPercent, tpbOut, tpsOut, drops.size());
                }
                writeProfit(row, ev[0], ev[1]);
            } else if (isInternal(cat)) {
                // INTERNAL: taxes forced to 0
                taxPctUsed = 0;
                var cfg = CalculationsDao.find(cat, key);
                if (cfg != null && cfg.sourceTableKey() != null && !cfg.sourceTableKey().isBlank()) {
                    String refKey = cfg.sourceTableKey();
                    List<Map<String, Object>> src = loadDetailRows(refKey);
                    var ids = src.stream().map(d -> toInt(d.get(COL_ID), -1)).filter(x -> x > 0)
                            .collect(Collectors.toSet());
                    if (!ids.isEmpty())
                        priceMap.putAll(Gw2PricesDao.loadLatest(ids));
                    int[] ev = bagEV(src, priceMap, 0); // INTERNAL derives without tax at this level
                    tpbOut = (long) ev[0];
                    tpsOut = (long) ev[1];
                    sourcePath = "INTERNAL(" + refKey + ")";
                    if (DebugTrace.on() && DebugTrace.allow(id, name)) {
                        DebugTrace.refSummary(refKey, "SUM", 0, tpbOut, tpsOut, src.size());
                    }
                    writeProfit(row, ev[0], ev[1]);
                } else {
                    tpbOut = 0L;
                    tpsOut = 0L;
                    sourcePath = "INTERNAL(0)";
                    writeProfit(row, 0, 0);
                }
            } else if (id != null && id > 0) {
                // Leaf item (normal TP row): id > 0
                int[] ps = priceMap.getOrDefault(id, new int[] { 0, 0 });
                priceBuy = ps[0];
                priceSell = ps[1];
                int buyNet = net(ps[0], taxesPercent);
                int sellNet = net(ps[1], taxesPercent);
                if (buyNet == 0 && sellNet == 0) {
                    // vendor fallback if available (apply same tax rule)
                    Integer vendor = ItemsDao.vendorValueById(id);
                    if (vendor != null) {
                        priceBuy = vendor;
                        priceSell = vendor;
                        buyNet = net(vendor, taxesPercent);
                        sellNet = net(vendor, taxesPercent);
                        sourcePath = "vendor(" + id + ")";
                    } else {
                        sourcePath = "gw2_prices(" + id + ")";
                    }
                } else {
                    sourcePath = "gw2_prices(" + id + ")";
                }
                tpbOut = (long) buyNet;
                tpsOut = (long) sellNet;
                writeProfit(row, buyNet, sellNet);
            } else if (isCoinRow(row)) {
                int amt = numericTotalAmount(row);
                tpbOut = (long) amt;
                tpsOut = (long) amt;
                sourcePath = "coin(total_amount)";
                writeProfit(row, amt, amt);
            } else {
                // unknown row, leave as-is
                sourcePath = "n/a";
            }

            sumBuy += (tpbOut == null ? 0 : tpbOut);
            sumSell += (tpsOut == null ? 0 : tpsOut);

            // ---- TRACE row ----
            if (DebugTrace.on() && printed < DebugTrace.limit() && DebugTrace.allow(id, name)) {
                DebugTrace.rowLine(
                        i + 1,
                        id, name, cat, key,
                        avg, total,
                        tpbIn, tpsIn,
                        sourcePath,
                        priceBuy, priceSell,
                        taxPctUsed,
                        tpbOut, tpsOut);
                printed++;
            }
        }

        // 4) Apply table-level aggregation if configured
        if (tableCfg != null) {
            applyAggregation(rows, tableCfg.operation());
        }

        // 5) Persist
        String out = toJson(rows);
        Jpa.txVoid(em -> em.createNativeQuery("""
                    UPDATE public.detail_tables
                       SET rows = :rows, updated_at = now()
                     WHERE detail_feature_id = :fid AND key = :k
                """).setParameter("rows", out)
                .setParameter("fid", detailFeatureId)
                .setParameter("k", tableKey)
                .executeUpdate());

        // ---- TRACE totals ----
        DebugTrace.totalLine(sumBuy, sumSell);
    }

    // ===== Helpers =====

    private static boolean isBag(String category, String key) {
        return eq(category, "bag") && key != null && !key.isBlank();
    }

    private static boolean isInternal(String category) {
        return eq(category, "INTERNAL");
    }

    private static String dominantCategory(List<Map<String, Object>> rows) {
        Map<String, Integer> freq = new HashMap<>();
        for (var r : rows) {
            String c = str(r.get(COL_CAT));
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
        if (isInternal(category))
            return 0; // never double tax
        var rowCfg = CalculationsDao.find(category, key);
        if (rowCfg != null)
            return clampPercent(rowCfg.taxes());
        if (tableCfg != null)
            return clampPercent(tableCfg.taxes());
        return 15; // default
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

    private static void applyAggregation(List<Map<String, Object>> rows, String op) {
        var buys = new ArrayList<Integer>();
        var sells = new ArrayList<Integer>();
        for (var r : rows) {
            Integer b = toIntBoxed(r.get(COL_TPB));
            Integer s = toIntBoxed(r.get(COL_TPS));
            if (b != null)
                buys.add(b);
            if (s != null)
                sells.add(s);
        }
        if (buys.isEmpty() && sells.isEmpty())
            return;

        int aggBuy, aggSell;
        switch ((op == null ? "SUM" : op.toUpperCase())) {
            case "AVG" -> {
                aggBuy = avg(buys);
                aggSell = avg(sells);
            }
            case "MIN" -> {
                aggBuy = buys.stream().min(Integer::compare).orElse(0);
                aggSell = sells.stream().min(Integer::compare).orElse(0);
            }
            case "MAX" -> {
                aggBuy = buys.stream().max(Integer::compare).orElse(0);
                aggSell = sells.stream().max(Integer::compare).orElse(0);
            }
            default -> {
                aggBuy = sum(buys);
                aggSell = sum(sells);
            }
        }

        // upsert a TOTAL row
        Map<String, Object> total = rows.stream()
                .filter(r -> "TOTAL".equalsIgnoreCase(str(r.get(COL_KEY)))
                        || "TOTAL".equalsIgnoreCase(str(r.get(COL_NAME))))
                .findFirst().orElse(null);
        if (total == null) {
            total = new LinkedHashMap<>();
            total.put(COL_KEY, "TOTAL");
            total.put(COL_NAME, "TOTAL");
            rows.add(total);
        }
        writeProfit(total, aggBuy, aggSell);
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
        Object v = row.containsKey(COL_TOTAL_AMOUNT) ? row.get(COL_TOTAL_AMOUNT) : row.get(COL_TOTAL_AMOUNT2);
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
            return M.readValue(json, new TypeReference<List<Map<String, Object>>>() {
            });
        } catch (Exception e) {
            throw new RuntimeException("Failed to parse detail_tables.rows JSON", e);
        }
    }

    private static String toJson(List<Map<String, Object>> rows) {
        try {
            return M.writeValueAsString(rows);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    private static List<Map<String, Object>> loadDetailRows(String key) {
        if (key == null || key.isBlank())
            return List.of();
        String json = Jpa.tx(em -> {
            var r = em.createNativeQuery("""
                        SELECT rows FROM public.detail_tables
                         WHERE key = :k
                         ORDER BY id DESC
                         LIMIT 1
                    """).setParameter("k", key).getResultList();
            return r.isEmpty() ? null : (String) r.get(0);
        });
        if (json == null || json.isBlank())
            return List.of();
        return parseRows(json);
    }

    // tiny helpers

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

    private static Long toLongBoxed(Object o) {
        if (o == null)
            return null;
        if (o instanceof Number n)
            return n.longValue();
        try {
            return (long) Math.floor(Double.parseDouble(String.valueOf(o).replace(',', '.')));
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

    private static Double toDoubleBoxed(Object o) {
        if (o == null)
            return null;
        if (o instanceof Number n)
            return n.doubleValue();
        try {
            return Double.valueOf(String.valueOf(o).replace(',', '.'));
        } catch (Exception e) {
            return null;
        }
    }

    private static Double totalAmountboxed(Map<String, Object> row) {
        Object v = row.containsKey(COL_TOTAL_AMOUNT) ? row.get(COL_TOTAL_AMOUNT) : row.get(COL_TOTAL_AMOUNT2);
        return toDoubleBoxed(v);
    }

    private static int countRowsWithTpFields(List<Map<String, Object>> rows) {
        int c = 0;
        for (var r : rows) {
            if (r.containsKey(COL_TPB) || r.containsKey(COL_TPS))
                c++;
        }
        return c;
    }
}

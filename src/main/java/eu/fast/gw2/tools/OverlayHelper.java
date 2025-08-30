package eu.fast.gw2.tools;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

public class OverlayHelper {

    // Column names (detail_tables)
    public static final String COL_ID = "Id";
    public static final String COL_IMAGE = "Image";
    public static final String COL_RARITY = "Rarity";
    public static final String COL_CAT = "Category";
    public static final String COL_KEY = "Key";
    public static final String COL_NAME = "Name";
    public static final String COL_AVG = "AverageAmount";
    public static final String COL_TOTAL_AMOUNT = "TotalAmount";
    public static final String COL_BEST_BUY = "BestChoiceBuy";
    public static final String COL_BEST_SELL = "BestChoiceSell";

    // ===== NEW 2×2 base columns =====
    // Canonical "pair" to think about: ItemBuyProfitTPBuyProfit (buy-side) &
    // ItemSellProfitTPSellProfit (sell-side)
    public static final String COL_ITEM_SELL_TPBUY = "ItemSellProfitTPBuyProfit";
    public static final String COL_ITEM_BUY_TPBUY = "ItemBuyProfitTPBuyProfit";
    public static final String COL_ITEM_SELL_TPSELL = "ItemSellProfitTPSellProfit";
    public static final String COL_ITEM_BUY_TPSELL = "ItemBuyProfitTPSellProfit";

    // Column names (tables - main) — per-hour variants for all four
    public static final String COL_ITEM_SELL_TPBUY_HR = "ItemSellProfitTPBuyProfitHr";
    public static final String COL_ITEM_BUY_TPBUY_HR = "ItemBuyProfitTPBuyProfitHr";
    public static final String COL_ITEM_SELL_TPSELL_HR = "ItemSellProfitTPSellProfitHr";
    public static final String COL_ITEM_BUY_TPSELL_HR = "ItemBuyProfitTPSellProfitHr";
    public static final String COL_HOURS = "Duration";

    // ===== NEW 2×2 Spirit Shard augments (Option B): four wSS columns (+Hr for
    // main) =====
    public static final String COL_ITEM_SELL_TPBUY_WSS = "ItemSellProfitTPBuyProfitwSS";
    public static final String COL_ITEM_BUY_TPBUY_WSS = "ItemBuyProfitTPBuyProfitwSS";
    public static final String COL_ITEM_SELL_TPSELL_WSS = "ItemSellProfitTPSellProfitwSS";
    public static final String COL_ITEM_BUY_TPSELL_WSS = "ItemBuyProfitTPSellProfitwSS";

    public static final String COL_ITEM_SELL_TPBUY_WSS_HR = "ItemSellProfitTPBuyProfitwSSHr";
    public static final String COL_ITEM_BUY_TPBUY_WSS_HR = "ItemBuyProfitTPBuyProfitwSSHr";
    public static final String COL_ITEM_SELL_TPSELL_WSS_HR = "ItemSellProfitTPSellProfitwSSHr";
    public static final String COL_ITEM_BUY_TPSELL_WSS_HR = "ItemBuyProfitTPSellProfitwSSHr";

    public static String str(Object o) {
        return o == null ? null : String.valueOf(o);
    }

    public static boolean eq(String a, String b) {
        return a != null && b != null && a.equalsIgnoreCase(b);
    }

    public static int toInt(Object o, int def) {
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

    public static Integer toIntBoxed(Object o) {
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

    public static Double toDouble(Object o, double def) {
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

    public static boolean isCompositeRef(String category, String key) {
        return category != null && !category.isBlank() && key != null && !key.isBlank();
    }

    public static boolean isInternal(String category) {
        return eq(category, "INTERNAL");
    }

    public static String dominantCategory(List<Map<String, Object>> rows) {
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

    public static int clampPercent(int p) {
        if (p < 0)
            return 0;
        if (p > 100)
            return 100;
        return p;
    }

    /** Historical "net": keep old behavior for bag EV (never negative). */
    public static int net(int value, int taxesPercent) {
        if (value <= 0)
            return 0;
        if (taxesPercent <= 0)
            return value;
        double f = 1.0 - (taxesPercent / 100.0);
        return (int) Math.floor(value * f);
    }

    /** Bag EV still uses non-negative semantics to match historical composites. */
    public static int[] bagEV(List<Map<String, Object>> drops, Map<Integer, int[]> priceMap, int taxesPercent) {
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

    // ===== Writers for new 2×2 results =====

    public static void writeFour(Map<String, Object> row,
            int itemBuy_tpBuy,
            int itemSell_tpBuy,
            int itemBuy_tpSell,
            int itemSell_tpSell) {
        row.put(COL_ITEM_BUY_TPBUY, itemBuy_tpBuy);
        row.put(COL_ITEM_SELL_TPBUY, itemSell_tpBuy);
        row.put(COL_ITEM_BUY_TPSELL, itemBuy_tpSell);
        row.put(COL_ITEM_SELL_TPSELL, itemSell_tpSell);
    }

    public static void writeFourWithHour(Map<String, Object> row,
            int itemBuy_tpBuy,
            int itemSell_tpBuy,
            int itemBuy_tpSell,
            int itemSell_tpSell) {
        writeFour(row, itemBuy_tpBuy, itemSell_tpBuy, itemBuy_tpSell, itemSell_tpSell);
        double hours = toDouble(row.get(COL_HOURS), 0.0);

        int v;

        v = (hours > 0.0) ? (int) Math.floor(itemSell_tpBuy / hours) : itemSell_tpBuy;
        row.put(COL_ITEM_SELL_TPBUY_HR, v);

        v = (hours > 0.0) ? (int) Math.floor(itemBuy_tpBuy / hours) : itemBuy_tpBuy;
        row.put(COL_ITEM_BUY_TPBUY_HR, v);

        v = (hours > 0.0) ? (int) Math.floor(itemSell_tpSell / hours) : itemSell_tpSell;
        row.put(COL_ITEM_SELL_TPSELL_HR, v);

        v = (hours > 0.0) ? (int) Math.floor(itemBuy_tpSell / hours) : itemBuy_tpSell;
        row.put(COL_ITEM_BUY_TPSELL_HR, v);
    }

    // ===== Aggregation across rows (TOTAL, BestChoice) =====

    public static void applyAggregation(List<Map<String, Object>> rows, String op) {
        // collectors for base
        var c_IS_TPB = new ArrayList<Integer>();
        var c_IB_TPB = new ArrayList<Integer>();
        var c_IS_TPS = new ArrayList<Integer>();
        var c_IB_TPS = new ArrayList<Integer>();

        // collectors for per-hour (may be absent; compute if hours>0 and base present)
        var c_IS_TPB_HR = new ArrayList<Integer>();
        var c_IB_TPB_HR = new ArrayList<Integer>();
        var c_IS_TPS_HR = new ArrayList<Integer>();
        var c_IB_TPS_HR = new ArrayList<Integer>();

        // wSS collectors
        var c_IS_TPB_WSS = new ArrayList<Integer>();
        var c_IB_TPB_WSS = new ArrayList<Integer>();
        var c_IS_TPS_WSS = new ArrayList<Integer>();
        var c_IB_TPS_WSS = new ArrayList<Integer>();

        var c_IS_TPB_WSS_HR = new ArrayList<Integer>();
        var c_IB_TPB_WSS_HR = new ArrayList<Integer>();
        var c_IS_TPS_WSS_HR = new ArrayList<Integer>();
        var c_IB_TPS_WSS_HR = new ArrayList<Integer>();

        for (var r : rows) {
            // base
            Integer sTPB = toIntBoxed(r.get(COL_ITEM_SELL_TPBUY));
            Integer bTPB = toIntBoxed(r.get(COL_ITEM_BUY_TPBUY));
            Integer sTPS = toIntBoxed(r.get(COL_ITEM_SELL_TPSELL));
            Integer bTPS = toIntBoxed(r.get(COL_ITEM_BUY_TPSELL));
            if (sTPB != null)
                c_IS_TPB.add(sTPB);
            if (bTPB != null)
                c_IB_TPB.add(bTPB);
            if (sTPS != null)
                c_IS_TPS.add(sTPS);
            if (bTPS != null)
                c_IB_TPS.add(bTPS);

            // hours (use provided, or compute if hours>0)
            Integer sTPB_hr = toIntBoxed(r.get(COL_ITEM_SELL_TPBUY_HR));
            Integer bTPB_hr = toIntBoxed(r.get(COL_ITEM_BUY_TPBUY_HR));
            Integer sTPS_hr = toIntBoxed(r.get(COL_ITEM_SELL_TPSELL_HR));
            Integer bTPS_hr = toIntBoxed(r.get(COL_ITEM_BUY_TPSELL_HR));

            if (sTPB_hr == null || bTPB_hr == null || sTPS_hr == null || bTPS_hr == null) {
                double hours = toDouble(r.get(COL_HOURS), 0.0);
                if (hours > 0.0) {
                    if (sTPB_hr == null && sTPB != null)
                        sTPB_hr = (int) Math.floor(sTPB / hours);
                    if (bTPB_hr == null && bTPB != null)
                        bTPB_hr = (int) Math.floor(bTPB / hours);
                    if (sTPS_hr == null && sTPS != null)
                        sTPS_hr = (int) Math.floor(sTPS / hours);
                    if (bTPS_hr == null && bTPS != null)
                        bTPS_hr = (int) Math.floor(bTPS / hours);
                }
            }
            if (sTPB_hr != null)
                c_IS_TPB_HR.add(sTPB_hr);
            if (bTPB_hr != null)
                c_IB_TPB_HR.add(bTPB_hr);
            if (sTPS_hr != null)
                c_IS_TPS_HR.add(sTPS_hr);
            if (bTPS_hr != null)
                c_IB_TPS_HR.add(bTPS_hr);

            // wSS base
            Integer sTPB_wss = toIntBoxed(r.get(COL_ITEM_SELL_TPBUY_WSS));
            Integer bTPB_wss = toIntBoxed(r.get(COL_ITEM_BUY_TPBUY_WSS));
            Integer sTPS_wss = toIntBoxed(r.get(COL_ITEM_SELL_TPSELL_WSS));
            Integer bTPS_wss = toIntBoxed(r.get(COL_ITEM_BUY_TPSELL_WSS));
            if (sTPB_wss != null)
                c_IS_TPB_WSS.add(sTPB_wss);
            if (bTPB_wss != null)
                c_IB_TPB_WSS.add(bTPB_wss);
            if (sTPS_wss != null)
                c_IS_TPS_WSS.add(sTPS_wss);
            if (bTPS_wss != null)
                c_IB_TPS_WSS.add(bTPS_wss);

            // wSS hr
            Integer sTPB_wss_hr = toIntBoxed(r.get(COL_ITEM_SELL_TPBUY_WSS_HR));
            Integer bTPB_wss_hr = toIntBoxed(r.get(COL_ITEM_BUY_TPBUY_WSS_HR));
            Integer sTPS_wss_hr = toIntBoxed(r.get(COL_ITEM_SELL_TPSELL_WSS_HR));
            Integer bTPS_wss_hr = toIntBoxed(r.get(COL_ITEM_BUY_TPSELL_WSS_HR));

            if (sTPB_wss_hr != null)
                c_IS_TPB_WSS_HR.add(sTPB_wss_hr);
            if (bTPB_wss_hr != null)
                c_IB_TPB_WSS_HR.add(bTPB_wss_hr);
            if (sTPS_wss_hr != null)
                c_IS_TPS_WSS_HR.add(sTPS_wss_hr);
            if (bTPS_wss_hr != null)
                c_IB_TPS_WSS_HR.add(bTPS_wss_hr);
        }

        if (c_IS_TPB.isEmpty() && c_IB_TPB.isEmpty() && c_IS_TPS.isEmpty() && c_IB_TPS.isEmpty()
                && c_IS_TPB_HR.isEmpty() && c_IB_TPB_HR.isEmpty() && c_IS_TPS_HR.isEmpty() && c_IB_TPS_HR.isEmpty()
                && c_IS_TPB_WSS.isEmpty() && c_IB_TPB_WSS.isEmpty() && c_IS_TPS_WSS.isEmpty() && c_IB_TPS_WSS.isEmpty()
                && c_IS_TPB_WSS_HR.isEmpty() && c_IB_TPB_WSS_HR.isEmpty() && c_IS_TPS_WSS_HR.isEmpty()
                && c_IB_TPS_WSS_HR.isEmpty()) {
            return;
        }

        String agg = (op == null ? "SUM" : op.toUpperCase(java.util.Locale.ROOT));
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

        // compute all aggregates
        Integer t_IS_TPB = c_IS_TPB.isEmpty() ? null : AGG.apply(c_IS_TPB);
        Integer t_IB_TPB = c_IB_TPB.isEmpty() ? null : AGG.apply(c_IB_TPB);
        Integer t_IS_TPS = c_IS_TPS.isEmpty() ? null : AGG.apply(c_IS_TPS);
        Integer t_IB_TPS = c_IB_TPS.isEmpty() ? null : AGG.apply(c_IB_TPS);

        Integer t_IS_TPB_HR = c_IS_TPB_HR.isEmpty() ? null : AGG.apply(c_IS_TPB_HR);
        Integer t_IB_TPB_HR = c_IB_TPB_HR.isEmpty() ? null : AGG.apply(c_IB_TPB_HR);
        Integer t_IS_TPS_HR = c_IS_TPS_HR.isEmpty() ? null : AGG.apply(c_IS_TPS_HR);
        Integer t_IB_TPS_HR = c_IB_TPS_HR.isEmpty() ? null : AGG.apply(c_IB_TPS_HR);

        Integer t_IS_TPB_WSS = c_IS_TPB_WSS.isEmpty() ? null : AGG.apply(c_IS_TPB_WSS);
        Integer t_IB_TPB_WSS = c_IB_TPB_WSS.isEmpty() ? null : AGG.apply(c_IB_TPB_WSS);
        Integer t_IS_TPS_WSS = c_IS_TPS_WSS.isEmpty() ? null : AGG.apply(c_IS_TPS_WSS);
        Integer t_IB_TPS_WSS = c_IB_TPS_WSS.isEmpty() ? null : AGG.apply(c_IB_TPS_WSS);

        Integer t_IS_TPB_WSS_HR = c_IS_TPB_WSS_HR.isEmpty() ? null : AGG.apply(c_IS_TPB_WSS_HR);
        Integer t_IB_TPB_WSS_HR = c_IB_TPB_WSS_HR.isEmpty() ? null : AGG.apply(c_IB_TPB_WSS_HR);
        Integer t_IS_TPS_WSS_HR = c_IS_TPS_WSS_HR.isEmpty() ? null : AGG.apply(c_IS_TPS_WSS_HR);
        Integer t_IB_TPS_WSS_HR = c_IB_TPS_WSS_HR.isEmpty() ? null : AGG.apply(c_IB_TPS_WSS_HR);

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

        if (t_IS_TPB != null)
            total.put(COL_ITEM_SELL_TPBUY, t_IS_TPB);
        if (t_IB_TPB != null)
            total.put(COL_ITEM_BUY_TPBUY, t_IB_TPB);
        if (t_IS_TPS != null)
            total.put(COL_ITEM_SELL_TPSELL, t_IS_TPS);
        if (t_IB_TPS != null)
            total.put(COL_ITEM_BUY_TPSELL, t_IB_TPS);

        if (t_IS_TPB_HR != null)
            total.put(COL_ITEM_SELL_TPBUY_HR, t_IS_TPB_HR);
        if (t_IB_TPB_HR != null)
            total.put(COL_ITEM_BUY_TPBUY_HR, t_IB_TPB_HR);
        if (t_IS_TPS_HR != null)
            total.put(COL_ITEM_SELL_TPSELL_HR, t_IS_TPS_HR);
        if (t_IB_TPS_HR != null)
            total.put(COL_ITEM_BUY_TPSELL_HR, t_IB_TPS_HR);

        if (t_IS_TPB_WSS != null)
            total.put(COL_ITEM_SELL_TPBUY_WSS, t_IS_TPB_WSS);
        if (t_IB_TPB_WSS != null)
            total.put(COL_ITEM_BUY_TPBUY_WSS, t_IB_TPB_WSS);
        if (t_IS_TPS_WSS != null)
            total.put(COL_ITEM_SELL_TPSELL_WSS, t_IS_TPS_WSS);
        if (t_IB_TPS_WSS != null)
            total.put(COL_ITEM_BUY_TPSELL_WSS, t_IB_TPS_WSS);

        if (t_IS_TPB_WSS_HR != null)
            total.put(COL_ITEM_SELL_TPBUY_WSS_HR, t_IS_TPB_WSS_HR);
        if (t_IB_TPB_WSS_HR != null)
            total.put(COL_ITEM_BUY_TPBUY_WSS_HR, t_IB_TPB_WSS_HR);
        if (t_IS_TPS_WSS_HR != null)
            total.put(COL_ITEM_SELL_TPSELL_WSS_HR, t_IS_TPS_WSS_HR);
        if (t_IB_TPS_WSS_HR != null)
            total.put(COL_ITEM_BUY_TPSELL_WSS_HR, t_IB_TPS_WSS_HR);

        // ---------- BestChoice ----------
        // Preserve semantic: choose best "buy" from ItemBuy_TPBuy, and best "sell" from
        // ItemSell_TPSell
        if ("MAX".equalsIgnoreCase(agg)) {
            String bestBuyName = null;
            String bestSellName = null;
            int bestBuyVal = Integer.MIN_VALUE;
            int bestSellVal = Integer.MIN_VALUE;

            for (var r : rows) {
                String rName = str(r.get(COL_NAME));
                if ("TOTAL".equalsIgnoreCase(rName))
                    continue;

                Integer buyV = toIntBoxed(r.get(COL_ITEM_BUY_TPBUY));
                Integer sellV = toIntBoxed(r.get(COL_ITEM_SELL_TPSELL));

                if (buyV != null && buyV > bestBuyVal) {
                    bestBuyVal = buyV;
                    bestBuyName = (rName != null && !rName.isBlank()) ? rName : str(r.get(COL_KEY));
                }
                if (sellV != null && sellV > bestSellVal) {
                    bestSellVal = sellV;
                    bestSellName = (rName != null && !rName.isBlank()) ? rName : str(r.get(COL_KEY));
                }
            }

            if (bestBuyName != null && !bestBuyName.isBlank())
                total.put(COL_BEST_BUY, bestBuyName);
            else
                total.remove(COL_BEST_BUY);

            if (bestSellName != null && !bestSellName.isBlank())
                total.put(COL_BEST_SELL, bestSellName);
            else
                total.remove(COL_BEST_SELL);
        } else {
            total.remove(COL_BEST_BUY);
            total.remove(COL_BEST_SELL);
        }
    }

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

    public static boolean isCoinRow(Map<String, Object> row) {
        String name = str(row.get(COL_NAME));
        return "Coin".equalsIgnoreCase(name);
    }

    public static int numericTotalAmount(Map<String, Object> row) {
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

    public static Set<Integer> extractIds(List<Map<String, Object>> rows) {
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
}

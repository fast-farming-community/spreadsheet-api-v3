package eu.fast.gw2.tools;

import java.util.*;

public class OverlayHelper {

    // Column names (detail_tables)
    public static final String COL_ID = "Id";
    public static final String COL_IMAGE = "Image";
    public static final String COL_RARITY = "Rarity";
    public static final String COL_CAT = "Category";
    public static final String COL_KEY = "Key";
    public static final String COL_NAME = "Name";
    public static final String COL_AVG = "AverageAmount";
    public static final String COL_TPB = "TPBuyProfit";
    public static final String COL_TPS = "TPSellProfit";
    public static final String COL_TOTAL_AMOUNT = "TotalAmount";

    // Column names (tables - main)
    public static final String COL_TPB_HR = "TPBuyProfitHr";
    public static final String COL_TPS_HR = "TPSellProfitHr";
    public static final String COL_HOURS = "Duration";

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

    public static int net(int value, int taxesPercent) {
        if (value <= 0)
            return 0;
        if (taxesPercent <= 0)
            return value;
        double f = 1.0 - (taxesPercent / 100.0);
        return (int) Math.floor(value * f);
    }

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

    public static void writeProfit(Map<String, Object> row, int buy, int sell) {
        row.put(COL_TPB, buy);
        row.put(COL_TPS, sell);
    }

    public static void writeProfitWithHour(Map<String, Object> row, int buy, int sell) {
        writeProfit(row, buy, sell);
        double hours = toDouble(row.get(COL_HOURS), 0.0);
        int buyHr = (hours > 0.0) ? (int) Math.floor(buy / hours) : buy;
        int sellHr = (hours > 0.0) ? (int) Math.floor(sell / hours) : sell;
        row.put(COL_TPB_HR, buyHr);
        row.put(COL_TPS_HR, sellHr);
    }

    public static void applyAggregation(List<Map<String, Object>> rows, String op) {
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

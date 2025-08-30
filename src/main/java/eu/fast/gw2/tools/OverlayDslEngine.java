// REPLACE ENTIRE FILE: eu.fast.gw2.tools.OverlayDslEngine
package eu.fast.gw2.tools;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import eu.fast.gw2.dao.CalculationsDao;
import eu.fast.gw2.enums.Tier;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * STRICT DSL engine:
 * - Requires formulas_json for every (Category|Key).
 * - No legacy inference. If formulas are missing/malformed, return null; caller
 * zeros & may log.
 * - Returns unit-level (buy,sell). Detail caller multiplies by AverageAmount;
 * main adds per-hour.
 */
public class OverlayDslEngine {

    public static record EvalResult(int buy, int sell) {
    }

    private static final ObjectMapper OM = new ObjectMapper();
    private static final Map<String, DslExpr> EXPR_CACHE = new ConcurrentHashMap<>();

    private static final String FIELD_TPB = "TPBuyProfit";
    private static final String FIELD_TPS = "TPSellProfit";

    public static EvalResult evaluateRowStrict(
            String category,
            String key,
            Map<String, Object> row,
            Tier tier,
            int taxesPercent,
            Map<Integer, int[]> priceMap) {

        // Look up exact (category|key) config
        CalculationsDao.Config cfg = OverlayCalc.getCalcCfg(category, key);
        if (cfg == null)
            return null;

        String formulasJson = cfg.formulasJson();
        if (formulasJson == null || formulasJson.isBlank())
            return null;

        try {
            JsonNode root = OM.readTree(formulasJson);
            String mode = optText(root, "mode", null);

            // Variables and functions. "Key" variable == provided key (no override).
            DslContext ctx = new DslContext(row, tier, taxesPercent, priceMap, key);

            // Compile expressions if provided; else synthesize from mode.
            DslExpr exprTPB = compileExprCached(category, key, FIELD_TPB, root.get(FIELD_TPB), mode);
            DslExpr exprTPS = compileExprCached(category, key, FIELD_TPS, root.get(FIELD_TPS), mode);
            if (exprTPB == null || exprTPS == null)
                return null;

            long b = Math.round(Math.floor(exprTPB.eval(ctx)));
            long s = Math.round(Math.floor(exprTPS.eval(ctx)));
            return new EvalResult((int) clampToInt(b), (int) clampToInt(s));

        } catch (Exception e) {
            System.err.printf(java.util.Locale.ROOT,
                    "OverlayDslEngine STRICT: parse/exec error for (%s|%s): %s%n",
                    String.valueOf(category), String.valueOf(key), e.getMessage());
            return null;
        }
    }

    // ---------- helpers ----------

    private static long clampToInt(long v) {
        if (v > Integer.MAX_VALUE)
            return Integer.MAX_VALUE;
        if (v < Integer.MIN_VALUE)
            return Integer.MIN_VALUE;
        return v;
    }

    private static String optText(JsonNode n, String field, String def) {
        if (n == null)
            return def;
        JsonNode v = n.get(field);
        return (v == null || v.isNull()) ? def : v.asText();
    }

    private static DslExpr compileExprCached(String category, String key, String field, JsonNode node, String mode) {
        final String ck = ((category == null ? "" : category.trim().toUpperCase(java.util.Locale.ROOT))
                + "|" + (key == null ? "" : key.trim()) + "|" + field);
        DslExpr cached = EXPR_CACHE.get(ck);
        if (cached != null)
            return cached;

        String exprText = null;

        if (node != null && !node.isNull()) {
            exprText = node.asText(null);
        } else if (mode != null) {
            // Compact mode-only configs
            switch (mode.trim().toUpperCase(java.util.Locale.ROOT)) {
                case "LEAF" -> {
                    if (FIELD_TPB.equals(field))
                        exprText = "NET(BUY(Id), taxes)";
                    if (FIELD_TPS.equals(field))
                        exprText = "NET(SELL(Id), taxes)";
                }
                case "COMPOSITE" -> {
                    if (FIELD_TPB.equals(field))
                        exprText = "EV(Key, taxes).buy";
                    if (FIELD_TPS.equals(field))
                        exprText = "EV(Key, taxes).sell";
                }
                case "INTERNAL" -> {
                    if (FIELD_TPB.equals(field))
                        exprText = "EV(Key, 0).buy";
                    if (FIELD_TPS.equals(field))
                        exprText = "EV(Key, 0).sell";
                }
                default -> {
                }
            }
        }

        if (exprText == null || exprText.isBlank())
            return null;

        DslExpr compiled = DslExpr.parse(exprText);
        EXPR_CACHE.put(ck, compiled);
        return compiled;
    }

    // --------------------------
    // DSL evaluation context
    // --------------------------
    static final class DslContext {
        final Map<String, Object> row;
        final Tier tier;
        final int taxesPercent;
        final Map<Integer, int[]> priceMap;
        final String key; // no override

        DslContext(Map<String, Object> row, Tier tier, int taxesPercent, Map<Integer, int[]> priceMap, String key) {
            this.row = row;
            this.tier = tier;
            this.taxesPercent = taxesPercent;
            this.priceMap = priceMap;
            this.key = key;
        }

        // Variables
        double var(String name) {
            return switch (name) {
                case "Id" -> OverlayHelper.toInt(row.get(OverlayHelper.COL_ID), -1);
                case "AverageAmount", "QTY" -> OverlayHelper.toDouble(row.get(OverlayHelper.COL_AVG), 1.0);
                case "taxes" -> taxesPercent;
                default -> Double.NaN;
            };
        }

        String strVar(String name) {
            return switch (name) {
                case "Category" -> OverlayHelper.str(row.get(OverlayHelper.COL_CAT));
                case "Key" -> key;
                case "Name" -> OverlayHelper.str(row.get(OverlayHelper.COL_NAME));
                default -> null;
            };
        }

        // Functions
        double BUY(double id) {
            int iid = (int) Math.floor(id);
            int[] ps = priceMap.getOrDefault(iid, new int[] { 0, 0 });
            return ps[0];
        }

        double SELL(double id) {
            int iid = (int) Math.floor(id);
            int[] ps = priceMap.getOrDefault(iid, new int[] { 0, 0 });
            return ps[1];
        }

        double VENDOR(double id) {
            int iid = (int) Math.floor(id);
            Integer v = OverlayCache.vendorValueCached(iid);
            return (v == null ? 0.0 : v.doubleValue());
        }

        double NET(double value, double taxes) {
            int v = (int) Math.floor(value);
            int t = (int) Math.floor(taxes);
            return OverlayHelper.net(v, t);
        }

        EvResult EV(String key, double taxes) {
            if (key == null || key.isBlank())
                return new EvResult(0, 0);
            int t = (int) Math.floor(taxes);
            int[] ev = OverlayCalc.evForDetail(key, priceMap, t, tier.columnKey());
            return new EvResult(ev[0], ev[1]);
        }

        double QTY() {
            return OverlayHelper.toDouble(row.get(OverlayHelper.COL_AVG), 1.0);
        }

        double FALLBACK(double... xs) {
            for (double x : xs)
                if (x != 0.0)
                    return x;
            return 0.0;
        }

        double FLOOR(double x) {
            return Math.floor(x);
        }
    }

    /** Pair object used for EV(key, taxes) */
    static final class EvResult {
        final double buy, sell;

        EvResult(double b, double s) {
            this.buy = b;
            this.sell = s;
        }
    }
}

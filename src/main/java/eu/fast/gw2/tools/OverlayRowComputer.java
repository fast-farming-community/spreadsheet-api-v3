// REPLACE ENTIRE FILE: eu.fast.gw2.tools.OverlayRowComputer
package eu.fast.gw2.tools;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import eu.fast.gw2.dao.CalculationsDao;
import eu.fast.gw2.enums.Tier;

public final class OverlayRowComputer {

    private OverlayRowComputer() {
    }

    private static final double MIN_COPPER = 0.5;

    // -------- compute core --------
    static final class ComputeContext {
        final boolean isMain;
        final Tier tier;
        /** MAIN: "pageId|pageName"; DETAIL: key */
        final String tableKey;
        /** DETAIL: fid; MAIN: null */
        final Long detailFeatureIdOrNull;
        /** Table-level config used only for taxes fallback */
        final CalculationsDao.Config tableConfig;
        final Map<Integer, int[]> priceByItemId;
        final Map<Integer, String> imageUrlByItemId;
        final Map<Integer, String> rarityByItemId;

        ComputeContext(boolean isMain, Tier tier, String tableKey, Long detailFeatureIdOrNull,
                CalculationsDao.Config tableConfig, Map<Integer, int[]> priceByItemId,
                Map<Integer, String> imageUrlByItemId, Map<Integer, String> rarityByItemId) {
            this.isMain = isMain;
            this.tier = tier;
            this.tableKey = tableKey;
            this.detailFeatureIdOrNull = detailFeatureIdOrNull;
            this.tableConfig = tableConfig;
            this.priceByItemId = priceByItemId;
            this.imageUrlByItemId = imageUrlByItemId;
            this.rarityByItemId = rarityByItemId;
        }
    }

    static void computeRow(Map<String, Object> row, ComputeContext ctx, int rowIndex,
            OverlayProfiler prof, OverlayProblemLog problems) {

        String rawCategory = OverlayHelper.str(row.get(OverlayHelper.COL_CAT));
        String rawKey = OverlayHelper.str(row.get(OverlayHelper.COL_KEY));
        int itemId = OverlayHelper.toInt(row.get(OverlayHelper.COL_ID), -1);

        // enrich image/rarity if we can (safe; does not touch profit numbers)
        if (itemId > 0) {
            String imageUrl = ctx.imageUrlByItemId.get(itemId);
            if (imageUrl != null && !imageUrl.isBlank())
                row.put(OverlayHelper.COL_IMAGE, imageUrl);
            String rarity = ctx.rarityByItemId.get(itemId);
            if (rarity != null && !rarity.isBlank())
                row.put(OverlayHelper.COL_RARITY, rarity);
        }

        // UNCHECKED -> do not overwrite profit values
        if ("UNCHECKED".equalsIgnoreCase(rawCategory)) {
            if (prof != null)
                prof.fastItem++;
            return;
        }

        // Resolve effective (category,key) deterministically
        String effCategory;
        String effKey;

        if ("INTERNAL".equalsIgnoreCase(rawCategory)) {
            if (ctx.isMain) {
                int pageId = OverlayDBAccess.pageIdFromComposite(ctx.tableKey);
                String pageName = OverlayDBAccess.pageNameFromComposite(ctx.tableKey);
                String featureName = OverlayDBAccess.featureNameByPageId(pageId);
                effCategory = "INTERNAL";
                effKey = (featureName == null ? "" : featureName) + "/" + (pageName == null ? "" : pageName);
            } else {
                effCategory = "INTERNAL";
                effKey = (rawKey == null ? "" : rawKey);
            }
        } else if ("NEGATIVE".equalsIgnoreCase(rawCategory)) {
            effCategory = "NEGATIVE";
            effKey = (rawKey == null ? "" : rawKey);
        } else {
            // All other categories resolved via detail_features
            if (ctx.isMain) {
                // MAIN: find detail feature by row's key
                String dfName = OverlayDBAccess.detailFeatureNameByKey(rawKey);
                effCategory = (dfName == null ? "" : dfName);
                effKey = (rawKey == null ? "" : rawKey);
            } else {
                // DETAIL: we have fid
                String dfName = (ctx.detailFeatureIdOrNull == null) ? null
                        : OverlayDBAccess.detailFeatureNameById(ctx.detailFeatureIdOrNull);
                effCategory = (dfName == null ? "" : dfName);
                effKey = (rawKey == null ? "" : rawKey);
            }
        }

        // NEGATIVE: untaxed, negative unit prices × AvgAmount
        if ("NEGATIVE".equalsIgnoreCase(effCategory)) {
            double qty = OverlayHelper.toDouble(row.get(OverlayHelper.COL_AVG), 1.0);
            int[] ps = (itemId > 0) ? ctx.priceByItemId.getOrDefault(itemId, new int[] { 0, 0 }) : new int[] { 0, 0 };
            int unitBuy = (ps.length > 0 ? ps[0] : 0);
            int unitSell = (ps.length > 1 ? ps[1] : 0);
            int buy = (int) Math.round(-qty * unitBuy);
            int sell = (int) Math.round(-qty * unitSell);

            if (ctx.isMain)
                OverlayHelper.writeFourWithHour(row, buy, sell, buy, sell);
            else
                OverlayHelper.writeFour(row, buy, sell, buy, sell);

            writeSpiritShardAugments(row, ctx);
            if (prof != null)
                prof.fastItem++;
            return;
        }

        // Taxes now based on resolved (category,key)
        int taxesPct = OverlayCalc.pickTaxesPercent(effCategory, effKey, ctx.tableConfig);

        // INTERNAL composite (MAIN) or general composite ref: EV path with per-row
        // Datasets->op
        boolean isCompositeRef = (effKey != null && !effKey.isBlank()
                && ("INTERNAL".equalsIgnoreCase(effCategory) || !OverlayHelper.isInternal(effCategory)));

        if (isCompositeRef) {
            String op = deriveAggFromDatasets(row);
            int[] ev = OverlayCalc.evForDetail(effKey, ctx.priceByItemId, taxesPct, ctx.tier.columnKey(), op);
            int evBuy = (ev != null && ev.length > 0) ? ev[0] : 0;
            int evSell = (ev != null && ev.length > 1) ? ev[1] : 0;

            int IB_TPB = evBuy, IS_TPB = evBuy, IB_TPS = evSell, IS_TPS = evSell;

            if (!ctx.isMain) {
                double qty = OverlayHelper.toDouble(row.get(OverlayHelper.COL_AVG), 1.0);
                IB_TPB = (int) Math.round(IB_TPB * qty);
                IS_TPB = (int) Math.round(IS_TPB * qty);
                IB_TPS = (int) Math.round(IB_TPS * qty);
                IS_TPS = (int) Math.round(IS_TPS * qty);
            }

            if (ctx.isMain)
                OverlayHelper.writeFourWithHour(row, IB_TPB, IS_TPB, IB_TPS, IS_TPS);
            else
                OverlayHelper.writeFour(row, IB_TPB, IS_TPB, IB_TPS, IS_TPS);

            writeSpiritShardAugments(row, ctx);
            if (prof != null)
                prof.fastComposite++;
            return;
        }

        // Plain item (no composite key) path
        boolean looksPlainItem = (effKey == null || effKey.isBlank()) && itemId > 0;
        if (looksPlainItem) {
            int[] ps = ctx.priceByItemId.get(itemId);
            int tpb = (ps == null || ps.length < 1) ? 0 : Math.max(0, ps[0]);
            int tps = (ps == null || ps.length < 2) ? 0 : Math.max(0, ps[1]);
            int sellNet = netSellAfterTax(tps, taxesPct);

            if (tpb == 0 && sellNet == 0) {
                Integer vv = OverlayCache.vendorValueCached(itemId);
                if (vv != null && vv > 0)
                    sellNet = vv;
            }

            int IB_TPB = tpb, IS_TPB = tpb, IB_TPS = sellNet, IS_TPS = sellNet;

            if (!ctx.isMain) {
                double qty = OverlayHelper.toDouble(row.get(OverlayHelper.COL_AVG), 1.0);
                IB_TPB = (int) Math.round(IB_TPB * qty);
                IS_TPB = (int) Math.round(IS_TPB * qty);
                IB_TPS = (int) Math.round(IB_TPS * qty);
                IS_TPS = (int) Math.round(IS_TPS * qty);
            }

            if (Math.abs(IB_TPB) < MIN_COPPER && Math.abs(IS_TPB) < MIN_COPPER
                    && Math.abs(IB_TPS) < MIN_COPPER && Math.abs(IS_TPS) < MIN_COPPER) {
                if (ctx.isMain)
                    OverlayHelper.writeFourWithHour(row, 0, 0, 0, 0);
                else
                    OverlayHelper.writeFour(row, 0, 0, 0, 0);
                writeSpiritShardAugments(row, ctx);
                if (prof != null)
                    prof.belowCutoff++;
                return;
            }

            if (ctx.isMain)
                OverlayHelper.writeFourWithHour(row, IB_TPB, IS_TPB, IB_TPS, IS_TPS);
            else
                OverlayHelper.writeFour(row, IB_TPB, IS_TPB, IB_TPS, IS_TPS);

            writeSpiritShardAugments(row, ctx);
            if (prof != null)
                prof.fastItem++;
            return;
        }

        // DSL fallback (STRICT) using resolved (category,key)
        var eval = OverlayDslEngine.evaluateRowStrict(effCategory, effKey, row, ctx.tier, taxesPct, ctx.priceByItemId);
        if (eval == null) {
            if (ctx.isMain)
                OverlayHelper.writeFourWithHour(row, 0, 0, 0, 0);
            else
                OverlayHelper.writeFour(row, 0, 0, 0, 0);
            writeSpiritShardAugments(row, ctx);
            if (problems != null)
                problems.record(ctx.isMain, ctx.tableKey, ctx.detailFeatureIdOrNull, rowIndex, row, taxesPct,
                        "missing_formulas");
            return;
        }

        int IB_TPB = eval.buy(), IS_TPB = eval.buy(), IB_TPS = eval.sell(), IS_TPS = eval.sell();

        if (!ctx.isMain) {
            double qty = OverlayHelper.toDouble(row.get(OverlayHelper.COL_AVG), 1.0);
            IB_TPB = (int) Math.round(IB_TPB * qty);
            IS_TPB = (int) Math.round(IS_TPB * qty);
            IB_TPS = (int) Math.round(IB_TPS * qty);
            IS_TPS = (int) Math.round(IS_TPS * qty);
        }

        if (Math.abs(IB_TPB) < MIN_COPPER && Math.abs(IS_TPB) < MIN_COPPER
                && Math.abs(IB_TPS) < MIN_COPPER && Math.abs(IS_TPS) < MIN_COPPER) {
            if (ctx.isMain)
                OverlayHelper.writeFourWithHour(row, 0, 0, 0, 0);
            else
                OverlayHelper.writeFour(row, 0, 0, 0, 0);
            writeSpiritShardAugments(row, ctx);
            if (prof != null)
                prof.belowCutoff++;
            return;
        }

        if (ctx.isMain)
            OverlayHelper.writeFourWithHour(row, IB_TPB, IS_TPB, IB_TPS, IS_TPS);
        else
            OverlayHelper.writeFour(row, IB_TPB, IS_TPB, IB_TPS, IS_TPS);

        writeSpiritShardAugments(row, ctx);
        if (problems != null)
            problems.recordIfZero(ctx.isMain, ctx.tableKey, ctx.detailFeatureIdOrNull, rowIndex, row, taxesPct,
                    "computed_zero");
    }

    // -------- helpers used by runners --------

    static List<Map<String, Object>> deepCopyRows(List<Map<String, Object>> base) {
        if (base == null || base.isEmpty())
            return new ArrayList<>();
        List<Map<String, Object>> out = new ArrayList<>(base.size());
        for (Map<String, Object> r : base)
            out.add(new LinkedHashMap<>(r));
        return out;
    }

    private static int netSellAfterTax(int tps, int taxesPct) {
        if (taxesPct <= 0)
            return tps;
        return (int) Math.floor(tps * (100.0 - OverlayHelper.clampPercent(taxesPct)) / 100.0);
    }

    private static void writeSpiritShardAugments(Map<String, Object> row, ComputeContext ctx) {
        int base_IS_TPB = OverlayHelper.toInt(row.get(OverlayHelper.COL_ITEM_SELL_TPBUY), 0);
        int base_IB_TPB = OverlayHelper.toInt(row.get(OverlayHelper.COL_ITEM_BUY_TPBUY), 0);
        int base_IS_TPS = OverlayHelper.toInt(row.get(OverlayHelper.COL_ITEM_SELL_TPSELL), 0);
        int base_IB_TPS = OverlayHelper.toInt(row.get(OverlayHelper.COL_ITEM_BUY_TPSELL), 0);

        int itemId = OverlayHelper.toInt(row.get(OverlayHelper.COL_ID), -1);
        boolean isSpiritShardRow = (itemId == 23);

        int[] shard = OverlaySpiritShard.getShardUnitPair(ctx.tier, ctx.priceByItemId);
        int shardBuyUnit = (shard == null || shard.length < 1) ? 0 : shard[0];
        int shardSellUnit = (shard == null || shard.length < 2) ? 0 : shard[1];

        double avg = OverlayHelper.toDouble(row.get(OverlayHelper.COL_AVG), 1.0);
        int deltaBuy = (int) Math.round(avg * shardBuyUnit);
        int deltaSell = (int) Math.round(avg * shardSellUnit);

        if (isSpiritShardRow) {
            base_IS_TPB = base_IB_TPB = base_IS_TPS = base_IB_TPS = 0;

            if (ctx.isMain) {
                row.put(OverlayHelper.COL_ITEM_SELL_TPBUY_HR, 0);
                row.put(OverlayHelper.COL_ITEM_BUY_TPBUY_HR, 0);
                row.put(OverlayHelper.COL_ITEM_SELL_TPSELL_HR, 0);
                row.put(OverlayHelper.COL_ITEM_BUY_TPSELL_HR, 0);
            }

            row.put(OverlayHelper.COL_ITEM_SELL_TPBUY, 0);
            row.put(OverlayHelper.COL_ITEM_BUY_TPBUY, 0);
            row.put(OverlayHelper.COL_ITEM_SELL_TPSELL, 0);
            row.put(OverlayHelper.COL_ITEM_BUY_TPSELL, 0);
        }

        int IS_TPB_wSS = base_IS_TPB + deltaBuy;
        int IB_TPB_wSS = base_IB_TPB + deltaBuy;
        int IS_TPS_wSS = base_IS_TPS + deltaSell;
        int IB_TPS_wSS = base_IB_TPS + deltaSell;

        row.put(OverlayHelper.COL_ITEM_SELL_TPBUY_WSS, IS_TPB_wSS);
        row.put(OverlayHelper.COL_ITEM_BUY_TPBUY_WSS, IB_TPB_wSS);
        row.put(OverlayHelper.COL_ITEM_SELL_TPSELL_WSS, IS_TPS_wSS);
        row.put(OverlayHelper.COL_ITEM_BUY_TPSELL_WSS, IB_TPS_wSS);

        if (ctx.isMain) {
            double hours = OverlayHelper.toDouble(row.get(OverlayHelper.COL_HOURS), 0.0);

            row.put(OverlayHelper.COL_ITEM_SELL_TPBUY_WSS_HR,
                    (hours > 0.0) ? (int) Math.floor(IS_TPB_wSS / hours) : IS_TPB_wSS);
            row.put(OverlayHelper.COL_ITEM_BUY_TPBUY_WSS_HR,
                    (hours > 0.0) ? (int) Math.floor(IB_TPB_wSS / hours) : IB_TPB_wSS);
            row.put(OverlayHelper.COL_ITEM_SELL_TPSELL_WSS_HR,
                    (hours > 0.0) ? (int) Math.floor(IS_TPS_wSS / hours) : IS_TPS_wSS);
            row.put(OverlayHelper.COL_ITEM_BUY_TPSELL_WSS_HR,
                    (hours > 0.0) ? (int) Math.floor(IB_TPS_wSS / hours) : IB_TPS_wSS);
        }
    }

    /**
     * Map Datasets cell to an aggregation op for the referenced table.
     * Rules:
     * - "static" -> MAX
     * - any number (Number instance or numeric string) -> SUM
     * - missing/unknown -> SUM (warn)
     */
    private static String deriveAggFromDatasets(Map<String, Object> row) {
        Object ds = row.get("Datasets");
        if (ds == null) {
            warnBadDatasets(row, "null");
            return "SUM";
        }

        if (ds instanceof Number) {
            return "SUM";
        }

        String s = String.valueOf(ds).trim();
        if ("static".equalsIgnoreCase(s))
            return "MAX";

        if (!s.isEmpty() && s.matches("^-?\\d+(\\.\\d+)?$")) {
            return "SUM";
        }

        warnBadDatasets(row, s);
        return "SUM";
    }

    private static void warnBadDatasets(Map<String, Object> row, String val) {
        String c = OverlayHelper.str(row.get(OverlayHelper.COL_CAT));
        String k = OverlayHelper.str(row.get(OverlayHelper.COL_KEY));
        String n = OverlayHelper.str(row.get(OverlayHelper.COL_NAME));
        System.err.printf(java.util.Locale.ROOT,
                "Overlay EV WARNING: composite row has unknown Datasets=%s (Category='%s' Key='%s' Name='%s'). Using SUM.%n",
                String.valueOf(val), c, k, n);
    }
}

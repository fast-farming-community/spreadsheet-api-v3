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
        final String tableKey;
        final Long detailFeatureIdOrNull;
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

        String category = OverlayHelper.str(row.get(OverlayHelper.COL_CAT));
        String compositeKey = OverlayHelper.str(row.get(OverlayHelper.COL_KEY));
        int taxesPct = OverlayCalc.pickTaxesPercent(category, compositeKey, ctx.tableConfig);
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

        // ===== NEW: UNTOUCHED hard stop =====
        // If Category is exactly "UNTOUCHED" (case-sensitive), DO NOT overwrite any
        // profit columns (base, hr, wSS).
        if ("UNTOUCHED".equals(category)) {
            if (prof != null)
                prof.fastItem++; // count as a quick path; nothing written
            return;
        }

        // Special currency: Coin (id==1) — write same value to all four
        if (itemId == 1) {
            int amt = (int) Math.floor(OverlayHelper.toDouble(row.get(OverlayHelper.COL_AVG), 0.0));
            if (ctx.isMain) {
                OverlayHelper.writeFourWithHour(row, amt, amt, amt, amt);
            } else {
                OverlayHelper.writeFour(row, amt, amt, amt, amt);
            }
            writeSpiritShardAugments(row, ctx);
            return;
        }

        // ========== NEGATIVE category (exact, case-sensitive) ==========
        // AvgAmount * ( - unit_price buy/sell ) ; NEVER apply tax
        if ("NEGATIVE".equals(category)) {
            double qty = OverlayHelper.toDouble(row.get(OverlayHelper.COL_AVG), 1.0);

            int[] ps = (itemId > 0) ? ctx.priceByItemId.get(itemId) : null;
            int unitBuy = (ps == null || ps.length < 1) ? 0 : ps[0];
            int unitSell = (ps == null || ps.length < 2) ? 0 : ps[1];

            int itemBuy = (int) Math.round(qty * (-unitBuy));
            int itemSell = (int) Math.round(qty * (-unitSell));

            int IB_TPB = itemBuy;
            int IS_TPB = itemSell;
            int IB_TPS = itemBuy;
            int IS_TPS = itemSell;

            if (ctx.isMain)
                OverlayHelper.writeFourWithHour(row, IB_TPB, IS_TPB, IB_TPS, IS_TPS);
            else
                OverlayHelper.writeFour(row, IB_TPB, IS_TPB, IB_TPS, IS_TPS);

            writeSpiritShardAugments(row, ctx);
            if (prof != null)
                prof.fastItem++;
            return;
        }

        // ========== FAST PATH #1: Composite reference ==========
        if (OverlayHelper.isCompositeRef(category, compositeKey)) {
            int[] ev = OverlayCalc.evForDetail(compositeKey, ctx.priceByItemId, taxesPct, ctx.tier.columnKey());
            int evBuy = (ev != null && ev.length > 0) ? ev[0] : 0; // bagEV non-negative
            int evSell = (ev != null && ev.length > 1) ? ev[1] : 0;

            int IB_TPB = evBuy;
            int IS_TPB = evBuy;
            int IB_TPS = evSell;
            int IS_TPS = evSell;

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

        // ========== FAST PATH #2: Plain item ==========
        boolean looksPlainItem = !OverlayHelper.isInternal(category)
                && (compositeKey == null || compositeKey.isBlank())
                && itemId > 0;

        if (looksPlainItem) {
            int[] ps = ctx.priceByItemId.get(itemId);
            int tpb = (ps == null || ps.length < 1) ? 0 : ps[0];
            int tps = (ps == null || ps.length < 2) ? 0 : ps[1];

            int sellNet = netSellAfterTax(tps, taxesPct);

            if (tpb == 0 && sellNet == 0) {
                Integer vv = OverlayCache.vendorValueCached(itemId);
                if (vv != null && vv > 0)
                    sellNet = vv;
            }

            int IB_TPB = tpb;
            int IS_TPB = tpb;
            int IB_TPS = sellNet;
            int IS_TPS = sellNet;

            if (!ctx.isMain) {
                double qty = OverlayHelper.toDouble(row.get(OverlayHelper.COL_AVG), 1.0);
                IB_TPB = (int) Math.round(IB_TPB * qty);
                IS_TPB = (int) Math.round(IS_TPB * qty);
                IB_TPS = (int) Math.round(IB_TPS * qty);
                IS_TPS = (int) Math.round(IS_TPS * qty);
            }

            if (Math.abs(IB_TPB) < MIN_COPPER && Math.abs(IS_TPS) < MIN_COPPER
                    && Math.abs(IS_TPB) < MIN_COPPER && Math.abs(IB_TPS) < MIN_COPPER) {
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

        // ========== DSL fallback (STRICT) ==========
        var eval = OverlayDslEngine.evaluateRowStrict(category, compositeKey, row, ctx.tier, taxesPct,
                ctx.priceByItemId);

        if (eval == null) {
            System.err.printf(java.util.Locale.ROOT,
                    ctx.isMain
                            ? "Overlay STRICT(main): missing formulas for (category='%s', key='%s') in table='%s' row#%d name='%s'%n"
                            : (ctx.detailFeatureIdOrNull == null
                                    ? "Overlay STRICT(detailCore): missing formulas for (category='%s', key='%s') key='%s' row#%d name='%s'%n"
                                    : "Overlay STRICT(detail): missing formulas for (category='%s', key='%s') in table='%s' fid=%d row#%d name='%s'%n"),
                    String.valueOf(category), String.valueOf(compositeKey), ctx.tableKey,
                    rowIndex, String.valueOf(row.get(OverlayHelper.COL_NAME)));
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

        int baseBuy = eval.buy();
        int baseSell = eval.sell();

        int IB_TPB = baseBuy;
        int IS_TPB = baseBuy;
        int IB_TPS = baseSell;
        int IS_TPS = baseSell;

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
}

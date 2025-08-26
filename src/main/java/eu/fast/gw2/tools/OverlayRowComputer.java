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

        if (itemId > 0) {
            String imageUrl = ctx.imageUrlByItemId.get(itemId);
            if (imageUrl != null && !imageUrl.isBlank())
                row.put(OverlayHelper.COL_IMAGE, imageUrl);
            String rarity = ctx.rarityByItemId.get(itemId);
            if (rarity != null && !rarity.isBlank())
                row.put(OverlayHelper.COL_RARITY, rarity);
        }

        if (itemId == 1) {
            int amt = (int) Math.floor(OverlayHelper.toDouble(row.get(OverlayHelper.COL_AVG), 0.0));
            if (ctx.isMain)
                OverlayHelper.writeProfitWithHour(row, amt, amt);
            else
                OverlayHelper.writeProfit(row, amt, amt);
            return;
        }

        // FAST PATH #1: Composite reference
        if (OverlayHelper.isCompositeRef(category, compositeKey)) {
            int[] ev = OverlayCalc.evForDetail(compositeKey, ctx.priceByItemId, taxesPct, ctx.tier.columnKey());
            int buy = (ev != null && ev.length > 0) ? Math.max(0, ev[0]) : 0;
            int sell = (ev != null && ev.length > 1) ? Math.max(0, ev[1]) : 0;

            if (!ctx.isMain) {
                double qty = OverlayHelper.toDouble(row.get(OverlayHelper.COL_AVG), 1.0);
                buy = (int) Math.round(buy * qty);
                sell = (int) Math.round(sell * qty);
            }

            if (buy < 1 && sell < 1) {
                if (ctx.isMain)
                    OverlayHelper.writeProfitWithHour(row, 0, 0);
                else
                    OverlayHelper.writeProfit(row, 0, 0);
                if (prof != null)
                    prof.belowCutoff++;
                return;
            }

            if (ctx.isMain)
                OverlayHelper.writeProfitWithHour(row, buy, sell);
            else
                OverlayHelper.writeProfit(row, buy, sell);
            if (prof != null)
                prof.fastComposite++;
            return;
        }

        // FAST PATH #2: Plain item
        boolean looksPlainItem = !OverlayHelper.isInternal(category)
                && (compositeKey == null || compositeKey.isBlank())
                && itemId > 0;

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

            int buy = tpb, sell = sellNet;

            if (!ctx.isMain) {
                double qty = OverlayHelper.toDouble(row.get(OverlayHelper.COL_AVG), 1.0);
                buy = (int) Math.round(buy * qty);
                sell = (int) Math.round(sell * qty);
            }

            if (buy < 1 && sell < 1) {
                if (ctx.isMain)
                    OverlayHelper.writeProfitWithHour(row, 0, 0);
                else
                    OverlayHelper.writeProfit(row, 0, 0);
                if (prof != null)
                    prof.belowCutoff++;
                return;
            }

            if (ctx.isMain)
                OverlayHelper.writeProfitWithHour(row, buy, sell);
            else
                OverlayHelper.writeProfit(row, buy, sell);
            if (prof != null)
                prof.fastItem++;
            return;
        }

        // DSL fallback (STRICT)
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
                OverlayHelper.writeProfitWithHour(row, 0, 0);
            else
                OverlayHelper.writeProfit(row, 0, 0);
            if (problems != null)
                problems.record(ctx.isMain, ctx.tableKey, ctx.detailFeatureIdOrNull, rowIndex, row, taxesPct,
                        "missing_formulas");
            return;
        }

        if (ctx.isMain) {
            double buyRaw = eval.buy();
            double sellRaw = eval.sell();
            if (buyRaw < MIN_COPPER && sellRaw < MIN_COPPER) {
                OverlayHelper.writeProfitWithHour(row, 0, 0);
                if (prof != null)
                    prof.belowCutoff++;
                return;
            }
            OverlayHelper.writeProfitWithHour(row, eval.buy(), eval.sell());
            if (problems != null)
                problems.recordIfZero(true, ctx.tableKey, ctx.detailFeatureIdOrNull, rowIndex, row, taxesPct,
                        "computed_zero");
        } else {
            double qty = OverlayHelper.toDouble(row.get(OverlayHelper.COL_AVG), 1.0);
            double buyRaw = eval.buy() * qty;
            double sellRaw = eval.sell() * qty;
            if (buyRaw < MIN_COPPER && sellRaw < MIN_COPPER) {
                OverlayHelper.writeProfit(row, 0, 0);
                if (prof != null)
                    prof.belowCutoff++;
                return;
            }
            int buyTotal = (int) Math.round(buyRaw);
            int sellTotal = (int) Math.round(sellRaw);
            OverlayHelper.writeProfit(row, buyTotal, sellTotal);
            if (problems != null)
                problems.recordIfZero(false, ctx.tableKey, ctx.detailFeatureIdOrNull, rowIndex, row, taxesPct,
                        "computed_zero");
        }
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
        if (tps <= 0 || taxesPct <= 0)
            return Math.max(0, tps);
        return (int) Math.floor(tps * (100.0 - OverlayHelper.clampPercent(taxesPct)) / 100.0);
    }

}

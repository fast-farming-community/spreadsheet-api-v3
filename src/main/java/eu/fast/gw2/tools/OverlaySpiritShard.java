package eu.fast.gw2.tools;

import java.util.List;
import java.util.Map;

import eu.fast.gw2.dao.CalculationsDao;
import eu.fast.gw2.enums.Tier;

final class OverlaySpiritShard {

    /** Cache per tier label -> [buyUnit, sellUnit] */
    private static final java.util.concurrent.ConcurrentHashMap<String, int[]> CACHE = new java.util.concurrent.ConcurrentHashMap<>();

    /**
     * Returns the Spirit Shard unit profit pair [buyUnit, sellUnit] for a tier.
     * - Derived from INTERNAL|spirit-shard
     * - MAX across all LEAF candidates (unit-level, taxes=0)
     * - Does NOT multiply by AverageAmount (that's applied at the consumer row)
     */
    static int[] getShardUnitPair(Tier tier, Map<Integer, int[]> priceMap) {
        final String k = tier.label;
        int[] cached = CACHE.get(k);
        if (cached != null)
            return cached;

        // Locate definition for INTERNAL|spirit-shard (case-insensitive category
        // handling is in OverlayCalc)
        CalculationsDao.Config cfg = OverlayCalc.getCalcCfg("INTERNAL", "spirit-shard");

        // Which detail key holds the LEAF candidates. If source_table_key present,
        // prefer it.
        final String sourceKey = (cfg != null && cfg.sourceTableKey() != null && !cfg.sourceTableKey().isBlank())
                ? cfg.sourceTableKey()
                : "spirit-shard";

        // Load the base detail rows that list the leaf candidates
        List<Map<String, Object>> rows = OverlayCache.getBaseDetailRows(sourceKey);
        if (rows == null || rows.isEmpty()) {
            int[] zero = new int[] { 0, 0 };
            CACHE.put(k, zero);
            return zero;
        }

        // Ensure prices for all candidate items
        OverlayCalc.ensurePricesForIds(priceMap, OverlayHelper.extractIds(rows), tier.columnKey());

        int maxBuy = 0, maxSell = 0;

        // For LEAF unit-level: buy = BUY(Id), sell = SELL(Id) with taxes=0; vendor
        // fallback when both zero
        for (var r : rows) {
            int id = OverlayHelper.toInt(r.get(OverlayHelper.COL_ID), -1);
            if (id <= 0)
                continue;

            int[] ps = priceMap.getOrDefault(id, new int[] { 0, 0 });
            int buy = Math.max(0, ps[0]);
            int sell = Math.max(0, ps[1]);

            if (buy == 0 && sell == 0) {
                Integer vv = OverlayCache.vendorValueCached(id);
                if (vv != null && vv > 0)
                    sell = vv;
            }

            if (buy > maxBuy)
                maxBuy = buy;
            if (sell > maxSell)
                maxSell = sell;
        }

        int[] out = new int[] { maxBuy, maxSell };
        CACHE.put(k, out);
        return out;
    }

    private OverlaySpiritShard() {
    }
}

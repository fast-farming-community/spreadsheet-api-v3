package eu.fast.gw2.tools;

import eu.fast.gw2.enums.Tier;

import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Spirit Shard unit profit pair [buyUnit, sellUnit] per tier.
 * - Always reads from detail key "spirit-shard".
 * - MAX across all LEAF candidates (unit-level, taxes=0).
 * - Does NOT multiply by AverageAmount (that's applied by the consumer row).
 */
final class OverlaySpiritShard {

    /** Cache per tier label -> [buyUnit, sellUnit] */
    private static final ConcurrentHashMap<String, int[]> CACHE = new ConcurrentHashMap<>();

    static int[] getShardUnitPair(Tier tier, Map<Integer, int[]> priceMap) {
        final String k = tier.label;
        int[] cached = CACHE.get(k);
        if (cached != null)
            return cached;

        // Fixed detail key
        final String sourceKey = "spirit-shard";

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

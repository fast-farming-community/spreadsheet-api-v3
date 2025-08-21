package eu.fast.gw2.dao;

import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import eu.fast.gw2.tools.Jpa;

public class Gw2PricesDao {

    /**
     * Load tiered prices for the given item ids from gw2_prices_tiers.
     * Returns id -> [buy, sell]. Missing rows map to [0,0].
     */
    @SuppressWarnings("unchecked")
    public static Map<Integer, int[]> loadTier(List<Integer> ids, String tierKey) {
        if (ids == null || ids.isEmpty())
            return Collections.emptyMap();

        final String buyCol, sellCol;
        switch (tierKey) {
            case "5m" -> {
                buyCol = "buy_5m";
                sellCol = "sell_5m";
            }
            case "10m" -> {
                buyCol = "buy_10m";
                sellCol = "sell_10m";
            }
            case "15m" -> {
                buyCol = "buy_15m";
                sellCol = "sell_15m";
            }
            default -> {
                buyCol = "buy_60m";
                sellCol = "sell_60m";
            }
        }

        String sql = """
                    SELECT item_id, COALESCE(%s,0) AS b, COALESCE(%s,0) AS s
                      FROM public.gw2_prices_tiers
                     WHERE item_id IN (:ids)
                """.formatted(buyCol, sellCol);

        List<Object[]> rows = Jpa.tx(em -> em.createNativeQuery(sql).setParameter("ids", ids).getResultList());

        Map<Integer, int[]> out = new HashMap<>();
        for (Object[] r : rows) {
            out.put(((Number) r[0]).intValue(),
                    new int[] { ((Number) r[1]).intValue(), ((Number) r[2]).intValue() });
        }
        // fill absent ids with [0,0]
        for (Integer id : ids)
            out.putIfAbsent(id, new int[] { 0, 0 });
        return out;
    }

    // Read one (used by OverlayEngine fallback)
    public static Integer vendorValueById(int itemId) {
        return Jpa.tx(em -> {
            var r = em.createNativeQuery("""
                        SELECT vendor_value FROM public.gw2_prices WHERE item_id = :id
                    """).setParameter("id", itemId).getResultList();
            if (r.isEmpty())
                return null;
            Object o = r.get(0);
            return (o == null) ? null : ((Number) o).intValue();
        });
    }

    // Batch upsert vendor values we just fetched
    public static void upsertVendorValues(Map<Integer, Integer> vendorMap) {
        if (vendorMap == null || vendorMap.isEmpty())
            return;
        Jpa.txVoid(em -> {
            var sb = new StringBuilder();
            sb.append("""
                        INSERT INTO public.gw2_prices(item_id, buy, sell, vendor_value, updated_at, ts)
                        VALUES
                    """);
            boolean first = true;
            for (var e : vendorMap.entrySet()) {
                if (!first)
                    sb.append(',');
                first = false;
                // buy/sell placeholders keep existing values via ON CONFLICT
                sb.append("(").append(e.getKey()).append(",0,0,").append(e.getValue()).append(",now(),now())");
            }
            sb.append("""
                        ON CONFLICT (item_id) DO UPDATE
                          SET vendor_value = EXCLUDED.vendor_value,
                              updated_at   = now()
                    """);
            em.createNativeQuery(sb.toString()).executeUpdate();
        });
    }

}

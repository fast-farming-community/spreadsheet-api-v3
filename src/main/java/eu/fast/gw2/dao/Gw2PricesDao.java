package eu.fast.gw2.dao;

import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import eu.fast.gw2.jpa.Jpa;

public class Gw2PricesDao {

    /**
     * Load tiered prices for the given item ids from gw2_prices_tiers.
     * Returns id -> [buy, sell]. Missing rows map to [0,0].
     */
    @SuppressWarnings("unchecked")
    // eu.fast.gw2.dao.Gw2PricesDao
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
            case "20m" -> {
                buyCol = "buy_20m";
                sellCol = "sell_20m";
            }
            case "30m" -> {
                buyCol = "buy_30m";
                sellCol = "sell_30m";
            }
            default -> {
                buyCol = "buy_60m";
                sellCol = "sell_60m";
            } // fallback
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

}

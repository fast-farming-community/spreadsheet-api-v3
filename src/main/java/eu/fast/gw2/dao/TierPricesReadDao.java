package eu.fast.gw2.dao;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import eu.fast.gw2.enums.Tier;
import eu.fast.gw2.jpa.Jpa;

public class TierPricesReadDao {

    /**
     * Returns id -> [buy, sell] for the requested tier.
     * Fallback: if tier column is NULL, transparently try 60m.
     * If still NULL, returns [0,0] and OverlayEngine can vendor-fallback.
     */
    public static Map<Integer, int[]> loadByTier(Set<Integer> ids, Tier tier) {
        if (ids == null || ids.isEmpty())
            return Map.of();

        String colB = tier.colBuy, colS = tier.colSell;
        // We also want fallback to 60m if requested tier is null:
        String colB60 = Tier.T60M.colBuy, colS60 = Tier.T60M.colSell;

        String sql = ""
                + "SELECT item_id,\n"
                + "       COALESCE(" + colB + ", " + colB60 + ", 0) AS b,\n"
                + "       COALESCE(" + colS + ", " + colS60 + ", 0) AS s\n"
                + "FROM public.gw2_prices_tiers\n"
                + "WHERE item_id = ANY(:ids)";

        return Jpa.tx(em -> {
            var q = em.createNativeQuery(sql)
                    .setParameter("ids", ids);
            @SuppressWarnings("unchecked")
            List<Object[]> rows = q.getResultList();

            Map<Integer, int[]> out = new HashMap<>();
            for (Object[] r : rows) {
                int id = ((Number) r[0]).intValue();
                int buy = ((Number) r[1]).intValue();
                int sell = ((Number) r[2]).intValue();
                out.put(id, new int[] { buy, sell });
            }
            return out;
        });
    }
}

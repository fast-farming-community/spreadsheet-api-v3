package eu.fast.gw2.dao;

import java.util.HashMap;
import java.util.Map;
import java.util.Set;

import eu.fast.gw2.jpa.Jpa;

public class Gw2PricesDao {
    /** returns map id -> [buy, sell] in copper */
    public static Map<Integer, int[]> loadLatest(Set<Integer> ids) {
        if (ids == null || ids.isEmpty())
            return new HashMap<>();
        return Jpa.tx(em -> {
            var q = em.createNativeQuery("""
                        SELECT item_id, buy, sell
                          FROM public.gw2_prices
                         WHERE item_id = ANY(:ids)
                    """);
            q.setParameter("ids", ids.toArray(Integer[]::new));
            var res = q.getResultList();
            Map<Integer, int[]> out = new HashMap<>();
            for (Object row : res) {
                Object[] a = (Object[]) row;
                int id = ((Number) a[0]).intValue();
                int buy = ((Number) a[1]).intValue();
                int sell = ((Number) a[2]).intValue();
                out.put(id, new int[] { buy, sell });
            }
            return out;
        });
    }
}

package eu.fast.gw2.dao;

import java.util.Map;

import eu.fast.gw2.enums.Tier;
import eu.fast.gw2.tools.Jpa;

public class TierPricesDao {

    public static void upsertTier(Tier tier, Map<Integer, int[]> itemToBuySell) {
        if (itemToBuySell == null || itemToBuySell.isEmpty())
            return;

        final String buyCol = "buy_" + tier.label;
        final String sellCol = "sell_" + tier.label;
        final String tsCol = "ts_" + tier.label;

        // One canonical SQL string using the dynamic tier columns
        final String SQL = ("""
                    INSERT INTO public.gw2_prices_tiers (item_id, %s, %s, %s, updated_at)
                    VALUES (:id, :b, :s, now(), now())
                    ON CONFLICT (item_id) DO UPDATE
                       SET %s = EXCLUDED.%s,
                           %s = EXCLUDED.%s,
                           %s = now(),
                           updated_at = now()
                """).formatted(buyCol, sellCol, tsCol,
                buyCol, buyCol,
                sellCol, sellCol,
                tsCol);

        final int FLUSH_BATCH = 500;

        // Execute row-by-row (JPA doesn’t batch parameters by default)
        Jpa.txVoid(em -> {
            var q = em.createNativeQuery(SQL);
            int i = 0;
            for (Map.Entry<Integer, int[]> e : itemToBuySell.entrySet()) {
                int[] ps = e.getValue();
                q.setParameter("id", e.getKey());
                q.setParameter("b", (ps != null && ps.length > 0) ? ps[0] : 0);
                q.setParameter("s", (ps != null && ps.length > 1) ? ps[1] : 0);
                q.executeUpdate();

                if (++i % FLUSH_BATCH == 0) {
                    em.flush();
                    em.clear();
                }
            }
        });
    }

    public static void upsertActivity(Map<Integer, Integer> activity) {
        if (activity == null || activity.isEmpty())
            return;

        final String SQL = """
                    INSERT INTO public.gw2_prices_tiers (item_id, activity_last, activity_ts, updated_at)
                    VALUES (:id, :a, now(), now())
                    ON CONFLICT (item_id) DO UPDATE
                       SET activity_last = EXCLUDED.activity_last,
                           activity_ts   = now(),
                           updated_at    = now()
                """;

        Jpa.txVoid(em -> {
            var q = em.createNativeQuery(SQL);
            int i = 0;
            for (var e : activity.entrySet()) {
                q.setParameter("id", e.getKey());
                q.setParameter("a", e.getValue());
                q.executeUpdate();
                if (++i % 500 == 0) {
                    em.flush();
                    em.clear();
                }
            }
        });
    }
}

package eu.fast.gw2.dao;

import java.util.Map;
import java.util.Set;

import eu.fast.gw2.enums.Tier;
import eu.fast.gw2.tools.Jpa;

public class TierPricesDao {

    public static void upsertTier(Tier tier, Map<Integer, int[]> itemToBuySell) {
        if (itemToBuySell == null || itemToBuySell.isEmpty())
            return;

        final String buyCol = "buy_" + tier.label;
        final String sellCol = "sell_" + tier.label;
        final String tsCol = "ts_" + tier.label;

        final String SQL = ("""
                    INSERT INTO public.gw2_prices_tiers (item_id, %s, %s, %s, updated_at)
                    VALUES (:id, :b, :s, now(), now())
                    ON CONFLICT (item_id) DO UPDATE
                       SET %s = EXCLUDED.%s,
                           %s = EXCLUDED.%s,
                           %s = now(),
                           updated_at = now()
                """).formatted(buyCol, sellCol, tsCol, buyCol, buyCol, sellCol, sellCol, tsCol);

        final int FLUSH_BATCH = 500;

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

    /**
     * One-shot multi-tier upsert.
     * - Always writes 5m (buy/sell + ts_5m=now()).
     * - Writes 10m/15m/60m only when the ID is in the respective due set (else
     * leaves columns as-is).
     * - Writes activity_last/activity_ts for all rows.
     * 
     * @return number of rows inserted/updated (size of input map).
     */
    public static int upsertMulti(Map<Integer, int[]> prices,
            Set<Integer> due10,
            Set<Integer> due15,
            Set<Integer> due60,
            Map<Integer, Integer> activity) {

        if (prices == null || prices.isEmpty())
            return 0;

        // Build one big INSERT ... VALUES ... ON CONFLICT DO UPDATE
        StringBuilder sb = new StringBuilder(1024 + prices.size() * 64);
        sb.append("""
                    INSERT INTO public.gw2_prices_tiers (
                        item_id,
                        buy_5m,  sell_5m,  ts_5m,
                        buy_10m, sell_10m, ts_10m,
                        buy_15m, sell_15m, ts_15m,
                        buy_60m, sell_60m, ts_60m,
                        activity_last, activity_ts, updated_at
                    ) VALUES
                """);

        boolean first = true;
        for (var e : prices.entrySet()) {
            int id = e.getKey();
            int buy = (e.getValue() != null && e.getValue().length > 0) ? e.getValue()[0] : 0;
            int sell = (e.getValue() != null && e.getValue().length > 1) ? e.getValue()[1] : 0;
            int act = (activity != null && activity.containsKey(id)) ? activity.get(id) : 0;

            boolean d10 = due10 != null && due10.contains(id);
            boolean d15 = due15 != null && due15.contains(id);
            boolean d60 = due60 != null && due60.contains(id);

            if (!first)
                sb.append(',');
            first = false;

            sb.append('(').append(id).append(',')
                    // 5m always
                    .append(buy).append(',').append(sell).append(", now(),")
                    // 10m conditional
                    .append(d10 ? String.valueOf(buy) : "NULL").append(',')
                    .append(d10 ? String.valueOf(sell) : "NULL").append(',')
                    .append(d10 ? "now()" : "NULL").append(',')
                    // 15m conditional
                    .append(d15 ? String.valueOf(buy) : "NULL").append(',')
                    .append(d15 ? String.valueOf(sell) : "NULL").append(',')
                    .append(d15 ? "now()" : "NULL").append(',')
                    // 60m conditional
                    .append(d60 ? String.valueOf(buy) : "NULL").append(',')
                    .append(d60 ? String.valueOf(sell) : "NULL").append(',')
                    .append(d60 ? "now()" : "NULL").append(',')
                    // activity
                    .append(act).append(", now(), now())");
        }

        sb.append("""
                    ON CONFLICT (item_id) DO UPDATE SET
                      -- 5m always refresh
                      buy_5m  = EXCLUDED.buy_5m,
                      sell_5m = EXCLUDED.sell_5m,
                      ts_5m   = EXCLUDED.ts_5m,

                      -- 10m/15m/60m only when provided (EXCLUDED is non-null)
                      buy_10m  = COALESCE(EXCLUDED.buy_10m,  gw2_prices_tiers.buy_10m),
                      sell_10m = COALESCE(EXCLUDED.sell_10m, gw2_prices_tiers.sell_10m),
                      ts_10m   = COALESCE(EXCLUDED.ts_10m,   gw2_prices_tiers.ts_10m),

                      buy_15m  = COALESCE(EXCLUDED.buy_15m,  gw2_prices_tiers.buy_15m),
                      sell_15m = COALESCE(EXCLUDED.sell_15m, gw2_prices_tiers.sell_15m),
                      ts_15m   = COALESCE(EXCLUDED.ts_15m,   gw2_prices_tiers.ts_15m),

                      buy_60m  = COALESCE(EXCLUDED.buy_60m,  gw2_prices_tiers.buy_60m),
                      sell_60m = COALESCE(EXCLUDED.sell_60m, gw2_prices_tiers.sell_60m),
                      ts_60m   = COALESCE(EXCLUDED.ts_60m,   gw2_prices_tiers.ts_60m),

                      activity_last = EXCLUDED.activity_last,
                      activity_ts   = EXCLUDED.activity_ts,
                      updated_at    = now()
                """);

        final String sql = sb.toString();
        Jpa.txVoid(em -> em.createNativeQuery(sql).executeUpdate());
        return prices.size();
    }
}

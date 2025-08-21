package eu.fast.gw2.dao;

import eu.fast.gw2.jpa.Jpa;
import eu.fast.gw2.enums.Tier;

import java.util.List;
import java.util.Map;

public class TierPricesDao {

    public static void upsertTier(Tier tier, Map<Integer, int[]> itemToBuySell) {
        if (itemToBuySell == null || itemToBuySell.isEmpty())
            return;

        // Build a VALUES list for batch upsert
        // We keep column names strictly from the enum (no user input), so dynamic SQL
        // is safe.
        final String sql = ""
                + "INSERT INTO public.gw2_prices_tiers (item_id, " + tier.colBuy + ", " + tier.colSell + ", "
                + tier.colTs + ", updated_at)\n"
                + "VALUES (:id, :buy, :sell, now(), now())\n"
                + "ON CONFLICT (item_id) DO UPDATE SET\n"
                + "  " + tier.colBuy + " = EXCLUDED." + tier.colBuy + ",\n"
                + "  " + tier.colSell + " = EXCLUDED." + tier.colSell + ",\n"
                + "  " + tier.colTs + " = now(),\n"
                + "  updated_at       = now()";

        // Chunk to avoid too many statements in one transaction if the set is large
        final int BATCH = 500; // statement reuse; GW2 API limit is ≤200 per call, but DB can do bigger batches
        List<Map.Entry<Integer, int[]>> entries = itemToBuySell.entrySet().stream().toList();

        Jpa.txVoid(em -> {
            for (int i = 0; i < entries.size(); i++) {
                var e = entries.get(i);
                var q = em.createNativeQuery(sql)
                        .setParameter("id", e.getKey())
                        .setParameter("buy", e.getValue()[0])
                        .setParameter("sell", e.getValue()[1]);
                q.executeUpdate();

                // Simple pacing every BATCH rows (optional)
                if (i % BATCH == 0) {
                    em.flush();
                    em.clear();
                }
            }
        });
    }
}

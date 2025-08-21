package eu.fast.gw2.main;

import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Collectors;

import eu.fast.gw2.dao.Gw2PricesDao;
import eu.fast.gw2.dao.TierPricesDao;
import eu.fast.gw2.enums.Tier;
import eu.fast.gw2.tools.Gw2ApiClient;
import eu.fast.gw2.tools.Jpa;

public class RefreshTierPrices {

    public static void main(String[] args) throws Exception {
        // tier: 5|10|15|60 (with or without trailing 'm')
        String rawTier = arg(args, new String[] { "--tier=", "--tiers=", "-t=" }, "60");
        String tierArg = normalizeTier(rawTier);
        Tier tier = Tier.parse(tierArg);

        // Overall cap for THIS run (not the batch size). Null = unlimited.
        String limStr = arg(args, new String[] { "--limit=", "-l=" }, null);
        Integer overallCap = parseLimit(limStr);

        // inter-batch sleep (ms)
        int sleepMs = Integer.parseInt(Optional
                .ofNullable(System.getenv("GW2API_SLEEP_MS"))
                .orElse("150"));

        final int BATCH = 200; // GW2 API cap

        System.out.println(">>> RefreshTierPrices tier=" + tierArg +
                " overallCap=" + (overallCap == null ? "all" : overallCap) +
                " batch=" + BATCH + " sleepMs=" + sleepMs);

        // 1) pick only stale (or vendor-missing) items
        List<Integer> allIds = pickStaleItemIds(tier, overallCap);
        if (allIds.isEmpty()) {
            System.out.println("No stale items for " + tier.name() + ". Nothing to do.");
            return;
        }

        int total = allIds.size();
        int written = 0;

        for (int off = 0; off < total; off += BATCH) {
            int end = Math.min(off + BATCH, total);
            List<Integer> ids = allIds.subList(off, end);

            // 2) fetch items (flags + vendor) and prices for THIS batch
            Map<Integer, Boolean> isBound = Collections.emptyMap();
            Map<Integer, Integer> vendor = Collections.emptyMap();
            Map<Integer, int[]> prices = Collections.emptyMap();

            try {
                var items = Gw2ApiClient.fetchItemsBatch(ids);
                isBound = Gw2ApiClient.accountBoundMap(items);
                vendor = Gw2ApiClient.vendorValueMap(items);

                // keep your existing price fetch that returns Map<Integer,int[]>
                prices = Gw2ApiClient.fetchPrices(ids);
            } catch (Exception e) {
                System.err.println(
                        "Batch " + (off / BATCH + 1) + ": fetch failed (" + e.getMessage() + "). Skipping batch.");
                Thread.sleep(Math.max(sleepMs, 500));
                continue;
            }

            // merge + enforce AccountBound => 0/0
            Map<Integer, int[]> toWrite = new HashMap<>(ids.size());
            for (Integer id : ids) {
                int[] ps = prices.getOrDefault(id, new int[] { 0, 0 });
                if (Boolean.TRUE.equals(isBound.get(id))) {
                    ps = new int[] { 0, 0 };
                }
                toWrite.put(id, ps);
            }

            // build activity = buys.quantity + sells.quantity (fallback 0)
            Map<Integer, Integer> activity = new HashMap<>(ids.size());
            for (var p : Gw2ApiClient.fetchPricesBatch(ids)) {
                int qBuy = (p.buys() == null ? 0 : Math.max(0, p.buys().quantity()));
                int qSell = (p.sells() == null ? 0 : Math.max(0, p.sells().quantity()));
                activity.put(p.id(), qBuy + qSell);
            }

            // tier prices + vendor values
            try {
                TierPricesDao.upsertTier(tier, toWrite);
                Gw2PricesDao.upsertVendorValues(vendor);
                written += toWrite.size();
            } catch (Exception e) {
                System.err.println("Batch " + (off / BATCH + 1) + ": DB upsert failed (" + e.getMessage() + ").");
            }

            System.out.printf(Locale.ROOT, "  [%d/%d] upserted into %s%n", end, total, tier.name());

            if (end < total && sleepMs > 0) {
                try {
                    Thread.sleep(sleepMs);
                } catch (InterruptedException ignored) {
                }
            }
        }

        System.out.println("Done. Wrote " + written + " rows into tier " + tier.name());
    }

    private static List<Integer> pickStaleItemIds(Tier tier, Integer overallCap) {
        final String tsCol = "ts_" + tier.label; // ts_5m|ts_10m|ts_15m|ts_60m

        final int fastMin = Integer.parseInt(tier.label.replace("m", "")); // 5,10,15,60
        final int slowMin = 60;

        final int activityThreshold = Integer.parseInt(
                Optional.ofNullable(System.getenv("GW2_LISTINGS_THRESHOLD")).orElse("5000"));

        return Jpa.tx(em -> {
            String sql = """
                        WITH candidates AS (
                          SELECT i.id,
                                 t.%1$s        AS ts_tier,
                                 COALESCE(t.activity_last, 0) AS activity,
                                 gp.vendor_value
                          FROM public.items i
                          LEFT JOIN public.gw2_prices_tiers t ON t.item_id = i.id
                          LEFT JOIN public.gw2_prices      gp ON gp.item_id = i.id
                          WHERE i.tradable = true
                        )
                        SELECT id
                        FROM candidates
                        WHERE
                          -- vendor backfill
                          vendor_value IS NULL
                          OR
                          -- adaptive window:
                          (
                            ts_tier IS NULL
                            OR ts_tier < now() - (
                                  CASE WHEN activity >= :thr
                                       THEN make_interval(mins := :fast)
                                       ELSE make_interval(mins := :slow)
                                  END
                              )
                          )
                        ORDER BY
                          COALESCE(ts_tier, TIMESTAMP 'epoch') ASC, id ASC
                        %2$s
                    """.formatted(tsCol, (overallCap == null ? "" : "LIMIT :lim"));

            var q = em.createNativeQuery(sql);
            q.setParameter("thr", activityThreshold);
            q.setParameter("fast", fastMin);
            q.setParameter("slow", slowMin);
            if (overallCap != null)
                q.setParameter("lim", overallCap);

            @SuppressWarnings("unchecked")
            var rows = (List<Number>) q.getResultList();
            System.out.printf(Locale.ROOT,
                    "Stale(%s): picked=%d (thr=%d, fast=%dm, slow=%dm)%n",
                    tier.label, rows.size(), activityThreshold, fastMin, slowMin);

            return rows.stream().map(Number::intValue).collect(Collectors.toList());
        });
    }

    // ---- small args/utils ----

    private static String arg(String[] args, String[] keys, String def) {
        for (String a : args) {
            for (String k : keys) {
                if (a.startsWith(k))
                    return a.substring(k.length());
            }
        }
        return def;
    }

    private static String normalizeTier(String s) {
        if (s == null || s.isBlank())
            return "60m";
        String t = s.trim().toLowerCase(Locale.ROOT);
        if (t.startsWith("t"))
            t = t.substring(1);
        if (!t.endsWith("m"))
            t = t + "m";
        return switch (t) {
            case "5m", "10m", "15m", "60m" -> t;
            default -> {
                System.out.println("! unknown tier '" + s + "', defaulting to 60m");
                yield "60m";
            }
        };
    }

    private static Integer parseLimit(String s) {
        if (s == null)
            return null;
        String v = s.trim().toLowerCase(Locale.ROOT);
        if (v.isEmpty() || v.equals("all") || v.equals("0"))
            return null;
        try {
            int n = Integer.parseInt(v);
            return (n <= 0) ? null : n;
        } catch (Exception e) {
            return null;
        }
    }
}

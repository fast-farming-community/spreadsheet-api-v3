package eu.fast.gw2.main;

import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Collectors;

import eu.fast.gw2.dao.TierPricesDao;
import eu.fast.gw2.enums.Tier;
import eu.fast.gw2.gw2api.Gw2ApiClient;
import eu.fast.gw2.jpa.Jpa;

public class RefreshTierPrices {

    public static void main(String[] args) throws Exception {
        // tier: 5|10|20|30|60 (with or without trailing 'm')
        String rawTier = arg(args, new String[] { "--tier=", "--tiers=", "-t=" }, "60");
        String tierArg = normalizeTier(rawTier);
        Tier tier = Tier.parse(tierArg);

        // Overall cap for THIS run (optional). This is NOT the batch size.
        // Omit to process all items.
        String limStr = arg(args, new String[] { "--limit=", "-l=" }, null);
        Integer overallCap = parseLimit(limStr); // null = unlimited

        // Optional inter-batch sleep to be gentle with the API (ms). Default 150ms.
        int sleepMs = Integer.parseInt(
                Optional.ofNullable(System.getenv("GW2API_SLEEP_MS")).orElse("150"));

        // Hard batch size for the API (GW2 cap is 200).
        final int BATCH = 200;

        System.out.println(">>> RefreshTierPrices tier=" + tierArg +
                " overallCap=" + (overallCap == null ? "all" : overallCap) +
                " batch=" + BATCH + " sleepMs=" + sleepMs);

        // 1) Get ALL candidate item ids (tradable first)
        List<Integer> allIds = pickCandidateItemIds(overallCap);
        if (allIds.isEmpty()) {
            System.out.println("No candidate items found.");
            return;
        }

        int total = allIds.size();
        int written = 0;

        for (int off = 0; off < total; off += BATCH) {
            int end = Math.min(off + BATCH, total);
            List<Integer> ids = allIds.subList(off, end);

            // 2) Fetch flags + prices for THIS batch
            Map<Integer, Boolean> isBound = Collections.emptyMap();
            Map<Integer, int[]> prices = Collections.emptyMap();
            try {
                isBound = Gw2ApiClient.fetchAccountBoundFlags(ids); // id -> true if AccountBound
                prices = Gw2ApiClient.fetchPrices(ids); // id -> [buy, sell]
            } catch (Exception e) {
                System.err.println(
                        "Batch " + (off / BATCH + 1) + ": fetch failed (" + e.getMessage() + "). Skipping batch.");
                // Optional: backoff a bit on error
                Thread.sleep(Math.max(sleepMs, 500));
                continue;
            }

            // 3) Merge and enforce AccountBound = 0/0
            Map<Integer, int[]> toWrite = new HashMap<>(ids.size());
            for (Integer id : ids) {
                int[] ps = prices.getOrDefault(id, new int[] { 0, 0 });
                if (Boolean.TRUE.equals(isBound.get(id))) {
                    ps = new int[] { 0, 0 };
                }
                toWrite.put(id, ps);
            }

            // 4) Upsert this batch into the requested tier
            try {
                TierPricesDao.upsertTier(tier, toWrite);
                written += toWrite.size();
            } catch (Exception e) {
                System.err.println("Batch " + (off / BATCH + 1) + ": DB upsert failed (" + e.getMessage() + ").");
            }

            // Progress + throttle
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

    /**
     * Keep your old arg(String,String,String); add this overload to accept multiple
     * prefixes
     */
    private static String arg(String[] args, String[] keys, String def) {
        for (String a : args) {
            for (String k : keys) {
                if (a.startsWith(k))
                    return a.substring(k.length());
            }
        }
        return def;
    }

    /** Accept "5", "5m", "t5m", "T5M" and normalize to "5m" etc. */
    private static String normalizeTier(String s) {
        if (s == null || s.isBlank())
            return "60m";
        String t = s.trim().toLowerCase(Locale.ROOT);
        // strip leading 't'
        if (t.startsWith("t"))
            t = t.substring(1);
        // ensure trailing 'm'
        if (!t.endsWith("m"))
            t = t + "m";
        // keep only known values
        switch (t) {
            case "5m":
            case "10m":
            case "20m":
            case "30m":
            case "60m":
                return t;
            default:
                System.out.println("! unknown tier '" + s + "', defaulting to 60m");
                return "60m";
        }
    }

    /** null = unlimited; >=1 = limit */
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

    private static List<Integer> pickCandidateItemIds(Integer overallCap) {
        return Jpa.tx(em -> {
            String base = """
                        SELECT id
                        FROM public.items
                        WHERE tradable = true
                        ORDER BY updated_at DESC
                    """;
            var q = em.createNativeQuery(overallCap != null ? base + " LIMIT :lim" : base);
            if (overallCap != null)
                q.setParameter("lim", overallCap);

            List<?> rows = q.getResultList();
            return rows.stream().map(o -> ((Number) o).intValue()).collect(Collectors.toList());
        });
    }
}

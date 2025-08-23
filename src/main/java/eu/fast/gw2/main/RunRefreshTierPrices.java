package eu.fast.gw2.main;

import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;

import eu.fast.gw2.dao.Gw2PricesDao;
import eu.fast.gw2.dao.TierPricesDao;
import eu.fast.gw2.tools.Gw2ApiClient;
import eu.fast.gw2.tools.Jpa;

public class RunRefreshTierPrices {

    public static void main(String[] args) throws Exception {
        String limStr = arg(args, new String[] { "--limit=", "-l=" }, null);
        Integer overallCap = parseLimit(limStr);

        int sleepMs = Integer.parseInt(Optional.ofNullable(System.getenv("GW2API_SLEEP_MS")).orElse("150"));
        final int BATCH = 200;

        System.out.println(">>> RefreshTierPrices (multi-tier) overallCap=" +
                (overallCap == null ? "all" : overallCap) +
                " batch=" + BATCH + " sleepMs=" + sleepMs);

        List<DueRow> candidates = pickDueCandidates(overallCap);
        if (candidates.isEmpty()) {
            System.out.println("No stale items for any tier. Nothing to do.");
            return;
        }
        System.out.println("Picked " + candidates.size() + " items to refresh.");

        int total = candidates.size();
        int batches = (total + BATCH - 1) / BATCH;

        // totals
        int total5m = 0, total10m = 0, total15m = 0, total60m = 0, totalVendorUpdated = 0;

        for (int bi = 0; bi < batches; bi++) {
            int off = bi * BATCH;
            int end = Math.min(off + BATCH, total);
            var rows = candidates.subList(off, end);
            var ids = rows.stream().map(DueRow::id).collect(Collectors.toList());

            Set<Integer> due10 = rows.stream().filter(DueRow::due10).map(DueRow::id).collect(Collectors.toSet());
            Set<Integer> due15 = rows.stream().filter(DueRow::due15).map(DueRow::id).collect(Collectors.toSet());
            Set<Integer> due60 = rows.stream().filter(DueRow::due60).map(DueRow::id).collect(Collectors.toSet());
            Set<Integer> vendorMissing = rows.stream().filter(DueRow::vendorMissing).map(DueRow::id)
                    .collect(Collectors.toSet());

            Map<Integer, Boolean> isBound;
            Map<Integer, Integer> vendor; // from API (subset will be used)
            Iterable<Gw2ApiClient.PriceEntry> priceEntries;
            try {
                var items = withRetry(() -> Gw2ApiClient.fetchItemsBatch(ids), 3, sleepMs, "items");
                isBound = Gw2ApiClient.accountBoundMap(items);
                vendor = Gw2ApiClient.vendorValueMap(items);
                priceEntries = withRetry(() -> Gw2ApiClient.fetchPricesBatch(ids), 3, sleepMs, "pricesBatch");
            } catch (Exception e) {
                System.err.println("Batch " + (bi + 1) + ": fetch failed (" + e.getMessage() + "). Skipping batch.");
                Thread.sleep(Math.max(sleepMs, 500));
                continue;
            }

            int cap = Math.max(16, ids.size() * 2);
            Map<Integer, int[]> normalized = new HashMap<>(cap);
            Map<Integer, Integer> activity = new HashMap<>(cap);

            for (var p : priceEntries) {
                int id = p.id();
                int buyQty = (p.buys() == null ? 0 : Math.max(0, p.buys().quantity()));
                int sellQty = (p.sells() == null ? 0 : Math.max(0, p.sells().quantity()));
                int buyUnit = (p.buys() == null ? 0 : Math.max(0, p.buys().unitPrice()));
                int sellUnit = (p.sells() == null ? 0 : Math.max(0, p.sells().unitPrice()));

                activity.put(id, buyQty + sellQty);
                if (Boolean.TRUE.equals(isBound.get(id))) {
                    normalized.put(id, new int[] { 0, 0 });
                } else {
                    normalized.put(id, new int[] { buyUnit, sellUnit });
                }
            }
            for (Integer id : ids) { // ensure presence
                normalized.putIfAbsent(id, new int[] { 0, 0 });
                activity.putIfAbsent(id, 0);
            }

            try {
                // Write all tiers in one call (5m always, coarse only if due)
                TierPricesDao.upsertMulti(normalized, due10, due15, due60, activity);

                // Logging (concise)
                int updated5m = normalized.size();
                total5m += updated5m;

                if (!due10.isEmpty()) {
                    total10m += due10.size();
                    System.out.println("buy_10m & sell_10m updated for ids: " + formatIds(due10, 50));
                }
                if (!due15.isEmpty()) {
                    total15m += due15.size();
                    System.out.println("buy_15m & sell_15m updated for ids: " + formatIds(due15, 50));
                }
                if (!due60.isEmpty()) {
                    total60m += due60.size();
                    System.out.println("buy_60m & sell_60m updated for ids: " + formatIds(due60, 50));
                }

                // Vendor: only insert for rows that are missing in DB (no per-batch diff-check)
                if (!vendorMissing.isEmpty() && !vendor.isEmpty()) {
                    Map<Integer, Integer> vendorToInsert = vendor.entrySet().stream()
                            .filter(e -> vendorMissing.contains(e.getKey()))
                            .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));

                    if (!vendorToInsert.isEmpty()) {
                        Gw2PricesDao.upsertVendorValues(vendorToInsert);
                        totalVendorUpdated += vendorToInsert.size();
                        System.out.println("vendor_value updated for ids: " + formatIds(vendorToInsert.keySet(), 50));
                    }
                }
            } catch (Exception e) {
                System.err.println("Batch " + (bi + 1) + ": DB upsert failed (" + e.getMessage() + ").");
            }

            System.out.printf(Locale.ROOT, "  [%d/%d] batch done: 5m=%d, 10m+=%d, 15m+=%d, 60m+=%d%n",
                    end, total, normalized.size(), due10.size(), due15.size(), due60.size());

            if (end < total && sleepMs > 0) {
                try {
                    Thread.sleep(sleepMs);
                } catch (InterruptedException ignored) {
                }
            }
        }

        System.out.println("Done. Summary:");
        System.out.println("  5m updated count = " + total5m);
        System.out.println("  10m updated count = " + total10m);
        System.out.println("  15m updated count = " + total15m);
        System.out.println("  60m updated count = " + total60m);
        System.out.println("  vendor_value updated count = " + totalVendorUpdated);
    }

    /** one-shot candidate picker with due flags for 10/15/60 + vendorMissing */
    private static List<DueRow> pickDueCandidates(Integer overallCap) {
        final int fastMin = 5, slowMin = 60;
        final int activityThreshold = Integer.parseInt(
                Optional.ofNullable(System.getenv("GW2_LISTINGS_THRESHOLD")).orElse("5000"));

        return Jpa.tx(em -> {
            String sql = """
                        WITH candidates AS (
                          SELECT i.id,
                                 t.ts_5m, t.ts_10m, t.ts_15m, t.ts_60m,
                                 COALESCE(t.activity_last, 0) AS activity,
                                 gp.vendor_value
                          FROM public.items i
                          LEFT JOIN public.gw2_prices_tiers t ON t.item_id = i.id
                          LEFT JOIN public.gw2_prices      gp ON gp.item_id = i.id
                          WHERE i.tradable = true
                        )
                        SELECT id,
                               (ts_10m IS NULL OR ts_10m < now() - make_interval(mins := 10)) AS due10,
                               (ts_15m IS NULL OR ts_15m < now() - make_interval(mins := 15)) AS due15,
                               (ts_60m IS NULL OR ts_60m < now() - make_interval(mins := 60)) AS due60,
                               (vendor_value IS NULL) AS vendorMissing
                        FROM candidates
                        WHERE
                          vendor_value IS NULL
                          OR
                          ts_5m IS NULL
                          OR ts_5m < now() - (
                                CASE WHEN activity >= :thr
                                     THEN make_interval(mins := :fast)
                                     ELSE make_interval(mins := :slow)
                                END
                          )
                          OR ts_10m IS NULL OR ts_10m < now() - make_interval(mins := 10)
                          OR ts_15m IS NULL OR ts_15m < now() - make_interval(mins := 15)
                          OR ts_60m IS NULL OR ts_60m < now() - make_interval(mins := 60)
                        ORDER BY
                          LEAST(
                            COALESCE(ts_5m,  TIMESTAMP 'epoch'),
                            COALESCE(ts_10m, TIMESTAMP 'epoch'),
                            COALESCE(ts_15m, TIMESTAMP 'epoch'),
                            COALESCE(ts_60m, TIMESTAMP 'epoch')
                          ) ASC, id ASC
                        %s
                    """.formatted(overallCap == null ? "" : "LIMIT :lim");

            var q = em.createNativeQuery(sql);
            q.setParameter("thr", activityThreshold);
            q.setParameter("fast", fastMin);
            q.setParameter("slow", slowMin);
            if (overallCap != null)
                q.setParameter("lim", overallCap);

            @SuppressWarnings("unchecked")
            List<Object[]> rows = q.getResultList();
            System.out.printf(Locale.ROOT, "Stale: picked=%d (thr=%d, fast=%dm, slow=%dm)%n",
                    rows.size(), activityThreshold, fastMin, slowMin);

            return rows.stream()
                    .map(r -> new DueRow(((Number) r[0]).intValue(),
                            r[1] != null && (Boolean) r[1],
                            r[2] != null && (Boolean) r[2],
                            r[3] != null && (Boolean) r[3],
                            r[4] != null && (Boolean) r[4]))
                    .collect(Collectors.toList());
        });
    }

    // ---- logging helpers

    private static String formatIds(Iterable<Integer> ids, int max) {
        var list = new java.util.ArrayList<Integer>();
        for (Integer i : ids)
            list.add(i);
        java.util.Collections.sort(list);
        if (list.size() <= max)
            return list.toString();
        var head = list.subList(0, max);
        return head.toString().replace("]", "") + ", ...(+" + (list.size() - max) + " more)]";
    }

    // ---- utils

    private static String arg(String[] args, String[] keys, String def) {
        for (String a : args)
            for (String k : keys)
                if (a.startsWith(k))
                    return a.substring(k.length());
        return def;
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

    @FunctionalInterface
    private interface ThrowingSupplier<T> {
        T get() throws Exception;
    }

    private static <T> T withRetry(ThrowingSupplier<T> action, int attempts, int baseSleepMs, String label)
            throws Exception {
        if (attempts <= 0)
            throw new IllegalArgumentException("attempts must be >= 1");
        for (int i = 0; i < attempts; i++) {
            try {
                return action.get();
            } catch (Exception e) {
                boolean lastAttempt = (i == attempts - 1);
                if (lastAttempt)
                    throw e;

                int backoff = Math.max(0, baseSleepMs) * Math.max(1, 1 << i);
                System.err.printf("  %s fetch failed (attempt %d/%d): %s%n", label, i + 1, attempts, e.getMessage());
                try {
                    Thread.sleep(backoff);
                } catch (InterruptedException ie) {
                    Thread.currentThread().interrupt();
                    throw ie;
                }
            }
        }
        throw new IllegalStateException("unreachable");
    }

    // --- helper holder
    private record DueRow(int id, boolean due10, boolean due15, boolean due60, boolean vendorMissing) {
    }
}

package eu.fast.gw2.tools;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

import eu.fast.gw2.dao.Gw2PricesDao;
import eu.fast.gw2.dao.TierPricesDao;

public class RefreshTierPrices {

    private static final int BATCH = 200;

    /** Execution summary for logging/metrics. */
    public record Summary(int picked, int updated2m, int updated10m, int updated60m, int vendorUpdated) {
    }

    /** Refactored runner (no main). */
    public static Summary refresh(Integer overallCap, int sleepMs) throws Exception {
        final long runStart = System.currentTimeMillis();

        System.out.println(">>> RefreshTierPrices (multi-tier) overallCap=" +
                (overallCap == null ? "all" : overallCap) +
                " batch=" + BATCH + " sleepMs=" + sleepMs);

        List<DueRow> candidates = pickDueCandidates(overallCap);
        if (candidates.isEmpty()) {
            System.out.println("No stale items for any tier. Nothing to do.");
            System.out.printf(Locale.ROOT, "RefreshTierPrices done in %.1fs%n",
                    (System.currentTimeMillis() - runStart) / 1000.0);
            return new Summary(0, 0, 0, 0, 0);
        }
        System.out.println("Picked " + candidates.size() + " items to refresh.");

        int total = candidates.size();
        int batches = (total + BATCH - 1) / BATCH;

        int total2m = 0, total10m = 0, total60m = 0, totalVendorUpdated = 0;

        for (int bi = 0; bi < batches; bi++) {
            int off = bi * BATCH;
            int end = Math.min(off + BATCH, total);
            List<DueRow> rows = candidates.subList(off, end);

            // Single pass over rows: collect everything with minimal allocations
            List<Integer> ids = new ArrayList<>(rows.size());
            Set<Integer> due10 = new HashSet<>();
            Set<Integer> due60 = new HashSet<>();
            Set<Integer> vendorMissing = new HashSet<>();
            Set<Integer> needItemsIds = new HashSet<>();

            boolean needImages = false;
            boolean needRarities = false;

            for (int i = 0; i < rows.size(); i++) {
                DueRow r = rows.get(i);
                int id = r.id();
                ids.add(id);
                if (r.due10())
                    due10.add(id);
                if (r.due60())
                    due60.add(id);
                if (r.vendorMissing()) {
                    vendorMissing.add(id);
                    needItemsIds.add(id);
                }
                if (r.imageMissing()) {
                    needImages = true;
                    needItemsIds.add(id);
                }
                if (r.rarityMissing()) {
                    needRarities = true;
                    needItemsIds.add(id);
                }
            }

            Map<Integer, Boolean> isBound; // empty when no /items fetch
            Map<Integer, Integer> vendor; // empty when no /items fetch
            List<Gw2ApiClient.PriceEntry> priceEntries;
            List<Gw2ApiClient.Item> items = List.of(); // stays empty if not needed

            try {
                // Prices are required every batch
                priceEntries = withRetry(() -> Gw2ApiClient.fetchPricesBatch(ids), 3, sleepMs, "pricesBatch");

                // /items only if there is any vendor/image/rarity gap
                if (!needItemsIds.isEmpty()) {
                    List<Integer> needList = new ArrayList<>(needItemsIds.size());
                    needList.addAll(needItemsIds);
                    items = withRetry(() -> Gw2ApiClient.fetchItemsBatch(needList), 3, sleepMs, "items");
                }

                isBound = Gw2ApiClient.accountBoundMap(items);
                vendor = Gw2ApiClient.vendorValueMap(items);

            } catch (Exception e) {
                System.err.println("Batch " + (bi + 1) + ": fetch failed (" + e.getMessage() + "). Skipping batch.");
                Thread.sleep(Math.max(sleepMs, 500));
                continue;
            }

            // Pre-size maps to avoid rehash
            int expected = Math.max(16, ids.size());
            Map<Integer, int[]> normalized = new HashMap<>(expected, 1.0f);
            Map<Integer, Integer> activity = new HashMap<>(expected, 1.0f);

            for (int i = 0; i < priceEntries.size(); i++) {
                Gw2ApiClient.PriceEntry p = priceEntries.get(i);
                int id = p.id();

                int buyQty = (p.buys() == null ? 0 : Math.max(0, p.buys().quantity()));
                int sellQty = (p.sells() == null ? 0 : Math.max(0, p.sells().quantity()));
                int buyUnit = (p.buys() == null ? 0 : Math.max(0, p.buys().unitPrice()));
                int sellUnit = (p.sells() == null ? 0 : Math.max(0, p.sells().unitPrice()));

                boolean acctBound = Boolean.TRUE.equals(isBound.get(id));
                activity.put(id, buyQty + sellQty);
                normalized.put(id, acctBound ? new int[] { 0, 0 } : new int[] { buyUnit, sellUnit });
            }
            // Ensure all ids are present (even if the API skipped some)
            for (int i = 0; i < ids.size(); i++) {
                int id = ids.get(i);
                normalized.putIfAbsent(id, new int[] { 0, 0 });
                activity.putIfAbsent(id, 0);
            }

            try {
                // Upsert tier prices
                TierPricesDao.upsertMulti(normalized, due10, due60, activity);

                total2m += normalized.size();
                total10m += due10.size();
                total60m += due60.size();

                // Vendor values (only for ones that were missing)
                if (!vendorMissing.isEmpty() && !vendor.isEmpty()) {
                    Map<Integer, Integer> vendorToInsert = new HashMap<>(vendorMissing.size());
                    for (Map.Entry<Integer, Integer> e : vendor.entrySet()) {
                        if (vendorMissing.contains(e.getKey())) {
                            vendorToInsert.put(e.getKey(), e.getValue());
                        }
                    }
                    if (!vendorToInsert.isEmpty()) {
                        Gw2PricesDao.upsertVendorValuesIfChanged(vendorToInsert);
                        totalVendorUpdated += vendorToInsert.size();
                    }
                }

                // image / rarity — only build maps we actually need
                if (!items.isEmpty()) {
                    if (needImages) {
                        Map<Integer, String> imagesMap = Gw2ApiClient.itemImageUrlMap(items);
                        if (!imagesMap.isEmpty()) {
                            try {
                                Gw2PricesDao.updateImagesIfChanged(imagesMap);
                            } catch (Exception ignored) {
                            }
                        }
                    }
                    if (needRarities) {
                        Map<Integer, String> raritiesMap = Gw2ApiClient.itemRarityMap(items);
                        if (!raritiesMap.isEmpty()) {
                            try {
                                Gw2PricesDao.updateRaritiesIfChanged(raritiesMap);
                            } catch (Exception ignored) {
                            }
                        }
                    }
                }
            } catch (Exception e) {
                System.err.println("Batch " + (bi + 1) + ": DB upsert failed (" + e.getMessage() + ").");
            }

            // --- concise batch line only ---
            System.out.printf(Locale.ROOT,
                    "  [%d/%d] batch done: 2m=%d, 10m+=%d, 60m+=%d%n",
                    end, total, normalized.size(), due10.size(), due60.size());

            if (end < total && sleepMs > 0) {
                try {
                    Thread.sleep(sleepMs);
                } catch (InterruptedException ie) {
                    Thread.currentThread().interrupt();
                }
            }
        }

        System.out.println(String.format(
                Locale.ROOT,
                "Updated RefreshTierPrices: 2m=%d, 10m=%d, 60m=%d, vendor=%d (%.1fs)",
                total2m, total10m, total60m, totalVendorUpdated,
                (System.currentTimeMillis() - runStart) / 1000.0));

        return new Summary(total, total2m, total10m, total60m, totalVendorUpdated);
    }

    /**
     * one-shot candidate picker with due flags for 10/60 + vendor/image/rarity
     * missing
     */
    private static List<DueRow> pickDueCandidates(Integer overallCap) {
        final int fastMin = 2, slowMin = 60;
        final int activityThreshold = Integer.parseInt(
                Optional.ofNullable(System.getenv("GW2_LISTINGS_THRESHOLD")).orElse("5000"));

        return Jpa.tx(em -> {
            String sql = """
                    WITH candidates AS (
                      SELECT i.id,
                             t.ts_2m, t.ts_10m, t.ts_60m,
                             COALESCE(t.activity_last, 0) AS activity,
                             gp.vendor_value, gp.image, gp.rarity
                      FROM public.items i
                      LEFT JOIN public.gw2_prices_tiers t ON t.item_id = i.id
                      LEFT JOIN public.gw2_prices      gp ON gp.item_id = i.id
                      WHERE i.tradable = true
                    )
                    SELECT id,
                           (ts_10m IS NULL OR ts_10m < now() - make_interval(mins := 10)) AS due10,
                           (ts_60m IS NULL OR ts_60m < now() - make_interval(mins := 60)) AS due60,
                           (vendor_value IS NULL) AS vendorMissing,
                           (image IS NULL) AS imageMissing,
                           (rarity IS NULL) AS rarityMissing
                    FROM candidates
                    WHERE
                      vendor_value IS NULL
                      OR image IS NULL
                      OR rarity IS NULL
                      OR ts_2m IS NULL
                      OR ts_2m < now() - (
                            CASE WHEN activity >= :thr
                                 THEN make_interval(mins := :fast)
                                 ELSE make_interval(mins := :slow)
                            END
                      )
                      OR ts_10m IS NULL OR ts_10m < now() - make_interval(mins := 10)
                      OR ts_60m IS NULL OR ts_60m < now() - make_interval(mins := 60)
                    ORDER BY
                      LEAST(
                        COALESCE(ts_2m,  TIMESTAMP 'epoch'),
                        COALESCE(ts_10m, TIMESTAMP 'epoch'),
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

            List<Object[]> rows = q.getResultList();
            System.out.printf(Locale.ROOT, "Stale: picked=%d (thr=%d, fast=%dm, slow=%dm)%n",
                    rows.size(), activityThreshold, fastMin, slowMin);

            List<DueRow> out = new ArrayList<>(rows.size());
            for (int i = 0; i < rows.size(); i++) {
                Object[] r = rows.get(i);
                out.add(new DueRow(
                        ((Number) r[0]).intValue(),
                        r[1] != null && (Boolean) r[1],
                        r[2] != null && (Boolean) r[2],
                        r[3] != null && (Boolean) r[3],
                        r[4] != null && (Boolean) r[4],
                        r[5] != null && (Boolean) r[5]));
            }
            return out;
        });
    }

    // ---- retry helper
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
    private record DueRow(int id, boolean due10, boolean due60, boolean vendorMissing, boolean imageMissing,
            boolean rarityMissing) {
    }
}

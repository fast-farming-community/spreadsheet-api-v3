package eu.fast.gw2.tools;

import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import eu.fast.gw2.dao.CalculationsDao;
import eu.fast.gw2.dao.Gw2PricesDao;
import eu.fast.gw2.dao.OverlayDao;
import eu.fast.gw2.enums.Tier;

public class OverlayEngine {

    private static final int RUN_LIMIT = Math.max(0, Integer.getInteger("overlay.limit", 50));
    private static final double MIN_COPPER = 0.5;

    // Reuse data across tables to avoid re-querying DB for each table
    private static final java.util.concurrent.ConcurrentHashMap<String, java.util.concurrent.ConcurrentHashMap<Integer, int[]>> PRICE_CACHE_BY_TIER = new java.util.concurrent.ConcurrentHashMap<>();
    private static final java.util.concurrent.ConcurrentHashMap<Integer, String> IMAGE_CACHE = new java.util.concurrent.ConcurrentHashMap<>();
    private static final java.util.concurrent.ConcurrentHashMap<Integer, String> RARITY_CACHE = new java.util.concurrent.ConcurrentHashMap<>();

    // Minimal problem stats (reset at run start; printed at end)
    private static final ProblemStats PROBLEMS = new ProblemStats();

    private static final class ProblemStats {
        int total = 0;
        final java.util.Map<String, Integer> reasonCount = new java.util.HashMap<>();
        final java.util.Map<String, Integer> catKeyCount = new java.util.HashMap<>();
        final java.util.List<String> samples = new java.util.ArrayList<>();
        final int maxSamples = Integer.getInteger("overlay.problemSamples", 200);

        void reset() {
            total = 0;
            reasonCount.clear();
            catKeyCount.clear();
            samples.clear();
        }

        void record(boolean isMain, String tableKey, Long fid, int rowIndex,
                java.util.Map<String, Object> row, int taxesPct, String reason) {
            total++;
            reasonCount.merge(reason, 1, Integer::sum);
            String cat = OverlayHelper.str(row.get(OverlayHelper.COL_CAT));
            String key = OverlayHelper.str(row.get(OverlayHelper.COL_KEY));
            String catKey = ((cat == null ? "" : cat) + "|" + (key == null ? "" : key))
                    .toUpperCase(java.util.Locale.ROOT);
            catKeyCount.merge(catKey, 1, Integer::sum);

            if (samples.size() < maxSamples) {
                int id = OverlayHelper.toInt(row.get(OverlayHelper.COL_ID), -1);
                String name = OverlayHelper.str(row.get(OverlayHelper.COL_NAME));
                Integer b = OverlayHelper.toIntBoxed(row.get(OverlayHelper.COL_TPB));
                Integer s = OverlayHelper.toIntBoxed(row.get(OverlayHelper.COL_TPS));
                Double avg = OverlayHelper.toDouble(row.get(OverlayHelper.COL_AVG), 0.0);
                samples.add(String.format(java.util.Locale.ROOT,
                        "{reason:%s, isMain:%s, table:'%s', fid:%s, row:%d, id:%d, name:'%s', cat:'%s', key:'%s', TPB:%s, TPS:%s, avg:%.3f, taxes:%d}",
                        reason, isMain, tableKey, (fid == null ? "null" : fid.toString()), rowIndex, id,
                        String.valueOf(name),
                        String.valueOf(cat), String.valueOf(key), String.valueOf(b), String.valueOf(s), avg, taxesPct));
            }
        }

        void recordIfZero(boolean isMain, String tableKey, Long fid, int rowIndex,
                java.util.Map<String, Object> row, int taxesPct, String reason) {
            int b = Math.max(0, OverlayHelper.toInt(row.get(OverlayHelper.COL_TPB), 0));
            int s = Math.max(0, OverlayHelper.toInt(row.get(OverlayHelper.COL_TPS), 0));
            if (b == 0 && s == 0)
                record(isMain, tableKey, fid, rowIndex, row, taxesPct, reason);
        }

        void printSummary() {
            System.out.println("=== Overlay Problems (minimal) ===");
            System.out.println("Total problem rows: " + total);
            System.out.println("-- by reason --");
            for (var e : reasonCount.entrySet()) {
                System.out.println("  " + e.getKey() + ": " + e.getValue());
            }
            if (!catKeyCount.isEmpty()) {
                System.out.println("-- top (Category|Key) --");
                java.util.List<java.util.Map.Entry<String, Integer>> list = new java.util.ArrayList<>(
                        catKeyCount.entrySet());
                list.sort((a, b) -> Integer.compare(b.getValue(), a.getValue()));
                int lim = Math.min(15, list.size());
                for (int i = 0; i < lim; i++) {
                    var e = list.get(i);
                    System.out.println("  " + e.getKey() + " -> " + e.getValue());
                }
            }
            if (!samples.isEmpty()) {
                System.out.println("-- sample rows (" + samples.size() + ") --");
                for (String s : samples)
                    System.out.println("  " + s);
            }
            System.out.println("=== End ===");
        }
    }

    // ---------------- public API ----------------

    public static void recomputeMain(String tableKey, Tier tier, boolean persist) {
        long startMs = System.currentTimeMillis();
        System.out.printf(java.util.Locale.ROOT, "Overlay: recomputeMain name='%s' tier=%s%n", tableKey,
                tier.columnKey());

        String rowsJson = OverlayDBAccess.getMainRowsJson(tableKey);
        if (rowsJson == null || rowsJson.isBlank()) {
            System.out.println("  -> no rows found, skipping.");
            return;
        }

        List<Map<String, Object>> rows = OverlayJson.parseRows(rowsJson);
        String tableCategory = OverlayHelper.dominantCategory(rows);
        CalculationsDao.Config tableConfig = CalculationsDao.find(tableCategory, tableKey);

        Set<String> referencedDetailKeys = collectDetailKeys(rows);
        OverlayCache.preloadDetailRows(referencedDetailKeys);

        Set<Integer> neededItemIds = collectAllNeededItemIds(rows, referencedDetailKeys);
        System.out.printf(java.util.Locale.ROOT, "  -> rows=%d, ids=%d%n", rows.size(), neededItemIds.size());

        Map<Integer, int[]> priceByItemId = getOrFillPriceCache(neededItemIds, tier);
        Map<Integer, String> imageUrlByItemId = getOrFillImageCache(neededItemIds);
        Map<Integer, String> rarityByItemId = getOrFillRarityCache(neededItemIds);

        ComputeContext ctx = new ComputeContext(
                true, tier, tableKey, null, tableConfig,
                priceByItemId, imageUrlByItemId, rarityByItemId);

        for (int rowIndex = 0; rowIndex < rows.size(); rowIndex++) {
            computeRow(rows.get(rowIndex), ctx, rowIndex);
        }

        if (tableConfig != null) {
            OverlayHelper.applyAggregation(rows, tableConfig.operation());
        }

        if (persist) {
            OverlayDBAccess.updateMain(tableKey, OverlayJson.toJson(rows));
        }

        long durMs = System.currentTimeMillis() - startMs;
        long sumBuy = 0, sumSell = 0;
        for (var r : rows) {
            Integer b = OverlayHelper.toIntBoxed(r.get(OverlayHelper.COL_TPB));
            Integer s = OverlayHelper.toIntBoxed(r.get(OverlayHelper.COL_TPS));
            if (b != null)
                sumBuy += b;
            if (s != null)
                sumSell += s;
        }
        System.out.printf(java.util.Locale.ROOT,
                "  -> recomputeMain done: sumBuy=%d sumSell=%d persisted=%s (%.1fs)%n",
                sumBuy, sumSell, persist ? "yes" : "no", durMs / 1000.0);
    }

    /** Recompute one detail table identified by (detailFeatureId, tableKey). */
    public static void recompute(long detailFeatureId, String tableKey, Tier tier, boolean persist) {
        long startMs = System.currentTimeMillis();
        System.out.printf(java.util.Locale.ROOT, "Overlay: recompute detail fid=%d key='%s' tier=%s%n",
                detailFeatureId, tableKey, tier.columnKey());

        String rowsJson = OverlayDBAccess.getDetailRowsJson(detailFeatureId, tableKey);
        if (rowsJson == null || rowsJson.isBlank()) {
            System.out.println("  -> no rows found, skipping.");
            return;
        }

        List<Map<String, Object>> rows = OverlayJson.parseRows(rowsJson);
        String tableCategory = OverlayHelper.dominantCategory(rows);
        CalculationsDao.Config tableConfig = CalculationsDao.find(tableCategory, tableKey);

        Set<String> referencedDetailKeys = collectDetailKeys(rows);
        OverlayCache.preloadDetailRows(referencedDetailKeys);

        Set<Integer> neededItemIds = collectAllNeededItemIds(rows, referencedDetailKeys);
        System.out.printf(java.util.Locale.ROOT, "  -> rows=%d, ids=%d%n", rows.size(), neededItemIds.size());

        Map<Integer, int[]> priceByItemId = getOrFillPriceCache(neededItemIds, tier);
        Map<Integer, String> imageUrlByItemId = getOrFillImageCache(neededItemIds);
        Map<Integer, String> rarityByItemId = getOrFillRarityCache(neededItemIds);

        ComputeContext ctx = new ComputeContext(
                false, tier, tableKey, detailFeatureId, tableConfig,
                priceByItemId, imageUrlByItemId, rarityByItemId);

        for (int rowIndex = 0; rowIndex < rows.size(); rowIndex++) {
            computeRow(rows.get(rowIndex), ctx, rowIndex);
        }

        if (tableConfig != null) {
            OverlayHelper.applyAggregation(rows, tableConfig.operation());
        }

        if (persist) {
            OverlayDBAccess.updateDetail(detailFeatureId, tableKey, OverlayJson.toJson(rows));
        }

        long durMs = System.currentTimeMillis() - startMs;
        System.out.printf(java.util.Locale.ROOT, "  -> recompute detail done: persisted=%s (%.1fs)%n",
                persist ? "yes" : "no", durMs / 1000.0);
    }

    // ---------- private cores (json-returning wrappers) ----------

    private static List<Map<String, Object>> recomputeCoreDetail(long detailFeatureId, String tableKey, Tier tier) {
        String rowsJson = OverlayDBAccess.getDetailRowsJson(detailFeatureId, tableKey);
        if (rowsJson == null || rowsJson.isBlank())
            return List.of();

        List<Map<String, Object>> rows = OverlayJson.parseRows(rowsJson);
        String tableCategory = OverlayHelper.dominantCategory(rows);
        CalculationsDao.Config tableConfig = CalculationsDao.find(tableCategory, tableKey);

        Set<String> referencedDetailKeys = collectDetailKeys(rows);
        OverlayCache.preloadDetailRows(referencedDetailKeys);

        Set<Integer> neededItemIds = collectAllNeededItemIds(rows, referencedDetailKeys);

        Map<Integer, int[]> priceByItemId = getOrFillPriceCache(neededItemIds, tier);
        Map<Integer, String> imageUrlByItemId = getOrFillImageCache(neededItemIds);
        Map<Integer, String> rarityByItemId = getOrFillRarityCache(neededItemIds);

        ComputeContext ctx = new ComputeContext(
                false, tier, tableKey, detailFeatureId, tableConfig,
                priceByItemId, imageUrlByItemId, rarityByItemId);

        for (int rowIndex = 0; rowIndex < rows.size(); rowIndex++) {
            computeRow(rows.get(rowIndex), ctx, rowIndex);
        }

        if (tableConfig != null) {
            OverlayHelper.applyAggregation(rows, tableConfig.operation());
        }

        return rows;
    }

    private static List<Map<String, Object>> recomputeCoreMain(String tableKey, Tier tier) {
        String rowsJson = OverlayDBAccess.getMainRowsJson(tableKey);
        if (rowsJson == null || rowsJson.isBlank())
            return List.of();

        List<Map<String, Object>> rows = OverlayJson.parseRows(rowsJson);
        String tableCategory = OverlayHelper.dominantCategory(rows);
        CalculationsDao.Config tableConfig = CalculationsDao.find(tableCategory, tableKey);

        Set<String> referencedDetailKeys = collectDetailKeys(rows);
        OverlayCache.preloadDetailRows(referencedDetailKeys);

        Set<Integer> neededItemIds = collectAllNeededItemIds(rows, referencedDetailKeys);

        Map<Integer, int[]> priceByItemId = getOrFillPriceCache(neededItemIds, tier);
        Map<Integer, String> imageUrlByItemId = getOrFillImageCache(neededItemIds);
        Map<Integer, String> rarityByItemId = getOrFillRarityCache(neededItemIds);

        ComputeContext ctx = new ComputeContext(
                true, tier, tableKey, null, tableConfig,
                priceByItemId, imageUrlByItemId, rarityByItemId);

        for (int rowIndex = 0; rowIndex < rows.size(); rowIndex++) {
            computeRow(rows.get(rowIndex), ctx, rowIndex);
        }

        if (tableConfig != null) {
            OverlayHelper.applyAggregation(rows, tableConfig.operation());
        }

        return rows;
    }

    // -------- public helpers (unchanged signatures) --------

    public static String recomputeDetailJson(long fid, String key, Tier tier) {
        var rows = recomputeCoreDetail(fid, key, tier);
        return OverlayJson.toJson(rows);
    }

    public static String recomputeMainJson(String key, Tier tier) {
        var rows = recomputeCoreMain(key, tier);
        return OverlayJson.toJson(rows);
    }

    public static void recomputeMainPersist(String key, Tier tier) {
        var rows = recomputeCoreMain(key, tier);
        OverlayDBAccess.updateMain(key, OverlayJson.toJson(rows));
    }

    /** Convenience overload using the common tier set. */
    public static void recomputeAndPersistAllOverlays(int sleepMs) {
        Tier[] tiers = { Tier.T5M, Tier.T15M, Tier.T60M };
        recomputeAndPersistAllOverlays(tiers, sleepMs);
    }

    /** Warm tier caches once across all main tables to avoid per-table DB hits. */
    private static void warmCachesForTier(List<String> mainTargets, Tier tier) {
        final long t0 = System.currentTimeMillis();

        Set<String> allDetailKeys = new HashSet<>();
        Set<Integer> allIds = new HashSet<>();

        // Pass 1: scan mains to collect referenced detail keys + direct item ids
        for (String name : mainTargets) {
            String rowsJson = OverlayDBAccess.getMainRowsJson(name);
            if (rowsJson == null || rowsJson.isBlank())
                continue;

            List<Map<String, Object>> rows = OverlayJson.parseRows(rowsJson);
            allDetailKeys.addAll(collectDetailKeys(rows));

            for (var r : rows) {
                String cat = OverlayHelper.str(r.get(OverlayHelper.COL_CAT));
                String key = OverlayHelper.str(r.get(OverlayHelper.COL_KEY));
                if (OverlayHelper.isCompositeRef(cat, key) || OverlayHelper.isInternal(cat))
                    continue;
                int id = OverlayHelper.toInt(r.get(OverlayHelper.COL_ID), -1);
                if (id > 0)
                    allIds.add(id);
            }
        }

        // Pass 2: preload detail rows, then collect ids inside those detail rows
        OverlayCache.preloadDetailRows(allDetailKeys);
        for (String k : allDetailKeys) {
            var drops = OverlayCache.getDetailRowsCached(k);
            allIds.addAll(OverlayHelper.extractIds(drops));
        }

        // Pass 3: single bulk loads into caches
        getOrFillPriceCache(allIds, tier);
        getOrFillImageCache(allIds);
        getOrFillRarityCache(allIds);

        long ms = System.currentTimeMillis() - t0;
        System.out.printf(java.util.Locale.ROOT,
                "Warm tier caches: tier=%s keys=%d ids=%d in %.1fs%n",
                tier.name(), allDetailKeys.size(), allIds.size(), ms / 1000.0);
    }

    public static void recomputeAndPersistAllOverlays(Tier[] tiers, int sleepMs) {
        PROBLEMS.reset();
        final long runStartMs = System.currentTimeMillis();

        OverlayCalc.preloadAllCalcsIfNeeded();

        var detailTargets = OverlayDBAccess.listDetailTargets();
        var mainTargets = OverlayDBAccess.listMainTargets();

        if (RUN_LIMIT > 0 && detailTargets.size() > RUN_LIMIT) {
            detailTargets = detailTargets.subList(0, RUN_LIMIT);
            System.out.printf(java.util.Locale.ROOT, "Limiter: detail targets capped to %d (overlay.limit=%d)%n",
                    detailTargets.size(), RUN_LIMIT);
        }
        if (RUN_LIMIT > 0 && mainTargets.size() > RUN_LIMIT) {
            mainTargets = mainTargets.subList(0, RUN_LIMIT);
            System.out.printf(java.util.Locale.ROOT, "Limiter: main targets capped to %d (overlay.limit=%d)%n",
                    mainTargets.size(), RUN_LIMIT);
        }

        for (Tier t : tiers) {
            long tierStart = System.currentTimeMillis();
            int ok = 0, fail = 0;
            System.out.println("Overlay: recompute detail_tables for tier " + t.name());
            for (Object[] row : detailTargets) {
                long fid = ((Number) row[0]).longValue();
                String key = (String) row[1];
                try {
                    String json = recomputeDetailJson(fid, key, t);
                    if (json != null)
                        OverlayDao.upsertDetail(fid, key, t.label, json);
                    ok++;
                } catch (Exception e) {
                    fail++;
                    System.err.printf("! detail tier=%s fid=%d key='%s' -> %s: %s%n",
                            t.name(), fid, key, e.getClass().getSimpleName(),
                            (e.getMessage() == null ? "<no message>" : e.getMessage()));
                }
                sleepQuiet(sleepMs);
            }
            System.out.printf(java.util.Locale.ROOT, "  detail %s ok=%d fail=%d%n", t.name(), ok, fail);
            long tierDur = System.currentTimeMillis() - tierStart;
            System.out.printf(java.util.Locale.ROOT, "Tier %s (detail) finished in %.1fs%n", t.name(),
                    tierDur / 1000.0);
        }

        for (Tier t : tiers) {
            long tierStart = System.currentTimeMillis();
            int ok = 0, fail = 0;
            System.out.println("Overlay: recompute main tables for tier " + t.name());

            warmCachesForTier(mainTargets, t);

            for (String name : mainTargets) {
                try {
                    String json = recomputeMainJson(name, t);
                    if (json != null)
                        OverlayDao.upsertMain(name, t.label, json);
                    ok++;
                } catch (Exception e) {
                    fail++;
                    System.err.printf("! main tier=%s name='%s' -> %s: %s%n",
                            t.name(), name, e.getClass().getSimpleName(),
                            (e.getMessage() == null ? "<no message>" : e.getMessage()));
                }
                sleepQuiet(sleepMs);
            }
            System.out.printf(java.util.Locale.ROOT, "  main %s ok=%d fail=%d%n", t.name(), ok, fail);
            long tierDur = System.currentTimeMillis() - tierStart;
            System.out.printf(java.util.Locale.ROOT, "Tier %s (main) finished in %.1fs%n", t.name(), tierDur / 1000.0);
        }
        final long runDurMs = System.currentTimeMillis() - runStartMs;
        System.out.printf(java.util.Locale.ROOT, "Overlay: full run finished in %.1fs%n", runDurMs / 1000.0);

        PROBLEMS.printSummary();
    }

    private static void sleepQuiet(int ms) {
        if (ms <= 0)
            return;
        try {
            Thread.sleep(ms);
        } catch (InterruptedException ignored) {
            Thread.currentThread().interrupt();
        }
    }

    // ---------------- shared compute core ----------------

    private static final class ComputeContext {
        final boolean isMain;
        final Tier tier;
        final String tableKey;
        final Long detailFeatureIdOrNull;
        final CalculationsDao.Config tableConfig;
        final Map<Integer, int[]> priceByItemId;
        final Map<Integer, String> imageUrlByItemId;
        final Map<Integer, String> rarityByItemId;

        ComputeContext(
                boolean isMain,
                Tier tier,
                String tableKey,
                Long detailFeatureIdOrNull,
                CalculationsDao.Config tableConfig,
                Map<Integer, int[]> priceByItemId,
                Map<Integer, String> imageUrlByItemId,
                Map<Integer, String> rarityByItemId) {
            this.isMain = isMain;
            this.tier = tier;
            this.tableKey = tableKey;
            this.detailFeatureIdOrNull = detailFeatureIdOrNull;
            this.tableConfig = tableConfig;
            this.priceByItemId = priceByItemId;
            this.imageUrlByItemId = imageUrlByItemId;
            this.rarityByItemId = rarityByItemId;
        }
    }

    private static void computeRow(Map<String, Object> row, ComputeContext ctx, int rowIndex) {
        String category = OverlayHelper.str(row.get(OverlayHelper.COL_CAT));
        String compositeKey = OverlayHelper.str(row.get(OverlayHelper.COL_KEY));
        int taxesPct = OverlayCalc.pickTaxesPercent(category, compositeKey, ctx.tableConfig);

        int itemId = OverlayHelper.toInt(row.get(OverlayHelper.COL_ID), -1);

        if (itemId > 0) {
            String imageUrl = ctx.imageUrlByItemId.get(itemId);
            if (imageUrl != null && !imageUrl.isBlank())
                row.put(OverlayHelper.COL_IMAGE, imageUrl);

            String rarity = ctx.rarityByItemId.get(itemId);
            if (rarity != null && !rarity.isBlank())
                row.put(OverlayHelper.COL_RARITY, rarity);
        }

        if (itemId == 1) {
            int amt = (int) Math.floor(OverlayHelper.toDouble(row.get(OverlayHelper.COL_AVG), 0.0));
            if (ctx.isMain) {
                OverlayHelper.writeProfitWithHour(row, amt, amt);
            } else {
                OverlayHelper.writeProfit(row, amt, amt);
            }
            return;
        }

        var eval = OverlayDslEngine.evaluateRowStrict(category, compositeKey, row, ctx.tier, taxesPct,
                ctx.priceByItemId);
        if (eval == null) {
            System.err.printf(java.util.Locale.ROOT,
                    ctx.isMain
                            ? "Overlay STRICT(main): missing formulas for (category='%s', key='%s') in table='%s' row#%d name='%s'%n"
                            : (ctx.detailFeatureIdOrNull == null
                                    ? "Overlay STRICT(detailCore): missing formulas for (category='%s', key='%s') key='%s' row#%d name='%s'%n"
                                    : "Overlay STRICT(detail): missing formulas for (category='%s', key='%s') in table='%s' fid=%d row#%d name='%s'%n"),
                    String.valueOf(category),
                    String.valueOf(compositeKey),
                    ctx.tableKey,
                    ctx.detailFeatureIdOrNull == null ? rowIndex : rowIndex,
                    String.valueOf(row.get(OverlayHelper.COL_NAME)));
            if (ctx.isMain) {
                OverlayHelper.writeProfitWithHour(row, 0, 0);
            } else {
                OverlayHelper.writeProfit(row, 0, 0);
            }
            PROBLEMS.record(ctx.isMain, ctx.tableKey, ctx.detailFeatureIdOrNull, rowIndex, row, taxesPct,
                    "missing_formulas");
            return;
        }

        if (ctx.isMain) {
            // Direct row values
            double buyRaw = eval.buy();
            double sellRaw = eval.sell();

            // Skip rows that are effectively < 1c on both sides
            if (buyRaw < MIN_COPPER && sellRaw < MIN_COPPER) {
                OverlayHelper.writeProfitWithHour(row, 0, 0);
                return; // <-- no PROBLEMS.record* for smalls
            }

            OverlayHelper.writeProfitWithHour(row, eval.buy(), eval.sell());
            PROBLEMS.recordIfZero(ctx.isMain, ctx.tableKey, ctx.detailFeatureIdOrNull, rowIndex, row, taxesPct,
                    "computed_zero");
        } else {
            // Detail rows: apply quantity before cutoff
            double qty = OverlayHelper.toDouble(row.get(OverlayHelper.COL_AVG), 1.0);
            double buyRaw = eval.buy() * qty;
            double sellRaw = eval.sell() * qty;

            if (buyRaw < MIN_COPPER && sellRaw < MIN_COPPER) {
                OverlayHelper.writeProfit(row, 0, 0);
                return; // <-- no problem recorded
            }

            int buyTotal = (int) Math.round(buyRaw);
            int sellTotal = (int) Math.round(sellRaw);
            OverlayHelper.writeProfit(row, buyTotal, sellTotal);
            PROBLEMS.recordIfZero(ctx.isMain, ctx.tableKey, ctx.detailFeatureIdOrNull, rowIndex, row, taxesPct,
                    "computed_zero");
        }

    }

    // ---------------- key/id collectors ----------------

    private static Set<String> collectDetailKeys(List<Map<String, Object>> rows) {
        Set<String> keys = new HashSet<>();
        for (var row : rows) {
            String category = OverlayHelper.str(row.get(OverlayHelper.COL_CAT));
            String key = OverlayHelper.str(row.get(OverlayHelper.COL_KEY));
            if (key != null && !key.isBlank()
                    && (OverlayHelper.isCompositeRef(category, key) || OverlayHelper.isInternal(category))) {
                keys.add(key);
            }
        }
        return keys;
    }

    private static Set<Integer> collectAllNeededItemIds(List<Map<String, Object>> rows,
            Set<String> preloadedDetailKeys) {
        Set<Integer> needed = new HashSet<>(Math.max(16, rows.size() / 2));

        for (var row : rows) {
            String category = OverlayHelper.str(row.get(OverlayHelper.COL_CAT));
            String key = OverlayHelper.str(row.get(OverlayHelper.COL_KEY));
            if (OverlayHelper.isCompositeRef(category, key) || OverlayHelper.isInternal(category))
                continue;
            int itemId = OverlayHelper.toInt(row.get(OverlayHelper.COL_ID), -1);
            if (itemId > 0)
                needed.add(itemId);
        }

        if (preloadedDetailKeys != null) {
            for (String k : preloadedDetailKeys) {
                var drops = OverlayCache.getDetailRowsCached(k);
                needed.addAll(OverlayHelper.extractIds(drops));
            }
        }
        return needed;
    }

    private static Map<Integer, int[]> getOrFillPriceCache(Set<Integer> ids, Tier tier) {
        var cache = PRICE_CACHE_BY_TIER.computeIfAbsent(tier.columnKey(),
                k -> new java.util.concurrent.ConcurrentHashMap<>());
        if (ids != null && !ids.isEmpty()) {
            java.util.ArrayList<Integer> missing = null;
            for (Integer id : ids) {
                if (!cache.containsKey(id)) {
                    if (missing == null)
                        missing = new java.util.ArrayList<>();
                    missing.add(id);
                }
            }
            if (missing != null) {
                // single DB roundtrip for all missing ids of this tier
                cache.putAll(Gw2PricesDao.loadTier(missing, tier.columnKey()));
            }
        }
        return cache; // safe to pass directly to ctx (read-only use)
    }

    private static Map<Integer, String> getOrFillImageCache(Set<Integer> ids) {
        if (ids != null && !ids.isEmpty()) {
            java.util.ArrayList<Integer> missing = null;
            for (Integer id : ids) {
                if (!IMAGE_CACHE.containsKey(id)) {
                    if (missing == null)
                        missing = new java.util.ArrayList<>();
                    missing.add(id);
                }
            }
            if (missing != null) {
                IMAGE_CACHE.putAll(Gw2PricesDao.loadImageUrlsByIds(new java.util.HashSet<>(missing)));
            }
        }
        return IMAGE_CACHE;
    }

    private static Map<Integer, String> getOrFillRarityCache(Set<Integer> ids) {
        if (ids != null && !ids.isEmpty()) {
            java.util.ArrayList<Integer> missing = null;
            for (Integer id : ids) {
                if (!RARITY_CACHE.containsKey(id)) {
                    if (missing == null)
                        missing = new java.util.ArrayList<>();
                    missing.add(id);
                }
            }
            if (missing != null) {
                RARITY_CACHE.putAll(Gw2PricesDao.loadRaritiesByIds(new java.util.HashSet<>(missing)));
            }
        }
        return RARITY_CACHE;
    }

}

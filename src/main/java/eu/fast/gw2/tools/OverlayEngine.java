package eu.fast.gw2.tools;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import eu.fast.gw2.dao.CalculationsDao;
import eu.fast.gw2.dao.OverlayDao;
import eu.fast.gw2.enums.Tier;

public class OverlayEngine {

    private static final int RUN_LIMIT = Math.max(0, Integer.getInteger("overlay.limit", 5000));
    private static final double MIN_COPPER = 0.5;

    // --- writer queue ---
    private static final int TIER_THREADS = Math.max(1, Integer.getInteger("overlay.tierThreads", 3));
    private static final int WRITER_BATCH = Math.max(1, Integer.getInteger("overlay.writerBatch", 512));
    private static final boolean WRITER_USE_BATCH = Boolean
            .parseBoolean(System.getProperty("overlay.writerUseBatch", "true"));

    private static final java.util.concurrent.LinkedBlockingQueue<Upsert> UPSERT_Q = new java.util.concurrent.LinkedBlockingQueue<>(
            4096);

    private static volatile boolean WRITER_STOP = false;
    private static Thread WRITER_THREAD;

    private static final class Upsert {
        final boolean isMain;
        final long fid; // detail only
        final String keyOrName; // key for detail, name for main
        final String tierLabel;
        final String json;

        Upsert(boolean isMain, long fid, String keyOrName, String tierLabel, String json) {
            this.isMain = isMain;
            this.fid = fid;
            this.keyOrName = keyOrName;
            this.tierLabel = tierLabel;
            this.json = json;
        }
    }

    private static void startWriter() {
        WRITER_STOP = false;
        WRITER_THREAD = new Thread(() -> {
            long t0 = System.currentTimeMillis();
            int flushed = 0;
            final ArrayList<Upsert> buf = new ArrayList<>(WRITER_BATCH);

            try {
                while (!WRITER_STOP || !UPSERT_Q.isEmpty()) {
                    // small wait to coalesce
                    Upsert first = UPSERT_Q.poll(250, java.util.concurrent.TimeUnit.MILLISECONDS);
                    if (first == null)
                        continue;

                    buf.add(first);
                    UPSERT_Q.drainTo(buf, WRITER_BATCH - buf.size());

                    // In-batch de-dupe (keep last)
                    LinkedHashMap<String, Upsert> uniq = new LinkedHashMap<>(buf.size() * 2);
                    for (Upsert u : buf) {
                        String k = u.isMain
                                ? ("M|" + u.keyOrName + "|" + u.tierLabel)
                                : ("D|" + u.fid + "|" + u.keyOrName + "|" + u.tierLabel);
                        uniq.put(k, u); // last wins
                    }

                    if (WRITER_USE_BATCH) {
                        // Split to two lists and flush with DB batch
                        List<String> mainNames = new ArrayList<>();
                        List<String> mainTiers = new ArrayList<>();
                        List<String> mainJsons = new ArrayList<>();
                        List<Long> detFids = new ArrayList<>();
                        List<String> detKeys = new ArrayList<>();
                        List<String> detTiers = new ArrayList<>();
                        List<String> detJsons = new ArrayList<>();

                        for (Upsert u : uniq.values()) {
                            if (u.isMain) {
                                mainNames.add(u.keyOrName);
                                mainTiers.add(u.tierLabel);
                                mainJsons.add(u.json);
                            } else {
                                detFids.add(u.fid);
                                detKeys.add(u.keyOrName);
                                detTiers.add(u.tierLabel);
                                detJsons.add(u.json);
                            }
                        }

                        try {
                            if (!mainNames.isEmpty()) {
                                flushed += OverlayDao.upsertMainBatch(mainNames, mainTiers, mainJsons);
                            }
                            if (!detKeys.isEmpty()) {
                                flushed += OverlayDao.upsertDetailBatch(detFids, detKeys, detTiers, detJsons);
                            }
                        } catch (Throwable batchEx) {
                            // Fallback to row-by-row if batch fails
                            System.err.println(
                                    "Writer batch path failed; falling back to per-row: " + batchEx.getMessage());
                            for (Upsert u : uniq.values()) {
                                try {
                                    if (u.isMain) {
                                        OverlayDao.upsertMain(u.keyOrName, u.tierLabel, u.json);
                                    } else {
                                        OverlayDao.upsertDetail(u.fid, u.keyOrName, u.tierLabel, u.json);
                                    }
                                    flushed++;
                                } catch (Exception e) {
                                    System.err.printf("Writer upsert failed (%s/%s): %s%n",
                                            u.isMain ? "main" : "detail", u.keyOrName, e.getMessage());
                                }
                            }
                        }
                    } else {
                        // Legacy per-row path
                        for (Upsert u : uniq.values()) {
                            try {
                                if (u.isMain) {
                                    OverlayDao.upsertMain(u.keyOrName, u.tierLabel, u.json);
                                } else {
                                    OverlayDao.upsertDetail(u.fid, u.keyOrName, u.tierLabel, u.json);
                                }
                                flushed++;
                            } catch (Exception e) {
                                System.err.printf("Writer upsert failed (%s/%s): %s%n",
                                        u.isMain ? "main" : "detail", u.keyOrName, e.getMessage());
                            }
                        }
                    }

                    buf.clear();
                }
            } catch (InterruptedException ie) {
                Thread.currentThread().interrupt();
            } finally {
                long ms = System.currentTimeMillis() - t0;
                System.out.printf(java.util.Locale.ROOT,
                        "Writer flushed %d upserts in %.1fs (batch=%d, useBatch=%s)%n",
                        flushed, ms / 1000.0, WRITER_BATCH, WRITER_USE_BATCH);
            }
        }, "overlay-writer");
        WRITER_THREAD.setDaemon(true);
        WRITER_THREAD.start();
    }

    private static void stopWriter() {
        WRITER_STOP = true;
        if (WRITER_THREAD != null) {
            try {
                WRITER_THREAD.join();
            } catch (InterruptedException ie) {
                Thread.currentThread().interrupt();
            }
        }
    }

    private static void enqueueMain(String name, String tierLabel, String json) {
        UPSERT_Q.offer(new Upsert(true, 0L, name, tierLabel, json));
    }

    private static void enqueueDetail(long fid, String key, String tierLabel, String json) {
        UPSERT_Q.offer(new Upsert(false, fid, key, tierLabel, json));
    }

    // --- profiling ---
    private static final boolean PROFILE = Boolean.parseBoolean(System.getProperty("overlay.profile", "true"));
    private static final long SLOW_TABLE_MS = Long.getLong("overlay.profile.slowTableMs", 0L);

    /** Per-thread (per-tier) profiler; no sync needed. */
    private static final class Prof {
        long tablesDetail, tablesMain;
        long rowsDetail, rowsMain;
        long coinRows, belowCutoff;

        long copyMs, cfgMs, computeMs, evalMs, attachMs, aggMs, jsonMs, enqueueMs, fastComposite, fastItem;

        // scratch per-table
        String tblName;
        int tblRows;
        boolean tblIsMain;
        long tCopy, tCfg, tAgg, tJson, tEnq;
        long tComputeStartNs;
        long evalNsAccum, attachNsAccum;

        void tableBegin(String name, boolean isMain, int rowsCount) {
            tblName = name;
            tblIsMain = isMain;
            tblRows = rowsCount;
            tCopy = tCfg = tAgg = tJson = tEnq = 0L;
            evalNsAccum = attachNsAccum = 0L;
            tComputeStartNs = System.nanoTime();
        }

        void tableEnd() {
            long computeNs = System.nanoTime() - tComputeStartNs;
            long computeMsLocal = computeNs / 1_000_000L;
            long evalMsLocal = evalNsAccum / 1_000_000L;
            long attachMsLocal = attachNsAccum / 1_000_000L;

            computeMs += computeMsLocal;
            evalMs += evalMsLocal;
            attachMs += attachMsLocal;

            if (PROFILE && computeMsLocal >= SLOW_TABLE_MS) {
                System.out.printf(java.util.Locale.ROOT,
                        "Overlay[prof] %s table='%s' rows=%d times: copy=%dms cfg=%dms compute=%dms (eval=%dms attach=%dms) agg=%dms json=%dms enqueue=%dms%n",
                        tblIsMain ? "MAIN" : "DETAIL",
                        tblName, tblRows,
                        tCopy, tCfg, computeMsLocal, evalMsLocal, attachMsLocal, tAgg, tJson, tEnq);
            }
            tblName = null;
        }
    }

    // -------- problem stats --------
    private static final ProblemStats PROBLEMS = new ProblemStats();

    private static final class ProblemStats {
        int total = 0;
        final Map<String, Integer> reasonCount = new HashMap<>();
        final Map<String, Integer> catKeyCount = new HashMap<>();
        final List<String> samples = new ArrayList<>();
        final int maxSamples = Integer.getInteger("overlay.problemSamples", 200);

        synchronized void reset() {
            total = 0;
            reasonCount.clear();
            catKeyCount.clear();
            samples.clear();
        }

        synchronized void record(boolean isMain, String tableKey, Long fid, int rowIndex,
                Map<String, Object> row, int taxesPct, String reason) {
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
                        String.valueOf(name), String.valueOf(cat), String.valueOf(key),
                        String.valueOf(b), String.valueOf(s), avg, taxesPct));
            }
        }

        synchronized void recordIfZero(boolean isMain, String tableKey, Long fid, int rowIndex,
                Map<String, Object> row, int taxesPct, String reason) {
            int b = Math.max(0, OverlayHelper.toInt(row.get(OverlayHelper.COL_TPB), 0));
            int s = Math.max(0, OverlayHelper.toInt(row.get(OverlayHelper.COL_TPS), 0));
            if (b == 0 && s == 0)
                record(isMain, tableKey, fid, rowIndex, row, taxesPct, reason);
        }

        synchronized void printSummary() {
            System.out.println("=== Overlay Problems (minimal) ===");
            System.out.println("Total problem rows: " + total);
            System.out.println("-- by reason --");
            for (var e : reasonCount.entrySet())
                System.out.println("  " + e.getKey() + ": " + e.getValue());
            if (!catKeyCount.isEmpty()) {
                System.out.println("-- top (Category|Key) --");
                List<Map.Entry<String, Integer>> list = new ArrayList<>(catKeyCount.entrySet());
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

    // ---------------- public helpers (keep signatures) ----------------

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

    public static void recomputeAndPersistAllOverlays(int sleepMs) {
        Tier[] tiers = { Tier.T5M, Tier.T15M, Tier.T60M };
        recomputeAndPersistAllOverlays(tiers, sleepMs);
    }

    // ---------------- Full-run flow: preload → compute → upsert ----------------

    public static void recomputeAndPersistAllOverlays(Tier[] tiers, int sleepMs) {
        PROBLEMS.reset();
        final long runStartMs = System.currentTimeMillis();
        startWriter();

        // 1) Preload everything
        OverlayCalc.preloadAll();

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

        // preload detail + main rows
        Set<String> allDetailKeys = new HashSet<>();
        for (Object[] r : detailTargets) {
            String key = (String) r[1];
            if (key != null && !key.isBlank())
                allDetailKeys.add(key);
        }
        OverlayCache.preloadDetailRows(allDetailKeys);
        OverlayCache.preloadMainRows(mainTargets);

        // collect ids and warm caches for *all* tiers (images/rarities once; prices per
        // tier)
        Set<Integer> allIds = OverlayCache.collectAllItemIdsFromPreloaded();
        OverlayCache.getOrFillImageCache(allIds);
        OverlayCache.getOrFillRarityCache(allIds);
        for (Tier t : tiers)
            OverlayCache.getOrFillPriceCache(allIds, t);

        // 2) Run each tier in parallel
        java.util.concurrent.ExecutorService pool = java.util.concurrent.Executors
                .newFixedThreadPool(Math.min(TIER_THREADS, Math.max(1, tiers.length)));

        java.util.List<java.util.concurrent.Future<?>> futures = new java.util.ArrayList<>();
        for (Tier t : tiers) {
            final var dt = detailTargets; // effectively final for lambda
            final var mt = mainTargets;
            futures.add(pool.submit(() -> runTier(t, dt, mt, sleepMs)));
        }

        // wait + shutdown
        for (var f : futures) {
            try {
                f.get();
            } catch (Exception e) {
                System.err.println("Tier task failed: " + e.getMessage());
            }
        }
        pool.shutdown();

        stopWriter();

        final long runDurMs = System.currentTimeMillis() - runStartMs;
        System.out.printf(java.util.Locale.ROOT, "Overlay: full run finished in %.1fs%n", runDurMs / 1000.0);
        PROBLEMS.printSummary();
    }

    // ---------------- legacy per-table cores (kept for API compatibility)
    // ----------------

    private static List<Map<String, Object>> recomputeCoreDetail(long detailFeatureId, String tableKey, Tier tier) {
        String rowsJson = OverlayDBAccess.getDetailRowsJson(detailFeatureId, tableKey);
        if (rowsJson == null || rowsJson.isBlank())
            return List.of();

        List<Map<String, Object>> rows = OverlayJson.parseRows(rowsJson);
        String tableCategory = OverlayHelper.dominantCategory(rows);
        var tableConfig = OverlayCalc.getCalcCfg(tableCategory, tableKey);

        Set<String> referencedDetailKeys = collectDetailKeys(rows);
        OverlayCache.preloadDetailRows(referencedDetailKeys);

        Set<Integer> neededItemIds = collectAllNeededItemIds(rows, referencedDetailKeys);
        var priceByItemId = OverlayCache.getOrFillPriceCache(neededItemIds, tier);
        var imageUrlByItemId = OverlayCache.getOrFillImageCache(neededItemIds);
        var rarityByItemId = OverlayCache.getOrFillRarityCache(neededItemIds);

        ComputeContext ctx = new ComputeContext(false, tier, tableKey, detailFeatureId, tableConfig,
                priceByItemId, imageUrlByItemId, rarityByItemId);

        for (int i = 0; i < rows.size(); i++)
            computeRow(rows.get(i), ctx, i); // legacy overload
        if (tableConfig != null)
            OverlayHelper.applyAggregation(rows, tableConfig.operation());
        return rows;
    }

    private static List<Map<String, Object>> recomputeCoreMain(String tableKey, Tier tier) {
        String rowsJson = OverlayDBAccess.getMainRowsJson(tableKey);
        if (rowsJson == null || rowsJson.isBlank())
            return List.of();

        List<Map<String, Object>> rows = OverlayJson.parseRows(rowsJson);
        String tableCategory = OverlayHelper.dominantCategory(rows);
        var tableConfig = OverlayCalc.getCalcCfg(tableCategory, tableKey);

        Set<String> referencedDetailKeys = collectDetailKeys(rows);
        OverlayCache.preloadDetailRows(referencedDetailKeys);

        Set<Integer> neededItemIds = collectAllNeededItemIds(rows, referencedDetailKeys);
        var priceByItemId = OverlayCache.getOrFillPriceCache(neededItemIds, tier);
        var imageUrlByItemId = OverlayCache.getOrFillImageCache(neededItemIds);
        var rarityByItemId = OverlayCache.getOrFillRarityCache(neededItemIds);

        ComputeContext ctx = new ComputeContext(true, tier, tableKey, null, tableConfig,
                priceByItemId, imageUrlByItemId, rarityByItemId);

        for (int i = 0; i < rows.size(); i++)
            computeRow(rows.get(i), ctx, i); // legacy overload
        if (tableConfig != null)
            OverlayHelper.applyAggregation(rows, tableConfig.operation());
        return rows;
    }

    // ---------------- compute core & helpers ----------------

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
                boolean isMain, Tier tier, String tableKey, Long detailFeatureIdOrNull,
                CalculationsDao.Config tableConfig, Map<Integer, int[]> priceByItemId,
                Map<Integer, String> imageUrlByItemId, Map<Integer, String> rarityByItemId) {
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

    // keep legacy callers working
    private static void computeRow(Map<String, Object> row, ComputeContext ctx, int rowIndex) {
        computeRow(row, ctx, rowIndex, null);
    }

    private static void computeRow(Map<String, Object> row, ComputeContext ctx, int rowIndex, Prof prof) {
        String category = OverlayHelper.str(row.get(OverlayHelper.COL_CAT));
        String compositeKey = OverlayHelper.str(row.get(OverlayHelper.COL_KEY));
        int taxesPct = OverlayCalc.pickTaxesPercent(category, compositeKey, ctx.tableConfig);
        int itemId = OverlayHelper.toInt(row.get(OverlayHelper.COL_ID), -1);

        long t0 = PROFILE ? System.nanoTime() : 0L;
        if (itemId > 0) {
            String imageUrl = ctx.imageUrlByItemId.get(itemId);
            if (imageUrl != null && !imageUrl.isBlank())
                row.put(OverlayHelper.COL_IMAGE, imageUrl);
            String rarity = ctx.rarityByItemId.get(itemId);
            if (rarity != null && !rarity.isBlank())
                row.put(OverlayHelper.COL_RARITY, rarity);
        }
        if (PROFILE && prof != null)
            prof.attachNsAccum += (System.nanoTime() - t0);

        if (itemId == 1) {
            int amt = (int) Math.floor(OverlayHelper.toDouble(row.get(OverlayHelper.COL_AVG), 0.0));
            if (ctx.isMain)
                OverlayHelper.writeProfitWithHour(row, amt, amt);
            else
                OverlayHelper.writeProfit(row, amt, amt);
            if (PROFILE && prof != null)
                prof.coinRows++;
            return;
        }

        // FAST PATH #1: Composite reference (always use global EV cache path)
        if (OverlayHelper.isCompositeRef(category, compositeKey)) {
            int[] ev = OverlayCalc.evForDetail(compositeKey, ctx.priceByItemId, taxesPct, ctx.tier.columnKey());
            int buy = (ev != null && ev.length > 0) ? Math.max(0, ev[0]) : 0;
            int sell = (ev != null && ev.length > 1) ? Math.max(0, ev[1]) : 0;

            if (!ctx.isMain) {
                double qty = OverlayHelper.toDouble(row.get(OverlayHelper.COL_AVG), 1.0);
                buy = (int) Math.round(buy * qty);
                sell = (int) Math.round(sell * qty);
            }

            if (buy < 1 && sell < 1) {
                if (ctx.isMain)
                    OverlayHelper.writeProfitWithHour(row, 0, 0);
                else
                    OverlayHelper.writeProfit(row, 0, 0);
                if (PROFILE && prof != null)
                    prof.belowCutoff++;
                return;
            }

            if (ctx.isMain)
                OverlayHelper.writeProfitWithHour(row, buy, sell);
            else
                OverlayHelper.writeProfit(row, buy, sell);
            if (PROFILE && prof != null)
                prof.fastComposite++;
            return;
        }

        // FAST PATH #2: Plain item row
        boolean looksPlainItem = !OverlayHelper.isInternal(category)
                && (compositeKey == null || compositeKey.isBlank())
                && itemId > 0;

        if (looksPlainItem) {
            int[] ps = ctx.priceByItemId.get(itemId);
            int tpb = (ps == null || ps.length < 1) ? 0 : Math.max(0, ps[0]);
            int tps = (ps == null || ps.length < 2) ? 0 : Math.max(0, ps[1]);
            int sellNet = netSellAfterTax(tps, taxesPct);

            if (tpb == 0 && sellNet == 0) {
                Integer vv = OverlayCache.vendorValueCached(itemId);
                if (vv != null && vv > 0)
                    sellNet = vv;
            }

            int buy = tpb;
            int sell = sellNet;

            if (!ctx.isMain) {
                double qty = OverlayHelper.toDouble(row.get(OverlayHelper.COL_AVG), 1.0);
                buy = (int) Math.round(buy * qty);
                sell = (int) Math.round(sell * qty);
            }

            if (buy < 1 && sell < 1) {
                if (ctx.isMain)
                    OverlayHelper.writeProfitWithHour(row, 0, 0);
                else
                    OverlayHelper.writeProfit(row, 0, 0);
                if (PROFILE && prof != null)
                    prof.belowCutoff++;
                return;
            }

            if (ctx.isMain)
                OverlayHelper.writeProfitWithHour(row, buy, sell);
            else
                OverlayHelper.writeProfit(row, buy, sell);
            if (PROFILE && prof != null)
                prof.fastItem++;
            return;
        }

        // Fallback: DSL
        t0 = PROFILE ? System.nanoTime() : 0L;
        var eval = OverlayDslEngine.evaluateRowStrict(category, compositeKey, row, ctx.tier, taxesPct,
                ctx.priceByItemId);
        if (PROFILE && prof != null)
            prof.evalNsAccum += (System.nanoTime() - t0);

        if (eval == null) {
            System.err.printf(java.util.Locale.ROOT,
                    ctx.isMain
                            ? "Overlay STRICT(main): missing formulas for (category='%s', key='%s') in table='%s' row#%d name='%s'%n"
                            : (ctx.detailFeatureIdOrNull == null
                                    ? "Overlay STRICT(detailCore): missing formulas for (category='%s', key='%s') key='%s' row#%d name='%s'%n"
                                    : "Overlay STRICT(detail): missing formulas for (category='%s', key='%s') in table='%s' fid=%d row#%d name='%s'%n"),
                    String.valueOf(category), String.valueOf(compositeKey), ctx.tableKey,
                    ctx.detailFeatureIdOrNull == null ? rowIndex : rowIndex,
                    String.valueOf(row.get(OverlayHelper.COL_NAME)));
            if (ctx.isMain)
                OverlayHelper.writeProfitWithHour(row, 0, 0);
            else
                OverlayHelper.writeProfit(row, 0, 0);
            PROBLEMS.record(ctx.isMain, ctx.tableKey, ctx.detailFeatureIdOrNull, rowIndex, row, taxesPct,
                    "missing_formulas");
            return;
        }

        if (ctx.isMain) {
            double buyRaw = eval.buy();
            double sellRaw = eval.sell();
            if (buyRaw < MIN_COPPER && sellRaw < MIN_COPPER) {
                OverlayHelper.writeProfitWithHour(row, 0, 0);
                if (PROFILE && prof != null)
                    prof.belowCutoff++;
                return;
            }
            OverlayHelper.writeProfitWithHour(row, eval.buy(), eval.sell());
            PROBLEMS.recordIfZero(ctx.isMain, ctx.tableKey, ctx.detailFeatureIdOrNull, rowIndex, row, taxesPct,
                    "computed_zero");
        } else {
            double qty = OverlayHelper.toDouble(row.get(OverlayHelper.COL_AVG), 1.0);
            double buyRaw = eval.buy() * qty;
            double sellRaw = eval.sell() * qty;
            if (buyRaw < MIN_COPPER && sellRaw < MIN_COPPER) {
                OverlayHelper.writeProfit(row, 0, 0);
                if (PROFILE && prof != null)
                    prof.belowCutoff++;
                return;
            }
            int buyTotal = (int) Math.round(buyRaw);
            int sellTotal = (int) Math.round(sellRaw);
            OverlayHelper.writeProfit(row, buyTotal, sellTotal);
            PROBLEMS.recordIfZero(ctx.isMain, ctx.tableKey, ctx.detailFeatureIdOrNull, rowIndex, row, taxesPct,
                    "computed_zero");
        }
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

    // collectors used by legacy per-table flows
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

    private static List<Map<String, Object>> deepCopyRows(List<Map<String, Object>> base) {
        if (base == null || base.isEmpty())
            return new ArrayList<>();
        List<Map<String, Object>> out = new ArrayList<>(base.size());
        for (Map<String, Object> r : base)
            out.add(new LinkedHashMap<>(r));
        return out;
    }

    private static int netSellAfterTax(int tps, int taxesPct) {
        if (tps <= 0 || taxesPct <= 0)
            return Math.max(0, tps);
        return (int) Math.floor(tps * (100.0 - OverlayHelper.clampPercent(taxesPct)) / 100.0);
    }

    private static final boolean ONLY_REFERENCED_DETAILS = Boolean
            .parseBoolean(System.getProperty("overlay.onlyReferencedDetails", "true"));
    private static final int REF_EXPAND_LEVELS = Math.max(0,
            Integer.getInteger("overlay.referencedDetailExpandLevels", 0));

    private static void runTier(Tier t, List<Object[]> detailTargets, List<String> mainTargets, int sleepMs) {
        final Prof prof = new Prof();

        // -------- PREWARM detail EVs used by MAIN tables (taxes=0 for composite refs)
        Set<String> refKeys = new HashSet<>();
        try {
            long warmStart = System.currentTimeMillis();
            refKeys = collectCompositeKeysReferencedByMains(mainTargets);

            if (!refKeys.isEmpty()) {
                OverlayCache.preloadDetailRows(refKeys);
            }

            Map<Integer, int[]> tierPrices = OverlayCache.getOrFillPriceCache(Collections.emptySet(), t);
            int warmed = 0, skipped = 0;
            for (String k : refKeys) {
                String ck = t.label + "|0|" + k; // same format as OverlayCalc.evForDetail
                if (OverlayCache.getEv(ck) != null) {
                    skipped++;
                    continue;
                }
                List<Map<String, Object>> drops = OverlayCache.getBaseDetailRows(k);
                int[] ev = (drops == null || drops.isEmpty())
                        ? new int[] { 0, 0 }
                        : OverlayHelper.bagEV(drops, tierPrices, 0); // taxes=0 for composite refs
                OverlayCache.putEv(ck, ev);
                warmed++;
            }

            long warmMs = System.currentTimeMillis() - warmStart;
            System.out.printf(java.util.Locale.ROOT,
                    "Warm EV cache: tier=%s detailKeys=%d warmed=%d skipped=%d in %.1fs%n",
                    t.name(), refKeys.size(), warmed, skipped, warmMs / 1000.0);
        } catch (Exception e) {
            System.err.println("Warm EV cache failed for tier " + t.name() + ": " + e.getMessage());
        }

        // -------- Determine which detail tables to actually recompute --------
        Set<String> allowedDetailKeys = null;
        if (ONLY_REFERENCED_DETAILS) {
            allowedDetailKeys = expandCompositeRefs(refKeys, REF_EXPAND_LEVELS);
            allowedDetailKeys.addAll(refKeys);
            System.out.printf(java.util.Locale.ROOT,
                    "Detail filter: tier=%s referenced=%d expandLevels=%d totalAllowed=%d%n",
                    t.name(), refKeys.size(), REF_EXPAND_LEVELS, allowedDetailKeys.size());
        }

        // -------- DETAIL --------
        long tierStart = System.currentTimeMillis();
        int ok = 0, fail = 0, skipped = 0;
        System.out.println("Overlay: recompute detail_tables for tier " + t.name());

        Map<Integer, int[]> priceMap = OverlayCache.getOrFillPriceCache(Collections.emptySet(), t);
        for (Object[] row : detailTargets) {
            long fid = ((Number) row[0]).longValue();
            String key = (String) row[1];

            if (ONLY_REFERENCED_DETAILS
                    && (key == null || allowedDetailKeys != null && !allowedDetailKeys.contains(key))) {
                skipped++;
                continue;
            }

            try {
                long tCopy0 = System.currentTimeMillis();
                List<Map<String, Object>> base = OverlayCache.getBaseDetailRows(key);
                if (base == null)
                    continue;
                List<Map<String, Object>> rows = deepCopyRows(base);
                long tCopy1 = System.currentTimeMillis();
                if (PROFILE)
                    prof.tablesDetail++;
                prof.copyMs += (tCopy1 - tCopy0);
                if (PROFILE)
                    prof.rowsDetail += rows.size();

                long tCfg0 = System.currentTimeMillis();
                String tableCategory = OverlayHelper.dominantCategory(rows);
                var tableConfig = OverlayCalc.getCalcCfg(tableCategory, key);
                long tCfg1 = System.currentTimeMillis();
                if (PROFILE)
                    prof.cfgMs += (tCfg1 - tCfg0);

                ComputeContext ctx = new ComputeContext(false, t, key, fid, tableConfig,
                        priceMap,
                        OverlayCache.getOrFillImageCache(Collections.emptySet()),
                        OverlayCache.getOrFillRarityCache(Collections.emptySet()));

                if (PROFILE)
                    prof.tableBegin(key, false, rows.size());
                for (int i = 0; i < rows.size(); i++)
                    computeRow(rows.get(i), ctx, i, prof);
                if (tableConfig != null) {
                    long tAgg0 = System.currentTimeMillis();
                    OverlayHelper.applyAggregation(rows, tableConfig.operation());
                    long tAgg1 = System.currentTimeMillis();
                    if (PROFILE)
                        prof.aggMs += (tAgg1 - tAgg0);
                }
                if (PROFILE)
                    prof.tableEnd();

                long tJson0 = System.currentTimeMillis();
                String json = OverlayJson.toJson(rows);
                long tJson1 = System.currentTimeMillis();
                if (PROFILE)
                    prof.jsonMs += (tJson1 - tJson0);

                long tEnq0 = System.currentTimeMillis();
                enqueueDetail(fid, key, t.label, json);
                long tEnq1 = System.currentTimeMillis();
                if (PROFILE)
                    prof.enqueueMs += (tEnq1 - tEnq0);

                ok++;
            } catch (Exception e) {
                fail++;
                System.err.printf("! detail tier=%s fid=%d key='%s' -> %s: %s%n",
                        t.name(), fid, key, e.getClass().getSimpleName(),
                        (e.getMessage() == null ? "<no message>" : e.getMessage()));
            }
            sleepQuiet(sleepMs);
        }
        if (ONLY_REFERENCED_DETAILS) {
            System.out.printf(java.util.Locale.ROOT, "  detail %s ok=%d skipped=%d fail=%d%n", t.name(), ok, skipped,
                    fail);
        } else {
            System.out.printf(java.util.Locale.ROOT, "  detail %s ok=%d fail=%d%n", t.name(), ok, fail);
        }
        long tierDur = System.currentTimeMillis() - tierStart;
        System.out.printf(java.util.Locale.ROOT, "Tier %s (detail) finished in %.1fs%n", t.name(), tierDur / 1000.0);

        // -------- MAIN --------
        tierStart = System.currentTimeMillis();
        ok = 0;
        fail = 0;
        System.out.println("Overlay: recompute main tables for tier " + t.name());

        priceMap = OverlayCache.getOrFillPriceCache(Collections.emptySet(), t);
        for (String name : mainTargets) {
            try {
                long tCopy0 = System.currentTimeMillis();
                List<Map<String, Object>> base = OverlayCache.getBaseMainRows(name);
                if (base == null)
                    continue;
                List<Map<String, Object>> rows = deepCopyRows(base);
                long tCopy1 = System.currentTimeMillis();
                if (PROFILE)
                    prof.tablesMain++;
                prof.copyMs += (tCopy1 - tCopy0);
                if (PROFILE)
                    prof.rowsMain += rows.size();

                long tCfg0 = System.currentTimeMillis();
                String tableCategory = OverlayHelper.dominantCategory(rows);
                var tableConfig = OverlayCalc.getCalcCfg(tableCategory, name);
                long tCfg1 = System.currentTimeMillis();
                if (PROFILE)
                    prof.cfgMs += (tCfg1 - tCfg0);

                ComputeContext ctx = new ComputeContext(true, t, name, null, tableConfig,
                        priceMap,
                        OverlayCache.getOrFillImageCache(Collections.emptySet()),
                        OverlayCache.getOrFillRarityCache(Collections.emptySet()));

                if (PROFILE)
                    prof.tableBegin(name, true, rows.size());
                for (int i = 0; i < rows.size(); i++)
                    computeRow(rows.get(i), ctx, i, prof);
                if (tableConfig != null) {
                    long tAgg0 = System.currentTimeMillis();
                    OverlayHelper.applyAggregation(rows, tableConfig.operation());
                    long tAgg1 = System.currentTimeMillis();
                    if (PROFILE)
                        prof.aggMs += (tAgg1 - tAgg0);
                }
                if (PROFILE)
                    prof.tableEnd();

                long tJson0 = System.currentTimeMillis();
                String json = OverlayJson.toJson(rows);
                long tJson1 = System.currentTimeMillis();
                if (PROFILE)
                    prof.jsonMs += (tJson1 - tJson0);

                long tEnq0 = System.currentTimeMillis();
                enqueueMain(name, t.label, json);
                long tEnq1 = System.currentTimeMillis();
                if (PROFILE)
                    prof.enqueueMs += (tEnq1 - tEnq0);

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
        tierDur = System.currentTimeMillis() - tierStart;
        System.out.printf(java.util.Locale.ROOT, "Tier %s (main) finished in %.1fs%n", t.name(), tierDur / 1000.0);

        // -------- Tier summary --------
        if (PROFILE) {
            System.out.printf(java.util.Locale.ROOT,
                    "Overlay[prof] TIER %s summary: tables(detail=%d, main=%d) rows(detail=%d, main=%d) coinRows=%d belowCutoff=%d fast(composite=%d, item=%d); time(ms): copy=%d cfg=%d compute=%d (eval=%d attach=%d) agg=%d json=%d enqueue=%d%n",
                    t.name(), prof.tablesDetail, prof.tablesMain, prof.rowsDetail, prof.rowsMain,
                    prof.coinRows, prof.belowCutoff, prof.fastComposite, prof.fastItem,
                    prof.copyMs, prof.cfgMs, prof.computeMs, prof.evalMs, prof.attachMs, prof.aggMs, prof.jsonMs,
                    prof.enqueueMs);
        }
    }

    // ---- helpers for referenced detail filtering ----

    private static Set<String> collectCompositeKeysReferencedByMains(List<String> mainTargets) {
        Set<String> refKeys = new HashSet<>();
        for (String name : mainTargets) {
            List<Map<String, Object>> base = OverlayCache.getBaseMainRows(name);
            if (base == null)
                continue;
            for (var row : base) {
                String cat = OverlayHelper.str(row.get(OverlayHelper.COL_CAT));
                String key = OverlayHelper.str(row.get(OverlayHelper.COL_KEY));
                if (key != null && !key.isBlank() && OverlayHelper.isCompositeRef(cat, key)) {
                    refKeys.add(key);
                }
            }
        }
        return refKeys;
    }

    private static Set<String> expandCompositeRefs(Set<String> seed, int levels) {
        if (seed == null || seed.isEmpty() || levels <= 0)
            return new HashSet<>();
        Set<String> all = new HashSet<>(seed);
        Set<String> frontier = new HashSet<>(seed);
        for (int depth = 0; depth < levels; depth++) {
            if (frontier.isEmpty())
                break;
            OverlayCache.preloadDetailRows(frontier);
            Set<String> next = new HashSet<>();
            for (String k : frontier) {
                List<Map<String, Object>> rows = OverlayCache.getBaseDetailRows(k);
                if (rows == null)
                    continue;
                for (var r : rows) {
                    String cat = OverlayHelper.str(r.get(OverlayHelper.COL_CAT));
                    String ref = OverlayHelper.str(r.get(OverlayHelper.COL_KEY));
                    if (ref != null && !ref.isBlank() && OverlayHelper.isCompositeRef(cat, ref) && !all.contains(ref)) {
                        next.add(ref);
                    }
                }
            }
            next.removeAll(all);
            all.addAll(next);
            frontier = next;
        }
        return all;
    }
}

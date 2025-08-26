package eu.fast.gw2.tools;

import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import eu.fast.gw2.enums.Tier;

public final class OverlayTierRunner implements Runnable {

    private static final boolean ONLY_REFERENCED_DETAILS = false;
    private static final int REF_EXPAND_LEVELS = 0;

    private final Tier t;
    private final List<Object[]> detailTargets;
    private final List<String> mainTargets;
    private final OverlayUpsertQueue writer;
    private final OverlayProblemLog problems;
    private final boolean profile;

    public OverlayTierRunner(Tier t,
            List<Object[]> detailTargets,
            List<String> mainTargets,
            OverlayUpsertQueue writer,
            OverlayProblemLog problems,
            boolean profile) {
        this.t = t;
        this.detailTargets = detailTargets;
        this.mainTargets = mainTargets;
        this.writer = writer;
        this.problems = problems;
        this.profile = profile;
    }

    @Override
    public void run() {
        final OverlayProfiler prof = new OverlayProfiler(t.name(), profile);

        // -------- PREWARM EVs used by MAIN tables (taxes=0 for composite refs)
        Set<String> refKeys = new HashSet<>();
        try {
            long warmStart = System.currentTimeMillis();
            refKeys = OverlayReferencePlanner.collectCompositeKeysReferencedByMains(mainTargets);

            if (!refKeys.isEmpty())
                OverlayCache.preloadDetailRows(refKeys);

            Map<Integer, int[]> tierPrices = OverlayCache.getOrFillPriceCache(Collections.emptySet(), t);
            int warmed = 0, skipped = 0;
            for (String k : refKeys) {
                String ck = t.label + "|0|" + k; // same as OverlayCalc.evForDetail cache key
                if (OverlayCache.getEv(ck) != null) {
                    skipped++;
                    continue;
                }
                List<Map<String, Object>> drops = OverlayCache.getBaseDetailRows(k);
                int[] ev = (drops == null || drops.isEmpty())
                        ? new int[] { 0, 0 }
                        : OverlayHelper.bagEV(drops, tierPrices, 0);
                OverlayCache.putEv(ck, ev);
                warmed++;
            }
            long warmMs = System.currentTimeMillis() - warmStart;
            System.out.printf(java.util.Locale.ROOT,
                    "Overlay TIER %s WARM: detailKeys=%d warmed=%d skipped=%d (%.1fs)%n",
                    t.name(), refKeys.size(), warmed, skipped, warmMs / 1000.0);
        } catch (Exception e) {
            System.err.println("Overlay TIER " + t.name() + " WARM: failed -> " + e.getMessage());
        }

        // -------- Determine which detail tables to recompute --------
        Set<String> allowedDetailKeys = null;
        if (ONLY_REFERENCED_DETAILS) {
            allowedDetailKeys = OverlayReferencePlanner.expandCompositeRefs(refKeys, REF_EXPAND_LEVELS);
            allowedDetailKeys.addAll(refKeys);
            System.out.printf(java.util.Locale.ROOT,
                    "Overlay TIER %s DETAIL: filter referenced=%d expand=%d allowed=%d%n",
                    t.name(), refKeys.size(), REF_EXPAND_LEVELS, allowedDetailKeys.size());
        }
        final Set<String> allowedDetailKeysCap = allowedDetailKeys;

        // pretty counters
        final int totalDetailPlanned = ONLY_REFERENCED_DETAILS
                ? (int) detailTargets.stream().filter(r -> {
                    String key = (String) r[1];
                    return key != null && allowedDetailKeysCap != null && allowedDetailKeysCap.contains(key);
                }).count()
                : detailTargets.size();
        final int totalMainPlanned = mainTargets.size();

        // -------- DETAIL --------
        long tierStart = System.currentTimeMillis();
        int ok = 0, fail = 0, skipped = 0, detailIndex = 0;
        System.out.println("Overlay TIER " + t.name() + " DETAIL: start");

        Map<Integer, int[]> priceMap = OverlayCache.getOrFillPriceCache(Collections.emptySet(), t);
        for (Object[] row : detailTargets) {
            long fid = ((Number) row[0]).longValue();
            String key = (String) row[1];

            try {
                List<Map<String, Object>> base = OverlayCache.getBaseDetailRows(key);
                if (base == null)
                    continue;

                List<Map<String, Object>> rows = OverlayRowComputer.deepCopyRows(base);
                if (profile)
                    prof.tablesDetail++;
                if (profile)
                    prof.rowsDetail += rows.size();

                String tableCategory = OverlayHelper.dominantCategory(rows);
                var tableConfig = OverlayCalc.getCalcCfg(tableCategory, key);

                var ctx = new OverlayRowComputer.ComputeContext(false, t, key, fid, tableConfig,
                        priceMap,
                        OverlayCache.getOrFillImageCache(Collections.emptySet()),
                        OverlayCache.getOrFillRarityCache(Collections.emptySet()));

                if (profile)
                    prof.tableBegin(key, false, rows.size(), ++detailIndex, Math.max(totalDetailPlanned, 1));
                for (int i = 0; i < rows.size(); i++)
                    OverlayRowComputer.computeRow(rows.get(i), ctx, i, prof, problems);
                if (tableConfig != null)
                    OverlayHelper.applyAggregation(rows, tableConfig.operation());

                writer.enqueueDetail(fid, key, t.label, OverlayJson.toJson(rows));
                ok++;
            } catch (Exception e) {
                fail++;
                System.err.printf("Overlay TIER %s DETAIL: ! fid=%d key='%s' -> %s: %s%n",
                        t.name(), fid, key, e.getClass().getSimpleName(),
                        (e.getMessage() == null ? "<no message>" : e.getMessage()));
            }
        }
        if (ONLY_REFERENCED_DETAILS) {
            System.out.printf(java.util.Locale.ROOT, "Overlay TIER %s DETAIL: ok=%d skipped=%d fail=%d%n",
                    t.name(), ok, skipped, fail);
        } else {
            System.out.printf(java.util.Locale.ROOT, "Overlay TIER %s DETAIL: ok=%d fail=%d%n",
                    t.name(), ok, fail);
        }
        long tierDur = System.currentTimeMillis() - tierStart;
        System.out.printf(java.util.Locale.ROOT, "Overlay TIER %s DETAIL: finished in %.1fs%n",
                t.name(), tierDur / 1000.0);

        // -------- MAIN --------
        tierStart = System.currentTimeMillis();
        ok = fail = 0;
        int mainIndex = 0;
        System.out.println("Overlay TIER " + t.name() + " MAIN: start");

        priceMap = OverlayCache.getOrFillPriceCache(Collections.emptySet(), t);
        for (String name : mainTargets) {
            try {
                List<Map<String, Object>> base = OverlayCache.getBaseMainRows(name);
                if (base == null)
                    continue;

                List<Map<String, Object>> rows = OverlayRowComputer.deepCopyRows(base);
                if (profile)
                    prof.tablesMain++;
                if (profile)
                    prof.rowsMain += rows.size();

                String tableCategory = OverlayHelper.dominantCategory(rows);
                var tableConfig = OverlayCalc.getCalcCfg(tableCategory, name);

                var ctx = new OverlayRowComputer.ComputeContext(true, t, name, null, tableConfig,
                        priceMap,
                        OverlayCache.getOrFillImageCache(Collections.emptySet()),
                        OverlayCache.getOrFillRarityCache(Collections.emptySet()));

                if (profile)
                    prof.tableBegin(name, true, rows.size(), ++mainIndex, Math.max(totalMainPlanned, 1));
                for (int i = 0; i < rows.size(); i++)
                    OverlayRowComputer.computeRow(rows.get(i), ctx, i, prof, problems);
                if (tableConfig != null)
                    OverlayHelper.applyAggregation(rows, tableConfig.operation());

                writer.enqueueMain(name, t.label, OverlayJson.toJson(rows));
                ok++;
            } catch (Exception e) {
                fail++;
                System.err.printf("Overlay TIER %s MAIN: ! name='%s' -> %s: %s%n",
                        t.name(), name, e.getClass().getSimpleName(),
                        (e.getMessage() == null ? "<no message>" : e.getMessage()));
            }
        }
        System.out.printf(java.util.Locale.ROOT, "Overlay TIER %s MAIN: ok=%d fail=%d%n", t.name(), ok, fail);
        tierDur = System.currentTimeMillis() - tierStart;
        System.out.printf(java.util.Locale.ROOT, "Overlay TIER %s MAIN: finished in %.1fs%n",
                t.name(), tierDur / 1000.0);

        prof.printSummary(t.name());
    }
}

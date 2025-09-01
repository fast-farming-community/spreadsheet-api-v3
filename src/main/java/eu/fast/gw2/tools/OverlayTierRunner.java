package eu.fast.gw2.tools;

import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import eu.fast.gw2.enums.Tier;

public final class OverlayTierRunner implements Runnable {

    private static final boolean ONLY_REFERENCED_DETAILS = false;

    private final Tier t;
    private final List<Object[]> detailTargets;
    private final List<String> mainTargets; // entries are "pageId|name"
    private final OverlayUpsertQueue writer;
    private final OverlayProfiler.Run run;
    private final boolean profile;

    public OverlayTierRunner(Tier t,
            List<Object[]> detailTargets,
            List<String> mainTargets,
            OverlayUpsertQueue writer,
            OverlayProfiler.Run run,
            boolean profile) {
        this.t = t;
        this.detailTargets = detailTargets;
        this.mainTargets = mainTargets;
        this.writer = writer;
        this.run = run;
        this.profile = profile;
    }

    @Override
    public void run() {
        final OverlayProfiler.Tier prof = run.newTier(t.name(), profile);

        // -------- PREWARM EVs used by MAIN tables (taxes=0 for composite refs)
        Set<String> refKeys = new HashSet<>();
        try {
            refKeys = OverlayReferencePlanner.collectCompositeKeysReferencedByMains(mainTargets);

            if (!refKeys.isEmpty())
                OverlayCache.preloadDetailRows(refKeys);

            Map<Integer, int[]> tierPrices = OverlayCache.getOrFillPriceCache(Collections.emptySet(), t);
            for (String k : refKeys) {
                String ck = t.label + "|0|SUM|" + k; // cache key shape with default SUM for warm
                if (OverlayCache.getEv(ck) != null) {
                    continue;
                }
                List<Map<String, Object>> drops = OverlayCache.getBaseDetailRows(k);
                int[] ev = (drops == null || drops.isEmpty()) ? new int[] { 0, 0 }
                        : OverlayHelper.bagEV(drops, tierPrices, 0);
                OverlayCache.putEv(ck, ev);
            }
        } catch (Exception e) {
            System.err.println("Overlay " + t.name() + " WARM: failed -> " + e.getMessage());
        }

        // -------- Determine which detail tables to recompute --------
        Set<String> allowedDetailKeys = null;
        final Set<String> allowedDetailKeysCap = allowedDetailKeys;

        final int totalDetailPlanned = ONLY_REFERENCED_DETAILS
                ? (int) detailTargets.stream().filter(r -> {
                    String key = (String) r[1];
                    return key != null && allowedDetailKeysCap != null && allowedDetailKeysCap.contains(key);
                }).count()
                : detailTargets.size();
        final int totalMainPlanned = mainTargets.size();

        // -------- DETAIL --------
        int fail = 0, detailIndex = 0;

        Map<Integer, int[]> priceMap = OverlayCache.getOrFillPriceCache(Collections.emptySet(), t);
        for (Object[] row : detailTargets) {
            long fid = ((Number) row[0]).longValue();
            String key = (String) row[1];

            try {
                List<Map<String, Object>> base = OverlayCache.getBaseDetailRows(key);
                if (base == null)
                    continue;

                List<Map<String, Object>> rows = OverlayRowComputer.deepCopyRows(base);
                if (profile) {
                    prof.tablesDetail++;
                    prof.rowsDetail += rows.size();
                }

                // Category for this detail table = detail_features.name
                String tableCategory = OverlayDBAccess.detailFeatureNameById(fid);
                var tableConfig = OverlayCalc.getCalcCfg(tableCategory, key);

                var ctx = new OverlayRowComputer.ComputeContext(false, t, key, fid, tableConfig,
                        priceMap,
                        OverlayCache.getOrFillImageCache(Collections.emptySet()),
                        OverlayCache.getOrFillRarityCache(Collections.emptySet()));

                if (profile)
                    prof.tableBegin(key, false, rows.size(), ++detailIndex, Math.max(totalDetailPlanned, 1));
                for (int i = 0; i < rows.size(); i++)
                    OverlayRowComputer.computeRow(rows.get(i), ctx, i, prof, run);

                // Keep detail TOTAL default as SUM (external manual overrides still allowed).
                OverlayHelper.applyAggregation(rows, "SUM");

                writer.enqueueDetail(fid, key, t.label, OverlayJson.toJson(rows));
            } catch (Exception e) {
                fail++;
                System.err.printf("Overlay %s DETAIL: ! fid=%d key='%s' -> %s: %s%n",
                        t.name(), fid, key, e.getClass().getSimpleName(),
                        (e.getMessage() == null ? "<no message>" : e.getMessage()));
            }
        }

        // contribute fail count to run-wide summary
        if (fail > 0)
            run.addFails(fail);

        // -------- MAIN --------
        fail = 0;
        int mainIndex = 0;

        priceMap = OverlayCache.getOrFillPriceCache(Collections.emptySet(), t);
        for (String compositeKey : mainTargets) {
            try {
                List<Map<String, Object>> base = OverlayCache.getBaseMainRows(compositeKey);
                if (base == null)
                    continue;

                List<Map<String, Object>> rows = OverlayRowComputer.deepCopyRows(base);
                if (profile) {
                    prof.tablesMain++;
                    prof.rowsMain += rows.size();
                }

                int pageIdForMain = OverlayDBAccess.pageIdFromComposite(compositeKey);
                String pageNameForMain = OverlayDBAccess.pageNameFromComposite(compositeKey);
                String featureNameForMain = OverlayDBAccess.featureNameByPageId(pageIdForMain);
                String aggKey = (featureNameForMain == null ? "" : featureNameForMain)
                        + "/" + (pageNameForMain == null ? "" : pageNameForMain);

                var tableConfig = OverlayCalc.getCalcCfg("INTERNAL", aggKey);

                var ctx = new OverlayRowComputer.ComputeContext(true, t, compositeKey, null, tableConfig,
                        priceMap,
                        OverlayCache.getOrFillImageCache(Collections.emptySet()),
                        OverlayCache.getOrFillRarityCache(Collections.emptySet()));

                if (profile)
                    prof.tableBegin(compositeKey, true, rows.size(), ++mainIndex, Math.max(totalMainPlanned, 1));
                for (int i = 0; i < rows.size(); i++)
                    OverlayRowComputer.computeRow(rows.get(i), ctx, i, prof, run);

                // MAIN (INTERNAL): policy = MAX
                OverlayHelper.applyAggregation(rows, "MAX");

                writer.enqueueMain(compositeKey, t.label, OverlayJson.toJson(rows));
            } catch (Exception e) {
                fail++;
                System.err.printf("Overlay %s MAIN: ! key='%s' -> %s: %s%n",
                        t.name(), compositeKey, e.getClass().getSimpleName(),
                        (e.getMessage() == null ? "<no message>" : e.getMessage()));
            }
        }

        // contribute fail count to run-wide summary
        if (fail > 0)
            run.addFails(fail);

        // finalize tier -> push counters into the run
        prof.finish();
    }
}

// REPLACE ENTIRE FILE: eu.fast.gw2.tools.OverlayTierRunner
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
    private final List<String> mainTargets; // entries are "pageId|name"
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
                String ck = t.label + "|0|" + k; // same cache key shape as evForDetail
                if (OverlayCache.getEv(ck) != null) {
                    skipped++;
                    continue;
                }
                List<Map<String, Object>> drops = OverlayCache.getBaseDetailRows(k);
                int[] ev = (drops == null || drops.isEmpty()) ? new int[] { 0, 0 }
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
                    OverlayRowComputer.computeRow(rows.get(i), ctx, i, prof, problems);

                // Decide + persist operation based on Datasets
                applyAggregationFromDatasets(rows, "detail", key, tableCategory, key);

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
            System.out.printf(java.util.Locale.ROOT, "Overlay TIER %s DETAIL: ok=%d fail=%d%n", t.name(), ok, fail);
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
                    OverlayRowComputer.computeRow(rows.get(i), ctx, i, prof, problems);

                // Decide + persist operation based on Datasets
                applyAggregationFromDatasets(rows, "main", compositeKey, "INTERNAL", aggKey);

                writer.enqueueMain(compositeKey, t.label, OverlayJson.toJson(rows));
                ok++;
            } catch (Exception e) {
                fail++;
                System.err.printf("Overlay TIER %s MAIN: ! key='%s' -> %s: %s%n",
                        t.name(), compositeKey, e.getClass().getSimpleName(),
                        (e.getMessage() == null ? "<no message>" : e.getMessage()));
            }
        }
        System.out.printf(java.util.Locale.ROOT, "Overlay TIER %s MAIN: ok=%d fail=%d%n", t.name(), ok, fail);
        long tierMain = System.currentTimeMillis() - tierStart;
        System.out.printf(java.util.Locale.ROOT, "Overlay TIER %s MAIN: finished in %.1fs%n",
                t.name(), tierMain / 1000.0);

        prof.printSummary(t.name());
    }

    // ========================================================================
    // Aggregation from "Datasets" column (detail + main), with logging AND
    // persisting the chosen op into public.calculations.operation for
    // (calcCategory, calcKey).
    // ========================================================================
    private static void applyAggregationFromDatasets(
            List<Map<String, Object>> rows,
            String kind,
            String tableKey,
            String calcCategory,
            String calcKey) {

        if (rows == null || rows.isEmpty()) {
            System.out.printf(java.util.Locale.ROOT,
                    "Overlay AGG: %s '%s' has no rows -> skip aggregation%n", kind, tableKey);
            return;
        }

        boolean sawStatic = false;
        boolean sawNumeric = false;
        int unknownCount = 0;
        StringBuilder unknownExamples = new StringBuilder();
        int eligible = 0;

        for (int i = 0; i < rows.size(); i++) {
            Map<String, Object> r = rows.get(i);
            if (r == null)
                continue;

            String category = OverlayHelper.str(r.get(OverlayHelper.COL_CAT));
            String key = OverlayHelper.str(r.get(OverlayHelper.COL_KEY));

            if ("NEGATIVE".equals(category) || "UNCHECKED".equals(category))
                continue;

            boolean internal = OverlayHelper.isInternal(category);
            boolean composite = OverlayHelper.isCompositeRef(category, key);
            if (!(internal || composite))
                continue;

            eligible++;

            Object ds = r.get("Datasets");
            if (ds == null) {
                unknownCount += logUnknown(kind, tableKey, i, r, "null", unknownExamples);
                continue;
            }

            if (ds instanceof Number num) {
                double v = num.doubleValue();
                if (v != 0.0)
                    sawNumeric = true;
                continue;
            }

            String s = String.valueOf(ds);
            if ("static".equals(s)) {
                sawStatic = true;
                continue;
            }

            String t = s.trim();
            if (!t.isEmpty() && t.matches("^-?\\d+(\\.\\d+)?$")) {
                try {
                    double v = Double.parseDouble(t);
                    if (v != 0.0)
                        sawNumeric = true;
                    continue;
                } catch (Exception ignored) {
                }
            }

            unknownCount += logUnknown(kind, tableKey, i, r, s, unknownExamples);
        }

        if (eligible == 0) {
            System.err.printf(java.util.Locale.ROOT,
                    "Overlay AGG WARNING: %s '%s' has no eligible rows for Datasets (only INTERNAL/COMPOSITE). Using SUM.%n",
                    kind, tableKey);
            persistAndApply(rows, kind, tableKey, calcCategory, calcKey, "SUM");
            return;
        }

        if (unknownCount > 0) {
            System.err.printf(java.util.Locale.ROOT,
                    "Overlay AGG WARNING: %s '%s' has %d unknown/empty Datasets among eligible rows. Examples:%n%s",
                    kind, tableKey, unknownCount, unknownExamples.toString());
        }

        String op;
        if (sawStatic && sawNumeric) {
            System.err.printf(java.util.Locale.ROOT,
                    "Overlay AGG WARNING: %s '%s' has mixed Datasets values (\"static\" + numbers). Using SUM. Please fix the sheet!%n",
                    kind, tableKey);
            op = "SUM";
        } else if (sawStatic) {
            op = "MAX";
        } else if (sawNumeric) {
            op = "SUM";
        } else {
            System.err.printf(java.util.Locale.ROOT,
                    "Overlay AGG WARNING: %s '%s' has no usable Datasets (need \"static\" or a number). Using SUM. Please fix the sheet!%n",
                    kind, tableKey);
            op = "SUM";
        }

        persistAndApply(rows, kind, tableKey, calcCategory, calcKey, op);
    }

    private static void persistAndApply(List<Map<String, Object>> rows, String kind, String tableKey,
            String calcCategory, String calcKey, String op) {
        try {
            // Persist operation so the DB source of truth is always aligned with Datasets
            OverlayDBAccess.upsertCalculationOperation(calcCategory, calcKey, op);
        } catch (Throwable t) {
            System.err.printf(java.util.Locale.ROOT,
                    "Overlay AGG ERROR: persist op failed for (%s|%s -> %s): %s%n",
                    String.valueOf(calcCategory), String.valueOf(calcKey), op, t.getMessage());
        }

        try {
            // Apply to the computed rows in-memory for this overlay write
            OverlayHelper.applyAggregation(rows, op);
        } catch (Throwable t) {
            System.err.printf(java.util.Locale.ROOT,
                    "Overlay AGG ERROR: %s '%s' applyAggregation('%s') failed: %s%n",
                    kind, tableKey, op, t.getMessage());
        }
    }

    private static int logUnknown(String kind, String tableKey, int rowIndex, Map<String, Object> r, String val,
            StringBuilder sink) {
        int id = OverlayHelper.toInt(r.get(OverlayHelper.COL_ID), -1);
        String name = OverlayHelper.str(r.get(OverlayHelper.COL_NAME));
        sink.append(String.format(java.util.Locale.ROOT,
                "  - %s '%s' row=%d Id=%d Name=\"%s\" Datasets=%s%n",
                kind, tableKey, rowIndex, id, name, String.valueOf(val)));
        return 1;
    }
}

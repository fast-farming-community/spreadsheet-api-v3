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

                // ---- INTERNAL vs COMPOSITE:
                // INTERNAL (in detail) must force MAX; only scan Datasets for COMPOSITE when
                // there is no INTERNAL.
                decideAndPersistOpCompositeOnly(rows, "detail", key, tableCategory, key);

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

                // MAIN (INTERNAL): always MAX, no Datasets scanning
                persistAndApply(rows, "main", compositeKey, "INTERNAL", aggKey, "MAX");

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
    // INTERNAL vs COMPOSITE op decision:
    // - If table has ANY INTERNAL rows -> op = MAX (do NOT scan Datasets).
    // - Else if table has COMPOSITE rows -> decide via Datasets over COMPOSITE rows
    // only.
    // - Else -> op = SUM (quiet default).
    // ========================================================================
    private static void decideAndPersistOpCompositeOnly(
            List<Map<String, Object>> rows,
            String kind,
            String tableKey,
            String calcCategory,
            String calcKey) {

        if (rows == null || rows.isEmpty()) {
            // nothing to do
            return;
        }

        boolean hasInternal = false;
        boolean hasComposite = false;

        for (Map<String, Object> r : rows) {
            if (r == null)
                continue;
            String category = OverlayHelper.str(r.get(OverlayHelper.COL_CAT));
            String key = OverlayHelper.str(r.get(OverlayHelper.COL_KEY));
            if ("NEGATIVE".equalsIgnoreCase(category) || "UNCHECKED".equalsIgnoreCase(category))
                continue;
            if (OverlayHelper.isInternal(category))
                hasInternal = true;
            else if (OverlayHelper.isCompositeRef(category, key))
                hasComposite = true;
        }

        if (hasInternal) {
            // hard rule: INTERNAL => MAX
            persistAndApply(rows, kind, tableKey, calcCategory, calcKey, "MAX");
            return;
        }

        if (hasComposite) {
            applyAggregationFromDatasetsCompositeOnly(rows, kind, tableKey, calcCategory, calcKey);
            return;
        }

        // No internal, no composite -> quiet SUM
        persistAndApply(rows, kind, tableKey, calcCategory, calcKey, "SUM");
    }

    // ========================================================================
    // Datasets decision ONLY over COMPOSITE rows
    // ========================================================================
    private static void applyAggregationFromDatasetsCompositeOnly(
            List<Map<String, Object>> rows,
            String kind,
            String tableKey,
            String calcCategory,
            String calcKey) {

        boolean sawStatic = false;
        boolean sawNumeric = false;
        int unknownCount = 0;
        StringBuilder unknownExamples = new StringBuilder();
        int compositeCount = 0;

        for (int i = 0; i < rows.size(); i++) {
            Map<String, Object> r = rows.get(i);
            if (r == null)
                continue;

            String category = OverlayHelper.str(r.get(OverlayHelper.COL_CAT));
            String key = OverlayHelper.str(r.get(OverlayHelper.COL_KEY));
            if ("NEGATIVE".equalsIgnoreCase(category) || "UNCHECKED".equalsIgnoreCase(category))
                continue;

            boolean composite = OverlayHelper.isCompositeRef(category, key);
            if (!composite)
                continue; // INTERNAL ignored entirely here

            compositeCount++;

            Object ds = r.get("Datasets");
            if (ds == null) {
                unknownCount += logUnknown(kind, tableKey, i, r, "null", unknownExamples);
                continue;
            }

            if (ds instanceof Number num) {
                if (num.doubleValue() != 0.0)
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
                    if (Double.parseDouble(t) != 0.0)
                        sawNumeric = true;
                    continue;
                } catch (Exception ignored) {
                }
            }

            unknownCount += logUnknown(kind, tableKey, i, r, s, unknownExamples);
        }

        if (compositeCount == 0) {
            persistAndApply(rows, kind, tableKey, calcCategory, calcKey, "SUM");
            return;
        }

        if (unknownCount > 0) {
            System.err.printf(java.util.Locale.ROOT,
                    "Overlay AGG WARNING: %s '%s' has %d unknown/empty Datasets among COMPOSITE rows. Examples:%n%s",
                    kind, tableKey, unknownCount, unknownExamples.toString());
        }

        String op;
        if (sawStatic && sawNumeric) {
            System.err.printf(java.util.Locale.ROOT,
                    "Overlay AGG WARNING: %s '%s' has mixed Datasets on COMPOSITE rows (\"static\" + numbers). Using SUM.%n",
                    kind, tableKey);
            op = "SUM";
        } else if (sawStatic) {
            op = "MAX";
        } else if (sawNumeric) {
            op = "SUM";
        } else {
            System.err.printf(java.util.Locale.ROOT,
                    "Overlay AGG WARNING: %s '%s' has no usable Datasets on COMPOSITE rows (need \"static\" or a number). Using SUM.%n",
                    kind, tableKey);
            op = "SUM";
        }

        persistAndApply(rows, kind, tableKey, calcCategory, calcKey, op);
    }

    private static void persistAndApply(
            List<Map<String, Object>> rows,
            String kind,
            String tableKey,
            String calcCategory,
            String calcKey,
            String op) {

        // Apply to computed rows first (local effect for this overlay write)
        try {
            OverlayHelper.applyAggregation(rows, op);
        } catch (Throwable t) {
            System.err.printf(java.util.Locale.ROOT,
                    "Overlay AGG ERROR: %s '%s' applyAggregation('%s') failed: %s%n",
                    kind, tableKey, op, t.getMessage());
        }

        // Persist chosen op in public.calculations — no info log, warnings only on
        // error
        try {
            if (calcCategory == null || calcCategory.isBlank() || calcKey == null || calcKey.isBlank()) {
                System.err.printf(java.util.Locale.ROOT,
                        "Overlay AGG WARNING: %s '%s' missing (category|key) for calculations upsert; op='%s' not persisted.%n",
                        kind, tableKey, op);
                return;
            }
            OverlayDBAccess.upsertCalculationOperation(calcCategory, calcKey, op);
        } catch (Throwable t) {
            System.err.printf(java.util.Locale.ROOT,
                    "Overlay AGG ERROR: persist op failed for (%s|%s -> %s): %s%n",
                    String.valueOf(calcCategory), String.valueOf(calcKey), op, t.getMessage());
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

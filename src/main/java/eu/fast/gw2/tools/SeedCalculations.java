package eu.fast.gw2.tools;

import java.time.Duration;
import java.time.Instant;
import java.util.*;
import java.util.stream.Collectors;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;

/**
 * Seeds/repairs public.calculations:
 * - Ensures formulas_json for all DB-backed pairs (mode only).
 * - Derives operation from JSON Datasets (static->MAX, number->SUM,
 * mixed->MAX).
 * - Applies defaults when missing: INTERNAL/* -> MAX, other -> SUM.
 * - Upserts orphans from JSON.
 * - FINAL BACKFILL for any row left without operation (LEAF excluded).
 */
public class SeedCalculations {

    private static final String COL_CAT = "Category";
    private static final String COL_KEY = "Key";
    private static final String COL_DATASETS = "Datasets";

    private static final String NOTES_PREFIX_DSL = "auto-seed:strict-dsl";
    private static final String NOTES_PREFIX_OP = "auto-seed:op-from-datasets";

    private static final ObjectMapper M = new ObjectMapper();

    private static final int DEFAULT_TAXES = 0;
    private static final int CHUNK_SIZE_UPSERT = 500;

    public static void run() throws Exception {
        Log.section("SeedCalculations: start");
        Instant t0 = Instant.now();

        // 1) Load JSON blobs
        List<Object[]> detailTables = Jpa.tx(em -> em.createNativeQuery("""
                        SELECT id, key, rows FROM public.detail_tables ORDER BY id DESC
                """).getResultList());

        List<Object[]> mainTables = Jpa.tx(em -> em.createNativeQuery("""
                        SELECT id, rows FROM public.tables ORDER BY id DESC
                """).getResultList());

        Log.info("Loaded JSON sources: detail_tables=%d, tables=%d (%.3fs)",
                detailTables.size(), mainTables.size(), secSince(t0));

        // 2) Scan JSON -> pairs & derived ops
        Instant t1 = Instant.now();
        final JsonScanResult scan = scanJsonForPairsAndOps(detailTables, mainTables);
        Log.info("JSON discovery: pairs=%d, withDerivedOps=%d (%.3fs)",
                scan.allJsonPairs.size(), scan.derivedOps.size(), secSince(t1));

        // 3) DB pairs
        Instant t2 = Instant.now();
        Set<Pair> dbPairs = loadDbPairs();
        Log.info("DB pairs: %d (%.3fs)", dbPairs.size(), secSince(t2));

        // 4) Existing calc rows
        Instant t3 = Instant.now();
        Map<Pair, Existing> existingCalc = Jpa.tx(em -> {
            List<Object[]> rows = em.createNativeQuery("""
                            SELECT category, key, (formulas_json IS NOT NULL) AS has_fjson
                              FROM public.calculations
                    """).getResultList();
            Map<Pair, Existing> map = new LinkedHashMap<>(rows.size() * 2);
            for (Object[] r : rows) {
                String c = (String) r[0];
                String k = (String) r[1];
                boolean f = toBool(r[2]);
                map.put(new Pair(n(c), n(k)), new Existing(f));
            }
            return map;
        });
        Log.info("Loaded existing calculations: %d (%.3fs)", existingCalc.size(), secSince(t3));

        // 5) Seed formulas_json (mode only) for all DB pairs
        Instant t4 = Instant.now();
        List<RowFormulas> needInsertF = new ArrayList<>();
        List<RowFormulas> needUpdateF = new ArrayList<>();
        ensureLeafFormulas(existingCalc, needInsertF, needUpdateF);
        for (Pair p : dbPairs) {
            final String mode = eqi(p.category(), "INTERNAL") ? "INTERNAL" : "COMPOSITE";
            final String fjson = "{\"mode\":\"" + mode + "\"}";
            Existing ex = existingCalc.get(p.norm());
            if (ex == null) {
                needInsertF.add(new RowFormulas(p.category(), p.key(), fjson, DEFAULT_TAXES,
                        NOTES_PREFIX_DSL + " (mode=" + mode + ")"));
                existingCalc.put(p.norm(), new Existing(true));
            } else if (!ex.hasFormulas) {
                needUpdateF.add(new RowFormulas(p.category(), p.key(), fjson, null,
                        NOTES_PREFIX_DSL + " (mode=" + mode + ")"));
                ex.hasFormulas = true;
            }
        }
        Log.info("Plan formulas -> inserts=%d updates=%d (%.3fs)", needInsertF.size(), needUpdateF.size(),
                secSince(t4));

        Instant t5 = Instant.now();
        int insF = execInsertFormulas(needInsertF);
        int updF = execUpdateFormulas(needUpdateF);
        Log.info("Applied formulas (%.3fs): +F=%d ~F=%d", secSince(t5), insF, updF);

        // 6) Build desired operations
        Map<Pair, String> desiredOps = new LinkedHashMap<>(scan.derivedOps);
        for (Pair p : scan.referencedByJson)
            desiredOps.putIfAbsent(p.norm(), defaultOpFor(p));

        Set<Pair> unrefDb = new LinkedHashSet<>(dbPairs);
        unrefDb.removeAll(scan.referencedByJson);
        for (Pair p : unrefDb)
            desiredOps.putIfAbsent(p.norm(), defaultOpFor(p));

        // Orphans from JSON
        Orphans orphans = findOrphans(dbPairs, scan.allJsonPairs);
        Log.info("Orphans: internal=%d, detail=%d", orphans.internalPairs.size(), orphans.detailPairs.size());

        // 7) Upsert operations/defaults/orphans
        Instant t6 = Instant.now();
        int upOp = execUpsertOperations(desiredOps, orphans);
        Log.info("Upserted operations (%.3fs): %d rows", secSince(t6), upOp);

        // 7.5) Final backfill for ANY remaining rows w/o operation
        Instant t7 = Instant.now();
        int backfilled = execBackfillMissingOps();
        Log.info("Backfilled default operations on existing rows (%.3fs): %d", secSince(t7), backfilled);

        // 8) Reporting
        logReferenceDiffs(dbPairs, scan.allJsonPairs);
        Log.section("SeedCalculations: complete (total %.3fs)", secSince(t0));
    }

    // ===== JSON scan =====
    private static JsonScanResult scanJsonForPairsAndOps(List<Object[]> detailTables, List<Object[]> mainTables) {
        Set<Pair> allPairs = new LinkedHashSet<>();
        Set<Pair> referenced = new LinkedHashSet<>();
        Map<Pair, Flag> flagsByPair = new HashMap<>();

        java.util.function.Consumer<List<Map<String, Object>>> visitRows = rows -> {
            for (var r : rows) {
                String cat = str(r.get(COL_CAT));
                String key = str(r.get(COL_KEY));

                if (isBlank(cat) && isBlank(key)) {
                    allPairs.add(new Pair("", ""));
                    continue;
                }
                if (isBlank(cat) || isBlank(key))
                    continue;

                Pair p = new Pair(cat.trim(), key.trim()).norm();
                allPairs.add(p);
                referenced.add(p);

                Object ds = r.get(COL_DATASETS);
                if (ds == null)
                    continue;

                Flag f = flagsByPair.computeIfAbsent(p, k -> new Flag());
                if (ds instanceof Number)
                    f.numeric = true;
                else {
                    String s = String.valueOf(ds).trim();
                    if ("static".equalsIgnoreCase(s))
                        f.stat = true;
                    else if (s.matches("^-?\\d+(\\.\\d+)?$"))
                        f.numeric = true;
                }
            }
        };

        for (Object[] t : detailTables) {
            String js = (String) t[2];
            if (isBlank(js))
                continue;
            try {
                List<Map<String, Object>> rows = M.readValue(js, new TypeReference<>() {
                });
                visitRows.accept(rows);
            } catch (Exception e) {
                Log.warn("JSON parse failed for detail_tables.id=%s key=%s: %s", t[0], t[1], e.getMessage());
            }
        }
        for (Object[] t : mainTables) {
            String js = (String) t[1];
            if (isBlank(js))
                continue;
            try {
                List<Map<String, Object>> rows = M.readValue(js, new TypeReference<>() {
                });
                visitRows.accept(rows);
            } catch (Exception e) {
                Log.warn("JSON parse failed for tables.id=%s: %s", t[0], e.getMessage());
            }
        }

        Map<Pair, String> derivedOps = new LinkedHashMap<>();
        for (var e : flagsByPair.entrySet()) {
            Pair p = e.getKey();
            Flag f = e.getValue();
            String op;
            if (f.stat && f.numeric) {
                Log.warn("Mixed Datasets for (%s|%s) -> MAX", p.category(), p.key());
                op = "MAX";
            } else if (f.stat)
                op = "MAX";
            else if (f.numeric)
                op = "SUM";
            else
                continue;
            derivedOps.put(p, op);
        }

        return new JsonScanResult(allPairs, referenced, derivedOps);
    }

    private static class Flag {
        boolean stat = false, numeric = false;
    }

    private record JsonScanResult(Set<Pair> allJsonPairs, Set<Pair> referencedByJson, Map<Pair, String> derivedOps) {
    }

    // ===== DB discovery =====
    private static Set<Pair> loadDbPairs() {
        Set<Pair> internal = Jpa.tx(em -> {
            List<?> rows = em.createNativeQuery("""
                            SELECT CONCAT(f.name,'/', p.name) AS k
                              FROM public.tables t
                              JOIN public.pages    p ON p.id = t.page_id
                              JOIN public.features f ON f.id = p.feature_id
                    """).getResultList();
            return rows.stream().map(o -> new Pair("INTERNAL", String.valueOf(o)))
                    .collect(Collectors.toCollection(LinkedHashSet::new));
        });

        Set<Pair> detail = Jpa.tx(em -> {
            List<Object[]> rows = em.createNativeQuery("""
                            SELECT df.name AS category, dt.key
                              FROM public.detail_tables dt
                              JOIN public.detail_features df ON df.id = dt.detail_feature_id
                    """).getResultList();
            return rows.stream().map(r -> new Pair((String) r[0], (String) r[1]))
                    .collect(Collectors.toCollection(LinkedHashSet::new));
        });

        Set<Pair> all = new LinkedHashSet<>(internal.size() + detail.size());
        all.addAll(internal);
        all.addAll(detail);
        return all;
    }

    private static String defaultOpFor(Pair p) {
        return eqi(p.category(), "INTERNAL") ? "MAX" : "SUM";
    }

    private static Orphans findOrphans(Set<Pair> dbPairs, Set<Pair> jsonPairs) {
        java.util.function.Predicate<Pair> isInternal = p -> eqi(p.category(), "INTERNAL") && !isBlank(p.key());
        java.util.function.Predicate<Pair> isDetail = p -> !eqi(p.category(), "INTERNAL") && !isBlank(p.category())
                && !isBlank(p.key());

        Set<Pair> dbDetail = dbPairs.stream().filter(isDetail).map(Pair::norm)
                .collect(Collectors.toCollection(LinkedHashSet::new));
        Set<Pair> jsDetail = jsonPairs.stream().filter(isDetail).map(Pair::norm)
                .collect(Collectors.toCollection(LinkedHashSet::new));

        Set<String> dbInternal = dbPairs.stream().filter(isInternal).map(Pair::key)
                .collect(Collectors.toCollection(LinkedHashSet::new));
        Set<String> jsInternal = jsonPairs.stream().filter(isInternal).map(Pair::key)
                .collect(Collectors.toCollection(LinkedHashSet::new));

        Set<Pair> detailOrphan = new LinkedHashSet<>(jsDetail);
        detailOrphan.removeAll(dbDetail);
        Set<String> internalOrphan = new LinkedHashSet<>(jsInternal);
        internalOrphan.removeAll(dbInternal);

        Set<Pair> internalOrphanPairs = internalOrphan.stream().map(k -> new Pair("INTERNAL", k))
                .collect(Collectors.toCollection(LinkedHashSet::new));

        return new Orphans(internalOrphanPairs, detailOrphan);
    }

    private static class Orphans {
        final Set<Pair> internalPairs, detailPairs;

        Orphans(Set<Pair> internal, Set<Pair> detail) {
            this.internalPairs = internal;
            this.detailPairs = detail;
        }
    }

    // ===== Formulas seeding (TEXT) =====
    private static void ensureLeafFormulas(Map<Pair, Existing> existingCalc,
            List<RowFormulas> needInsertF,
            List<RowFormulas> needUpdateF) {
        final Pair LEAF = new Pair("", "");
        final Existing exLeaf = existingCalc.get(LEAF.norm());
        final String leafFjson = "{\"mode\":\"LEAF\"}";
        if (exLeaf == null) {
            needInsertF.add(new RowFormulas("", "", leafFjson, DEFAULT_TAXES, NOTES_PREFIX_DSL + " (mode=LEAF)"));
            existingCalc.put(LEAF.norm(), new Existing(true));
        } else if (!exLeaf.hasFormulas) {
            needUpdateF.add(new RowFormulas("", "", leafFjson, null, NOTES_PREFIX_DSL + " (mode=LEAF)"));
            exLeaf.hasFormulas = true;
        }
    }

    private static int execInsertFormulas(List<RowFormulas> rows) {
        if (rows.isEmpty())
            return 0;
        int total = 0;
        for (int i = 0; i < rows.size(); i += CHUNK_SIZE_UPSERT) {
            List<RowFormulas> chunk = rows.subList(i, Math.min(rows.size(), i + CHUNK_SIZE_UPSERT));
            total += Jpa.tx(em -> {
                String values = chunk.stream()
                        .map(r -> "(?::text, ?::text, ?::text, ?::int, ?::text, ?::text)")
                        .collect(Collectors.joining(","));
                String sql = """
                        INSERT INTO public.calculations
                              (category, key, operation, taxes, formulas_json, notes)
                        VALUES """ + values + """
                         ON CONFLICT (category, key) DO UPDATE
                             SET formulas_json = EXCLUDED.formulas_json,
                                 taxes         = COALESCE(public.calculations.taxes, EXCLUDED.taxes),
                                 notes         = CASE
                                                   WHEN public.calculations.formulas_json IS NULL THEN EXCLUDED.notes
                                                   ELSE public.calculations.notes
                                                 END
                           WHERE public.calculations.formulas_json IS NULL
                        """;
                var q = em.createNativeQuery(sql);
                int idx = 1;
                for (RowFormulas r : chunk) {
                    q.setParameter(idx++, r.category);
                    q.setParameter(idx++, r.key);
                    q.setParameter(idx++, ""); // leave operation empty here
                    q.setParameter(idx++, r.taxesOrDefault == null ? DEFAULT_TAXES : r.taxesOrDefault);
                    q.setParameter(idx++, r.formulasJson);
                    q.setParameter(idx++, r.notes);
                }
                return q.executeUpdate();
            });
        }
        return total;
    }

    private static int execUpdateFormulas(List<RowFormulas> rows) {
        if (rows.isEmpty())
            return 0;
        int total = 0;
        for (int i = 0; i < rows.size(); i += CHUNK_SIZE_UPSERT) {
            List<RowFormulas> chunk = rows.subList(i, Math.min(rows.size(), i + CHUNK_SIZE_UPSERT));
            total += Jpa.tx(em -> {
                String values = chunk.stream()
                        .map(r -> "(?::text, ?::text, ?::text, ?::text)")
                        .collect(Collectors.joining(","));
                String sql = """
                        WITH src(category, key, fjson, n) AS (VALUES %s)
                        UPDATE public.calculations c
                           SET formulas_json = src.fjson,
                               notes = CASE WHEN c.formulas_json IS NULL THEN src.n ELSE c.notes END
                          FROM src
                         WHERE c.category = src.category
                           AND c.key      = src.key
                           AND c.formulas_json IS NULL
                        """.formatted(values);
                var q = em.createNativeQuery(sql);
                int idx = 1;
                for (RowFormulas r : chunk) {
                    q.setParameter(idx++, r.category);
                    q.setParameter(idx++, r.key);
                    q.setParameter(idx++, r.formulasJson);
                    q.setParameter(idx++, r.notes);
                }
                return q.executeUpdate();
            });
        }
        return total;
    }

    // ===== Operations upsert =====
    private static int execUpsertOperations(Map<Pair, String> desiredOps, Orphans orphans) {
        if (desiredOps.isEmpty() && orphans.detailPairs.isEmpty() && orphans.internalPairs.isEmpty())
            return 0;

        List<RowWithOp> rows = new ArrayList<>();
        for (var e : desiredOps.entrySet()) {
            Pair p = e.getKey();
            String op = e.getValue();
            String mode = eqi(p.category(), "INTERNAL") ? "INTERNAL" : "COMPOSITE";
            rows.add(new RowWithOp(p.category(), p.key(), op, "{\"mode\":\"" + mode + "\"}", DEFAULT_TAXES,
                    NOTES_PREFIX_OP + " (from Datasets/default)"));
        }
        for (Pair p : orphans.internalPairs) {
            rows.add(new RowWithOp(p.category(), p.key(), "MAX", "{\"mode\":\"INTERNAL\"}", DEFAULT_TAXES,
                    NOTES_PREFIX_OP + " (orphan default INTERNAL=MAX)"));
        }
        for (Pair p : orphans.detailPairs) {
            rows.add(new RowWithOp(p.category(), p.key(), "SUM", "{\"mode\":\"COMPOSITE\"}", DEFAULT_TAXES,
                    NOTES_PREFIX_OP + " (orphan default DETAIL=SUM)"));
        }

        int total = 0;
        for (int i = 0; i < rows.size(); i += CHUNK_SIZE_UPSERT) {
            List<RowWithOp> chunk = rows.subList(i, Math.min(rows.size(), i + CHUNK_SIZE_UPSERT));
            total += Jpa.tx(em -> {
                String values = chunk.stream()
                        .map(r -> "(?::text, ?::text, ?::text, ?::int, ?::text, ?::text)")
                        .collect(Collectors.joining(","));
                String sql = """
                        INSERT INTO public.calculations
                              (category, key, operation, taxes, formulas_json, notes)
                        VALUES """ + values + """
                         ON CONFLICT (category, key) DO UPDATE
                             SET operation     = EXCLUDED.operation,
                                 formulas_json = COALESCE(public.calculations.formulas_json, EXCLUDED.formulas_json),
                                 taxes         = COALESCE(public.calculations.taxes, EXCLUDED.taxes),
                                 notes         = public.calculations.notes
                        """;
                var q = em.createNativeQuery(sql);
                int idx = 1;
                for (RowWithOp r : chunk) {
                    q.setParameter(idx++, r.category);
                    q.setParameter(idx++, r.key);
                    q.setParameter(idx++, r.operation);
                    q.setParameter(idx++, r.taxesOrDefault == null ? DEFAULT_TAXES : r.taxesOrDefault);
                    q.setParameter(idx++, r.formulasJson);
                    q.setParameter(idx++, r.notes);
                }
                return q.executeUpdate();
            });
        }
        return total;
    }

    /**
     * FINAL backfill for existing rows that still lack operation (LEAF excluded).
     */
    private static int execBackfillMissingOps() {
        return Jpa.tx(em -> {
            String sql = """
                        UPDATE public.calculations c
                           SET operation     = CASE WHEN lower(c.category)='internal' THEN 'MAX' ELSE 'SUM' END,
                               formulas_json = COALESCE(
                                                   c.formulas_json,
                                                   CASE WHEN lower(c.category)='internal'
                                                        THEN '{"mode":"INTERNAL"}'
                                                        ELSE '{"mode":"COMPOSITE"}'
                                                   END
                                               ),
                               taxes         = COALESCE(c.taxes, :defaultTaxes)
                         WHERE (c.operation IS NULL OR c.operation = '')
                           AND NOT (COALESCE(c.category,'')='' AND COALESCE(c.key,'')='');
                    """;
            var q = em.createNativeQuery(sql);
            q.setParameter("defaultTaxes", DEFAULT_TAXES);
            return q.executeUpdate();
        });
    }

    // ===== Reporting =====
    private static void logReferenceDiffs(Set<Pair> dbPairs, Set<Pair> jsonPairs) {
        java.util.function.Predicate<Pair> isInternal = p -> eqi(p.category(), "INTERNAL") && !isBlank(p.key());
        java.util.function.Predicate<Pair> isDetail = p -> !eqi(p.category(), "INTERNAL") && !isBlank(p.category())
                && !isBlank(p.key());

        Set<String> dbInternal = dbPairs.stream().filter(isInternal).map(Pair::key)
                .collect(Collectors.toCollection(LinkedHashSet::new));
        Set<String> jsInternal = jsonPairs.stream().filter(isInternal).map(Pair::key)
                .collect(Collectors.toCollection(LinkedHashSet::new));

        Set<Pair> dbDetail = dbPairs.stream().filter(isDetail).map(Pair::norm)
                .collect(Collectors.toCollection(LinkedHashSet::new));
        Set<Pair> jsDetail = jsonPairs.stream().filter(isDetail).map(Pair::norm)
                .collect(Collectors.toCollection(LinkedHashSet::new));

        Set<String> internalUnref = new LinkedHashSet<>(dbInternal);
        internalUnref.removeAll(jsInternal);
        Set<Pair> detailUnref = new LinkedHashSet<>(dbDetail);
        detailUnref.removeAll(jsDetail);

        Set<String> internalOrphan = new LinkedHashSet<>(jsInternal);
        internalOrphan.removeAll(dbInternal);
        Set<Pair> detailOrphan = new LinkedHashSet<>(jsDetail);
        detailOrphan.removeAll(dbDetail);

        Log.section("Reference check (DB vs JSON)");
        Log.info("INTERNAL: unreferenced=%d, orphan=%d", internalUnref.size(), internalOrphan.size());
        for (String k : internalUnref)
            Log.info("UNREFERENCED INTERNAL: %s", k);
        for (String k : internalOrphan)
            Log.info("ORPHAN INTERNAL     : %s", k);

        Log.info("DETAIL  : unreferenced=%d, orphan=%d", detailUnref.size(), detailOrphan.size());
        for (Pair p : detailUnref)
            Log.info("UNREFERENCED DETAIL: %s :: %s", p.category(), p.key());
        for (Pair p : detailOrphan)
            Log.info("ORPHAN DETAIL     : %s :: %s", p.category(), p.key());
    }

    // ===== Models & utils =====
    private record RowFormulas(String category, String key, String formulasJson, Integer taxesOrDefault, String notes) {
    }

    private record RowWithOp(String category, String key, String operation, String formulasJson, Integer taxesOrDefault,
            String notes) {
    }

    private static class Existing {
        boolean hasFormulas;

        Existing(boolean f) {
            this.hasFormulas = f;
        }
    }

    public record Pair(String category, String key) {
        Pair norm() {
            return new Pair(n(category), n(key));
        }

        @Override
        public boolean equals(Object o) {
            if (this == o)
                return true;
            if (!(o instanceof Pair p))
                return false;
            return eqi(category, p.category) && eqi(key, p.key);
        }

        @Override
        public int hashCode() {
            return Objects.hash(n(category), n(key));
        }

        public String category() {
            return category;
        }

        public String key() {
            return key;
        }
    }

    private static boolean eqi(String a, String b) {
        return a == null ? b == null : a.equalsIgnoreCase(b);
    }

    private static String n(String s) {
        return (s == null ? "" : s.toLowerCase(java.util.Locale.ROOT));
    }

    private static String str(Object o) {
        return (o == null) ? null : String.valueOf(o);
    }

    private static boolean isBlank(String s) {
        return s == null || s.isBlank();
    }

    private static boolean toBool(Object o) {
        if (o instanceof Boolean b)
            return b;
        if (o instanceof Number n)
            return n.intValue() != 0;
        if (o instanceof String s)
            return Boolean.parseBoolean(s);
        return false;
    }

    private static double secSince(Instant start) {
        return Duration.between(start, Instant.now()).toMillis() / 1000.0;
    }

    private static class Log {
        static void section(String fmt, Object... args) {
            System.out.println("\n=== " + String.format(java.util.Locale.ROOT, fmt, args) + " ===");
        }

        static void info(String fmt, Object... args) {
            System.out.println("[INFO] " + String.format(java.util.Locale.ROOT, fmt, args));
        }

        static void warn(String fmt, Object... args) {
            System.err.println("[WARN] " + String.format(java.util.Locale.ROOT, fmt, args));
        }
    }
}

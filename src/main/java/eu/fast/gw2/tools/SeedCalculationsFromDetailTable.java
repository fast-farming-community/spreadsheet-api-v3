package eu.fast.gw2.tools;

import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.stream.Collectors;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;

/**
 * Seed calculations using the DB structure as the source of truth.
 *
 * Flow per run:
 * 1) Read JSON blobs from public.detail_tables.rows and public.tables.rows
 * (REPORTING ONLY).
 * 2) Parse JSON into distinct (Category, Key) pairs (Category+Key non-blank),
 * plus LEAF ("","") exception.
 * 3) Read (Category, Key) pairs from DB structure (SOURCE OF TRUTH for
 * seeding):
 * - INTERNAL: ("INTERNAL", CONCAT(features.name,'/',pages.name))
 * - DETAIL : (detail_features.name, detail_tables.key)
 * 4) Load existing public.calculations once (to know what’s missing).
 * 5) Seed ALL DB pairs (regardless of JSON), and ensure LEAF exists:
 * - formulas_json default by mode: INTERNAL / COMPOSITE (only when NULL)
 * - operation default: INTERNAL => MAX, else SUM (only when NULL)
 * 6) Report DB vs JSON references (UNREFERENCED vs ORPHAN). No structure
 * writes.
 *
 * Notes:
 * - Category is assumed to NEVER be empty, except the LEAF case ("","") which
 * is accepted from JSON only.
 * - JSON is used only for reporting; DB structure drives seeding.
 * - No pruning of calculations here.
 */
public class SeedCalculationsFromDetailTable {

    private static final String COL_CAT = "Category";
    private static final String COL_KEY = "Key";

    private static final String NOTES_PREFIX_DSL = "auto-seed:strict-dsl";
    private static final String NOTES_PREFIX_OPS = "auto-seed:table-op";

    private static final ObjectMapper M = new ObjectMapper();

    private static final int DEFAULT_TAXES = 0;
    private static final int CHUNK_SIZE_UPSERT = 500;

    public static void run() throws Exception {
        Log.section("SeedCalculations: start");
        Instant t0 = Instant.now();

        // ---------- 1) Load JSON blobs (reporting only) ----------
        List<Object[]> detailTables = Jpa.tx(em -> em.createNativeQuery("""
                    SELECT id, key, rows
                      FROM public.detail_tables
                     ORDER BY id DESC
                """).getResultList());

        List<Object[]> mainTables = Jpa.tx(em -> em.createNativeQuery("""
                    SELECT id, rows
                      FROM public.tables
                     ORDER BY id DESC
                """).getResultList());

        Log.info("Loaded JSON sources: detail_tables=%d, tables=%d (%.3fs)",
                detailTables.size(), mainTables.size(), secSince(t0));

        // ---------- 2) Discover (Category, Key) from JSON (reporting only) ----------
        Instant t1 = Instant.now();
        Set<Pair> jsonPairs = discoverPairsFromJsonOnly(detailTables, mainTables);
        Log.info("Discovered from JSON (Category,Key) pairs: %d (%.3fs)", jsonPairs.size(), secSince(t1));

        // ---------- 3) Load DB pairs (source of truth for seeding) ----------
        Instant t2 = Instant.now();
        Set<Pair> dbPairs = loadDbPairs();
        Log.info("Discovered from DB (Category,Key) pairs: %d (%.3fs)", dbPairs.size(), secSince(t2));

        // ---------- 4) Load existing calculations once ----------
        Instant t3 = Instant.now();
        Map<Pair, Existing> existingCalc = Jpa.tx(em -> {
            List<Object[]> rows = em.createNativeQuery("""
                        SELECT category, key,
                               (formulas_json IS NOT NULL) AS has_fjson,
                               (operation     IS NOT NULL) AS has_op
                          FROM public.calculations
                    """).getResultList();
            Map<Pair, Existing> map = new LinkedHashMap<>(rows.size() * 2);
            for (Object[] r : rows) {
                String c = (String) r[0];
                String k = (String) r[1];
                boolean f = toBool(r[2]);
                boolean o = toBool(r[3]);
                map.put(new Pair(n(c), n(k)), new Existing(f, o));
            }
            return map;
        });
        Log.info("Loaded existing calculations: %d (%.3fs)", existingCalc.size(), secSince(t3));

        // ---------- 5) Plan calc writes (batched) USING ALL DB PAIRS + ensure LEAF
        // ----------
        Instant t4 = Instant.now();

        List<RowFormulas> needInsertF = new ArrayList<>();
        List<RowFormulas> needUpdateF = new ArrayList<>();
        List<RowOp> needInsertOp = new ArrayList<>();
        List<RowOp> needUpdateOp = new ArrayList<>();

        // Seed the LEAF pair ("","") needed by overlays regardless of DB/JSON presence
        final Pair LEAF = new Pair("", "");
        final Existing exLeaf = existingCalc.get(LEAF.norm());
        final String leafFjson = "{\"mode\":\"LEAF\"}";
        final String leafOp = "SUM"; // default for non-INTERNAL
        if (exLeaf == null) {
            needInsertF.add(new RowFormulas("", "", leafOp, DEFAULT_TAXES, leafFjson,
                    NOTES_PREFIX_DSL + " (mode=LEAF)"));
            existingCalc.put(LEAF.norm(), new Existing(true, false));
            needInsertOp.add(new RowOp("", "", leafOp, DEFAULT_TAXES,
                    NOTES_PREFIX_OPS + " (op=" + leafOp + ")"));
        } else {
            if (!exLeaf.hasFormulas) {
                needUpdateF.add(new RowFormulas("", "", null, null, leafFjson,
                        NOTES_PREFIX_DSL + " (mode=LEAF)"));
                exLeaf.hasFormulas = true;
            }
            if (!exLeaf.hasOperation) {
                needUpdateOp.add(new RowOp("", "", leafOp, null,
                        NOTES_PREFIX_OPS + " (op=" + leafOp + ")"));
                exLeaf.hasOperation = true;
            }
        }

        // Final seed set = ALL DB pairs (preserve original case)
        List<Pair> pairsToSeed = new ArrayList<>(dbPairs);
        Log.info("DB pairs to seed (all, regardless of JSON): %d", pairsToSeed.size());

        for (Pair p : pairsToSeed) {
            final String mode = eqi(p.category(), "INTERNAL") ? "INTERNAL" : "COMPOSITE";
            final String fjson = "{\"mode\":\"" + mode + "\"}";
            final String defaultRowOp = eqi(p.category(), "INTERNAL") ? "MAX" : "SUM";

            Existing ex = existingCalc.get(p.norm());
            if (ex == null) {
                // brand new
                needInsertF.add(new RowFormulas(p.category(), p.key(), defaultRowOp, DEFAULT_TAXES, fjson,
                        NOTES_PREFIX_DSL + " (mode=" + mode + ")"));
                existingCalc.put(p.norm(), new Existing(true, false)); // mark formulas present for subsequent logic

                needInsertOp.add(new RowOp(p.category(), p.key(), defaultRowOp, DEFAULT_TAXES,
                        NOTES_PREFIX_OPS + " (op=" + defaultRowOp + ")"));
            } else {
                // Already present in calculations: fill missing fields only
                if (!ex.hasFormulas) {
                    needUpdateF.add(new RowFormulas(p.category(), p.key(), null, null, fjson,
                            NOTES_PREFIX_DSL + " (mode=" + mode + ")"));
                    ex.hasFormulas = true;
                }
                if (!ex.hasOperation) {
                    needUpdateOp.add(new RowOp(p.category(), p.key(), defaultRowOp, null,
                            NOTES_PREFIX_OPS + " (op=" + defaultRowOp + ")"));
                    ex.hasOperation = true;
                }
            }
        }

        Log.info("Plan -> F:+%d ~%d, O:+%d ~%d (%.3fs)",
                needInsertF.size(), needUpdateF.size(), needInsertOp.size(), needUpdateOp.size(), secSince(t4));

        // ---------- 6) Apply batched writes ----------
        Instant t5 = Instant.now();
        int insF = execInsertFormulas(needInsertF);
        int updF = execUpdateFormulas(needUpdateF);
        int insO = execInsertOps(needInsertOp);
        int updO = execUpdateOps(needUpdateOp);
        Log.info("Applied calculations (%.3fs): +F=%d ~F=%d, +O=%d ~O=%d",
                secSince(t5), insF, updF, insO, updO);

        // ---------- 7) Report DB vs JSON references (no writes) ----------
        Instant t6 = Instant.now();
        logReferenceDiffs(dbPairs, jsonPairs);
        Log.info("Reference reporting done (%.3fs)", secSince(t6));

        Log.section("SeedCalculations: complete (total %.3fs)", secSince(t0));
    }

    // ---------- JSON discovery (reporting only; accept LEAF as only exception)
    // ----------
    private static Set<Pair> discoverPairsFromJsonOnly(
            List<Object[]> detailTables, List<Object[]> mainTables) {
        Set<Pair> jsonPairs = new LinkedHashSet<>();
        int jsonErrors = 0;

        java.util.function.Consumer<List<Map<String, Object>>> collect = rows -> {
            for (var r : rows) {
                String cat = str(r.get(COL_CAT));
                String key = str(r.get(COL_KEY));

                if (isBlank(cat) && isBlank(key)) {
                    // LEAF exception: accept only when BOTH are blank
                    jsonPairs.add(new Pair("", ""));
                } else if (!isBlank(cat) && !isBlank(key)) {
                    // Normal case: require BOTH to be non-blank
                    jsonPairs.add(new Pair(cat.trim(), key.trim()));
                }
                // else: ignore partials
            }
        };

        // detail_tables.rows
        for (Object[] t : detailTables) {
            long id = ((Number) t[0]).longValue();
            String k = (String) t[1];
            String js = (String) t[2];
            if (isBlank(js))
                continue;
            try {
                List<Map<String, Object>> rows = M.readValue(js, new TypeReference<>() {
                });
                collect.accept(rows);
            } catch (Exception e) {
                if (++jsonErrors <= 5)
                    Log.warn("JSON parse failed for detail_tables.id=%d key=%s: %s", id, k, e.getMessage());
            }
        }

        // tables.rows
        for (Object[] t : mainTables) {
            long id = ((Number) t[0]).longValue();
            String js = (String) t[1];
            if (isBlank(js))
                continue;
            try {
                List<Map<String, Object>> rows = M.readValue(js, new TypeReference<>() {
                });
                collect.accept(rows);
            } catch (Exception e) {
                if (++jsonErrors <= 5)
                    Log.warn("JSON parse failed for tables.id=%d: %s", id, e.getMessage());
            }
        }

        return jsonPairs;
    }

    // ---------- DB discovery (source of truth for seeding) ----------
    private static Set<Pair> loadDbPairs() {
        // INTERNAL: feature/page from main structure
        Set<Pair> internal = Jpa.tx(em -> {
            List<?> rows = em.createNativeQuery("""
                        SELECT CONCAT(f.name,'/', p.name) AS k
                          FROM public.tables t
                          JOIN public.pages    p ON p.id = t.page_id
                          JOIN public.features f ON f.id = p.feature_id
                    """).getResultList();
            return rows.stream()
                    .map(o -> new Pair("INTERNAL", String.valueOf(o)))
                    .collect(Collectors.toCollection(LinkedHashSet::new));
        });

        // DETAIL: (category,key) from detail_* structure
        Set<Pair> detail = Jpa.tx(em -> {
            List<Object[]> rows = em.createNativeQuery("""
                        SELECT df.name AS category, dt.key
                          FROM public.detail_tables dt
                          JOIN public.detail_features df ON df.id = dt.detail_feature_id
                    """).getResultList();
            return rows.stream()
                    .map(r -> new Pair((String) r[0], (String) r[1]))
                    .collect(Collectors.toCollection(LinkedHashSet::new));
        });

        // UNION
        Set<Pair> all = new LinkedHashSet<>(internal.size() + detail.size());
        all.addAll(internal);
        all.addAll(detail);
        return all;
    }

    // ---------- Reporting: DB vs JSON references ----------
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

        // In DB but NOT referenced by JSON
        Set<String> internalUnref = new LinkedHashSet<>(dbInternal);
        internalUnref.removeAll(jsInternal);
        Set<Pair> detailUnref = new LinkedHashSet<>(dbDetail);
        detailUnref.removeAll(jsDetail);

        // Referenced by JSON but NOT present in DB
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

    // ---------- calculations writers (batched) ----------
    private static int execInsertFormulas(List<RowFormulas> rows) {
        if (rows.isEmpty())
            return 0;
        int total = 0;
        for (int i = 0; i < rows.size(); i += CHUNK_SIZE_UPSERT) {
            List<RowFormulas> chunk = rows.subList(i, Math.min(rows.size(), i + CHUNK_SIZE_UPSERT));
            total += Jpa.tx(em -> {
                String values = chunk.stream()
                        .map(r -> "(?::text, ?::text, ?::text, ?::int, NULL, ?::jsonb, ?::text)")
                        .collect(Collectors.joining(","));
                String sql = """
                        INSERT INTO public.calculations
                              (category, key, operation, taxes, source_table_key, formulas_json, notes)
                        VALUES """ + values + """
                         ON CONFLICT (category, key) DO UPDATE
                             SET formulas_json = EXCLUDED.formulas_json,
                                 notes = CASE
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
                    q.setParameter(idx++, r.operationOrDefault); // MAX for INTERNAL, SUM otherwise
                    q.setParameter(idx++, r.taxesOrDefault);
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
                        .map(r -> "(?::text, ?::text, ?::jsonb, ?::text)")
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

    private static int execInsertOps(List<RowOp> rows) {
        if (rows.isEmpty())
            return 0;
        int total = 0;
        for (int i = 0; i < rows.size(); i += CHUNK_SIZE_UPSERT) {
            List<RowOp> chunk = rows.subList(i, Math.min(rows.size(), i + CHUNK_SIZE_UPSERT));
            total += Jpa.tx(em -> {
                String values = chunk.stream()
                        .map(r -> "(?::text, ?::text, ?::text, ?::int, NULL, NULL, ?::text)")
                        .collect(Collectors.joining(","));
                String sql = """
                        INSERT INTO public.calculations
                              (category, key, operation, taxes, source_table_key, formulas_json, notes)
                        VALUES """ + values + """
                         ON CONFLICT (category, key) DO UPDATE
                             SET operation = EXCLUDED.operation,
                                 notes = CASE
                                           WHEN public.calculations.operation IS NULL THEN EXCLUDED.notes
                                           ELSE public.calculations.notes
                                         END
                           WHERE public.calculations.operation IS NULL
                        """;
                var q = em.createNativeQuery(sql);
                int idx = 1;
                for (RowOp r : chunk) {
                    q.setParameter(idx++, r.category);
                    q.setParameter(idx++, r.key);
                    q.setParameter(idx++, r.operation);
                    q.setParameter(idx++, r.taxesOrDefault);
                    q.setParameter(idx++, r.notes);
                }
                return q.executeUpdate();
            });
        }
        return total;
    }

    private static int execUpdateOps(List<RowOp> rows) {
        if (rows.isEmpty())
            return 0;
        int total = 0;
        for (int i = 0; i < rows.size(); i += CHUNK_SIZE_UPSERT) {
            List<RowOp> chunk = rows.subList(i, Math.min(rows.size(), i + CHUNK_SIZE_UPSERT));
            total += Jpa.tx(em -> {
                String values = chunk.stream()
                        .map(r -> "(?::text, ?::text, ?::text, ?::text)")
                        .collect(Collectors.joining(","));
                String sql = """
                        WITH src(category, key, op, n) AS (VALUES %s)
                        UPDATE public.calculations c
                           SET operation = src.op,
                               notes = CASE WHEN c.operation IS NULL THEN src.n ELSE c.notes END
                          FROM src
                         WHERE c.category = src.category
                           AND c.key      = src.key
                           AND c.operation IS NULL
                        """.formatted(values);
                var q = em.createNativeQuery(sql);
                int idx = 1;
                for (RowOp r : chunk) {
                    q.setParameter(idx++, r.category);
                    q.setParameter(idx++, r.key);
                    q.setParameter(idx++, r.operation);
                    q.setParameter(idx++, r.notes);
                }
                return q.executeUpdate();
            });
        }
        return total;
    }

    // ---------- models & utils ----------
    private record RowFormulas(String category, String key, String operationOrDefault, Integer taxesOrDefault,
            String formulasJson, String notes) {
    }

    private record RowOp(String category, String key, String operation, Integer taxesOrDefault, String notes) {
    }

    private static class Existing {
        boolean hasFormulas;
        boolean hasOperation;

        Existing(boolean f, boolean o) {
            this.hasFormulas = f;
            this.hasOperation = o;
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
    }

    private static boolean eqi(String a, String b) {
        return a == null ? b == null : a.equalsIgnoreCase(b);
    }

    private static String n(String s) {
        return (s == null ? "" : s.toLowerCase(Locale.ROOT));
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
            System.out.println("\n=== " + String.format(Locale.ROOT, fmt, args) + " ===");
        }

        static void info(String fmt, Object... args) {
            System.out.println("[INFO] " + String.format(Locale.ROOT, fmt, args));
        }

        static void warn(String fmt, Object... args) {
            System.err.println("[WARN] " + String.format(Locale.ROOT, fmt, args));
        }
    }
}

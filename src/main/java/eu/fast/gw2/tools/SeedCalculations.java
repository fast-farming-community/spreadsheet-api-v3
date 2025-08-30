// REPLACE ENTIRE FILE: eu.fast.gw2.tools.SeedCalculations
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
 * Writes ONLY formulas_json (and taxes/notes). NEVER touches operation.
 * Runtime will derive and persist operation from the "Datasets" column.
 */
public class SeedCalculations {

    private static final String COL_CAT = "Category";
    private static final String COL_KEY = "Key";

    private static final String NOTES_PREFIX_DSL = "auto-seed:strict-dsl";

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
                                   (formulas_json IS NOT NULL) AS has_fjson
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

        // ---------- 5) Plan formulas writes (batched) ----------
        Instant t4 = Instant.now();

        List<RowFormulas> needInsertF = new ArrayList<>();
        List<RowFormulas> needUpdateF = new ArrayList<>();

        // Ensure LEAF pair ("","") exists with mode=LEAF
        final Pair LEAF = new Pair("", "");
        final Existing exLeaf = existingCalc.get(LEAF.norm());
        final String leafFjson = "{\"mode\":\"LEAF\"}";
        if (exLeaf == null) {
            needInsertF.add(new RowFormulas("", "", leafFjson,
                    DEFAULT_TAXES, NOTES_PREFIX_DSL + " (mode=LEAF)"));
            existingCalc.put(LEAF.norm(), new Existing(true));
        } else if (!exLeaf.hasFormulas) {
            needUpdateF.add(new RowFormulas("", "", leafFjson,
                    null, NOTES_PREFIX_DSL + " (mode=LEAF)"));
            exLeaf.hasFormulas = true;
        }

        // Seed ALL DB pairs with INTERNAL/COMPOSITE modes (formulas only)
        for (Pair p : dbPairs) {
            final String mode = eqi(p.category(), "INTERNAL") ? "INTERNAL" : "COMPOSITE";
            final String fjson = "{\"mode\":\"" + mode + "\"}";

            Existing ex = existingCalc.get(p.norm());
            if (ex == null) {
                needInsertF.add(new RowFormulas(p.category(), p.key(), fjson,
                        DEFAULT_TAXES, NOTES_PREFIX_DSL + " (mode=" + mode + ")"));
                existingCalc.put(p.norm(), new Existing(true));
            } else if (!ex.hasFormulas) {
                needUpdateF.add(new RowFormulas(p.category(), p.key(), fjson,
                        null, NOTES_PREFIX_DSL + " (mode=" + mode + ")"));
                ex.hasFormulas = true;
            }
        }

        Log.info("Plan -> F:+%d ~%d (%.3fs)",
                needInsertF.size(), needUpdateF.size(), secSince(t4));

        // ---------- 6) Apply batched writes ----------
        Instant t5 = Instant.now();
        int insF = execInsertFormulas(needInsertF);
        int updF = execUpdateFormulas(needUpdateF);
        Log.info("Applied calculations (%.3fs): +F=%d ~F=%d",
                secSince(t5), insF, updF);

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
                    jsonPairs.add(new Pair("", "")); // LEAF
                } else if (!isBlank(cat) && !isBlank(key)) {
                    jsonPairs.add(new Pair(cat.trim(), key.trim()));
                }
            }
        };

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

        Set<Pair> all = new LinkedHashSet<>(internal.size() + detail.size());
        all.addAll(internal);
        all.addAll(detail);
        return all;
    }

    // ---------- reporting ----------
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

    // ---------- formulas writers (batched) ----------
    private static int execInsertFormulas(List<RowFormulas> rows) {
        if (rows.isEmpty())
            return 0;
        int total = 0;
        for (int i = 0; i < rows.size(); i += CHUNK_SIZE_UPSERT) {
            List<RowFormulas> chunk = rows.subList(i, Math.min(rows.size(), i + CHUNK_SIZE_UPSERT));
            total += Jpa.tx(em -> {
                String values = chunk.stream()
                        .map(r -> "(?::text, ?::text, NULL, ?::int, NULL, ?::jsonb, ?::text)")
                        .collect(Collectors.joining(","));
                String sql = """
                        INSERT INTO public.calculations
                              (category, key, operation, taxes, source_table_key, formulas_json, notes)
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

    // ---------- models & utils ----------
    private record RowFormulas(String category, String key, String formulasJson, Integer taxesOrDefault, String notes) {
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

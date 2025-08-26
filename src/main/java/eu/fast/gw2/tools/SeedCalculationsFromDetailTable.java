package eu.fast.gw2.tools;

import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;
import java.util.LinkedHashMap;
import java.util.Objects;
import java.util.stream.Collectors;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;

/**
 * Strict-DSL seeder + table-level ops (batched):
 * - Scans BOTH public.detail_tables.rows and public.tables.rows.
 * - Discovers distinct (Category, Key) pairs including LEAF (Key="").
 * - Loads existing rows from public.calculations once.
 * - Computes minimal INSERT/UPDATE sets in memory, then applies in batched SQL.
 * - UPSERTS default formulas_json={"mode": "..."} ONLY when formulas_json IS
 * NULL.
 * - Seeds table-level operation per table: INTERNAL => MAX, else SUM (ONLY when
 * operation IS NULL).
 * - PRUNES ONLY rows previously auto-seeded by this tool (notes LIKE our
 * prefixes).
 * - Adds detailed logging + timings.
 */
public class SeedCalculationsFromDetailTable {

    private static final String COL_CAT = "Category";
    private static final String COL_KEY = "Key";

    private static final String NOTES_PREFIX_DSL = "auto-seed:strict-dsl";
    private static final String NOTES_PREFIX_OPS = "auto-seed:table-op";

    private static final ObjectMapper M = new ObjectMapper();

    // satisfy NOT NULL on taxes (engine can still apply defaults)
    private static final int DEFAULT_TAXES = 0;

    // Chunk sizes for batched VALUES statements
    private static final int CHUNK_SIZE_UPSERT = 500; // safe for most PG param limits
    private static final int CHUNK_SIZE_PRUNE = 800;

    public static void run() throws Exception {
        Log.section("SeedCalculations: start");
        Log.info("Mode: gather first, then minimal batched writes");

        // ---------- Phase 1: Read JSON rows from BOTH sources ----------
        Instant t0 = Instant.now();
        List<Object[]> detailTables = Jpa.tx(em -> {
            @SuppressWarnings("unchecked")
            List<Object[]> rows = em.createNativeQuery("""
                        SELECT id, key, rows
                          FROM public.detail_tables
                         ORDER BY id DESC
                    """).getResultList();
            return rows;
        });

        List<Object[]> mainTables = Jpa.tx(em -> {
            @SuppressWarnings("unchecked")
            List<Object[]> rows = em.createNativeQuery("""
                        SELECT id, name, rows
                          FROM public.tables
                         ORDER BY id DESC
                    """).getResultList();
            return rows;
        });

        Log.info("Loaded table metas: detail=%d, main=%d (%.3fs)",
                detailTables.size(), mainTables.size(), secSince(t0));

        // ---------- Phase 2: Discover pairs in memory ----------
        Instant t1 = Instant.now();

        Set<Pair> discoveredDslPairs = new LinkedHashSet<>(); // (category,key)
        Set<Pair> discoveredTablePairs = new LinkedHashSet<>(); // (dominantCategory, tableKey/name)

        int jsonErrors = 0;

        java.util.function.Consumer<List<Map<String, Object>>> collectRowPairs = rows -> {
            for (var r : rows) {
                String cat = str(r.get(COL_CAT));
                String key = str(r.get(COL_KEY));

                if (isBlank(cat) && isBlank(key)) {
                    // LEAF = global rule ("","")
                    discoveredDslPairs.add(new Pair("", ""));
                } else if (!isBlank(key)) {
                    String c = (cat == null ? "" : cat.trim()); // categories lower-case in sheets
                    discoveredDslPairs.add(new Pair(c, key.trim()));
                }
            }
        };

        // parse detail rows
        for (Object[] t : detailTables) {
            long id = ((Number) t[0]).longValue();
            String tableKey = (String) t[1];
            String json = (String) t[2];
            if (isBlank(json))
                continue;

            try {
                List<Map<String, Object>> rows = M.readValue(json, new TypeReference<List<Map<String, Object>>>() {
                });
                collectRowPairs.accept(rows);
                String domCat = OverlayHelper.dominantCategory(rows);
                if (!isBlank(domCat) && !isBlank(tableKey)) {
                    discoveredTablePairs.add(new Pair(domCat.trim(), tableKey.trim()));
                }
            } catch (Exception e) {
                jsonErrors++;
                if (jsonErrors <= 5) {
                    Log.warn("JSON parse failed for detail_tables.id=%d key=%s (skipping): %s", id, tableKey,
                            e.getMessage());
                }
            }
        }

        // parse main rows
        for (Object[] t : mainTables) {
            long id = ((Number) t[0]).longValue();
            String name = (String) t[1];
            String json = (String) t[2];
            if (isBlank(json))
                continue;

            try {
                List<Map<String, Object>> rows = M.readValue(json, new TypeReference<List<Map<String, Object>>>() {
                });
                collectRowPairs.accept(rows);
                String domCat = OverlayHelper.dominantCategory(rows);
                if (!isBlank(domCat) && !isBlank(name)) {
                    discoveredTablePairs.add(new Pair(domCat.trim(), name.trim()));
                }
            } catch (Exception e) {
                jsonErrors++;
                if (jsonErrors <= 5) {
                    Log.warn("JSON parse failed for tables.id=%d name=%s (skipping): %s", id, name, e.getMessage());
                }
            }
        }

        Log.info("Discovered DSL pairs: %d; table-op pairs: %d (%.3fs)",
                discoveredDslPairs.size(), discoveredTablePairs.size(), secSince(t1));

        // ---------- Phase 3: Load existing calculations once ----------
        Instant t2 = Instant.now();

        Map<Pair, Existing> existing = Jpa.tx(em -> {
            @SuppressWarnings("unchecked")
            List<Object[]> rows = em.createNativeQuery("""
                        SELECT category, key,
                               (formulas_json IS NOT NULL) AS has_fjson,
                               (operation IS NOT NULL)    AS has_op
                          FROM public.calculations
                    """).getResultList();

            Map<Pair, Existing> map = new LinkedHashMap<>(rows.size() * 2);
            for (Object[] r : rows) {
                String c = (String) r[0];
                String k = (String) r[1];
                boolean hasF = toBool(r[2]);
                boolean hasO = toBool(r[3]);
                map.put(new Pair(n(c), n(k)), new Existing(hasF, hasO));
            }
            return map;
        });

        Log.info("Loaded existing calculations: %d (%.3fs)", existing.size(), secSince(t2));

        // ---------- Phase 4: Compute minimal write sets ----------
        Instant t3 = Instant.now();

        List<RowFormulas> needInsertF = new ArrayList<>();
        List<RowFormulas> needUpdateF = new ArrayList<>();

        for (Pair p : discoveredDslPairs) {
            final String mode = (p.category().isEmpty() && p.key().isEmpty())
                    ? "LEAF"
                    : (eqi(p.category(), "INTERNAL") ? "INTERNAL" : "COMPOSITE");

            final String fjson = "{\"mode\":\"" + mode + "\"}";
            final String defaultRowOp = "SUM";

            Existing ex = existing.get(p.norm());
            if (ex == null) {
                // no row -> INSERT with formulas_json + default row op
                needInsertF.add(new RowFormulas(p.category(), p.key(), defaultRowOp, DEFAULT_TAXES, fjson,
                        NOTES_PREFIX_DSL + " (mode=" + mode + ")"));
                // after we "simulate" insert, mark existing state to prevent duplicate op
                // inserts later
                existing.put(p.norm(), new Existing(true, false)); // has formulas now (post-insert), op may still be
                                                                   // null
            } else if (!ex.hasFormulas) {
                // row exists but formulas NULL -> UPDATE formulas_json only
                needUpdateF.add(new RowFormulas(p.category(), p.key(), null, null, fjson,
                        NOTES_PREFIX_DSL + " (mode=" + mode + ")"));
                ex.hasFormulas = true;
            }
        }

        List<RowOp> needInsertOp = new ArrayList<>();
        List<RowOp> needUpdateOp = new ArrayList<>();

        for (Pair p : discoveredTablePairs) {
            String op = eqi(p.category(), "INTERNAL") ? "MAX" : "SUM";
            Existing ex = existing.get(p.norm());
            if (ex == null) {
                // no row -> INSERT with operation only (formulas_json NULL)
                needInsertOp.add(
                        new RowOp(p.category(), p.key(), op, DEFAULT_TAXES, NOTES_PREFIX_OPS + " (op=" + op + ")"));
                existing.put(p.norm(), new Existing(false, true));
            } else if (!ex.hasOperation) {
                // row exists but op NULL -> UPDATE op only
                needUpdateOp.add(new RowOp(p.category(), p.key(), op, null, NOTES_PREFIX_OPS + " (op=" + op + ")"));
                ex.hasOperation = true;
            }
        }

        Log.info("Plan -> formulas: insert=%d, update=%d; ops: insert=%d, update=%d (%.3fs)",
                needInsertF.size(), needUpdateF.size(), needInsertOp.size(), needUpdateOp.size(), secSince(t3));

        // ---------- Phase 5: Apply writes in batches ----------
        Instant t4 = Instant.now();
        int insF = execInsertFormulas(needInsertF);
        int updF = execUpdateFormulas(needUpdateF);
        int insO = execInsertOps(needInsertOp);
        int updO = execUpdateOps(needUpdateOp);
        Log.info("Applied writes (%.3fs): inserted formulas=%d, updated formulas=%d, inserted ops=%d, updated ops=%d",
                secSince(t4), insF, updF, insO, updO);

        // ---------- Phase 6: Prune stale (only our prefixes) ----------
        Instant t5 = Instant.now();
        pruneStaleByPrefix(discoveredDslPairs, NOTES_PREFIX_DSL);
        pruneStaleByPrefix(discoveredTablePairs, NOTES_PREFIX_OPS);
        Log.info("Prune done (%.3fs)", secSince(t5));

        Log.section("SeedCalculations: complete (total %.3fs)", secSince(t0));
    }

    // ---------- batched writers ----------

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
                    q.setParameter(idx++, r.operationOrDefault);
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
                // Use VALUES and join on (category,key) to set formulas_json + conditional
                // notes bump
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
                               AND c.key = src.key
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
                               AND c.key = src.key
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

    // ---------- pruning helpers (scoped by notes prefix) ----------

    private static void pruneStaleByPrefix(Set<Pair> discoveredNow, String notesPrefix) {
        // load only rows we previously auto-seeded with this prefix
        List<Pair> autoSeeded = Jpa.tx(em -> {
            @SuppressWarnings("unchecked")
            List<Object[]> rows = em.createNativeQuery("""
                        SELECT category, key
                          FROM public.calculations
                         WHERE notes LIKE :prefix
                    """).setParameter("prefix", notesPrefix + "%").getResultList();
            return rows.stream().map(r -> new Pair((String) r[0], (String) r[1])).toList();
        });

        // Build a fast lookup of discovered normalized
        Set<Pair> discoveredNorm = discoveredNow.stream().map(Pair::norm).collect(Collectors.toSet());

        // Find stale
        List<Pair> stale = autoSeeded.stream()
                .map(Pair::norm)
                .filter(p -> !discoveredNorm.contains(p))
                .toList();

        if (stale.isEmpty()) {
            Log.info("Prune(%s): deleted=0", notesPrefix);
            return;
        }

        // Delete in chunks using tuple IN over VALUES
        int deletedTotal = 0;
        for (int i = 0; i < stale.size(); i += CHUNK_SIZE_PRUNE) {
            final List<Pair> chunk = stale.subList(i, Math.min(stale.size(), i + CHUNK_SIZE_PRUNE));
            deletedTotal += Jpa.tx(em -> {
                String values = chunk.stream()
                        .map(p -> "(?::text, ?::text)")
                        .collect(Collectors.joining(","));
                String sql = """
                            WITH doomed(category, key) AS (VALUES %s)
                            DELETE FROM public.calculations c
                             USING doomed
                             WHERE c.category = doomed.category
                               AND c.key = doomed.key
                               AND c.notes LIKE :prefix
                        """.formatted(values);
                var q = em.createNativeQuery(sql).setParameter("prefix", notesPrefix + "%");
                int idx = 1;
                for (Pair p : chunk) {
                    q.setParameter(idx++, p.category());
                    q.setParameter(idx++, p.key());
                }
                return q.executeUpdate();
            });
        }

        Log.info("Prune(%s): deleted=%d", notesPrefix, deletedTotal);
    }

    // ---------- model helpers ----------

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

    // ---------- utils ----------

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

    // ---------- tiny logger ----------

    private static class Log {
        static void section(String fmt, Object... args) {
            System.out.println("\n=== " + String.format(fmt, args) + " ===");
        }

        static void info(String fmt, Object... args) {
            System.out.println("[INFO] " + String.format(fmt, args));
        }

        static void warn(String fmt, Object... args) {
            System.err.println("[WARN] " + String.format(fmt, args));
        }
    }
}

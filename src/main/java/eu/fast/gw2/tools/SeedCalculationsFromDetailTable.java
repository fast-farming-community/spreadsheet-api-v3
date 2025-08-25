package eu.fast.gw2.tools;

import java.util.LinkedHashSet;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;

/**
 * Strict-DSL seeder + table-level ops:
 * - Scans BOTH public.detail_tables.rows and public.tables.rows.
 * - Discovers distinct (Category, Key) pairs including LEAF (Key="").
 * - UPSERTS default formulas_json={"mode": "..."} ONLY when formulas_json IS
 * NULL.
 * (Never overwrites an existing formulas_json.)
 * - Seeds table-level operation per table: INTERNAL => MAX, else SUM (set ONLY
 * when operation IS NULL).
 * - PRUNES ONLY rows previously auto-seeded by this tool (notes LIKE our
 * prefixes).
 */
public class SeedCalculationsFromDetailTable {

    private static final String COL_CAT = "Category";
    private static final String COL_KEY = "Key";

    private static final String NOTES_PREFIX_DSL = "auto-seed:strict-dsl";
    private static final String NOTES_PREFIX_OPS = "auto-seed:table-op";

    private static final ObjectMapper M = new ObjectMapper();

    // satisfy NOT NULL on taxes (engine can still apply defaults)
    private static final int DEFAULT_TAXES = 0;

    public static void run() throws Exception {
        System.out.println(
                ">>> SeedCalculations: default formulas_json (no overwrite) + table ops (INTERNAL->MAX, else SUM) + prune own");

        // 1) read JSON rows from BOTH sources
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

        // 2) discover row-level (category,key) pairs INCLUDING LEAF (key="")
        Set<Pair> discoveredDslPairs = new LinkedHashSet<>();
        // 2b) discover table-level pairs (dominant category + tableName) for ops
        Set<Pair> discoveredTablePairs = new LinkedHashSet<>();

        int jsonErrors = 0;

        // helper to parse row arrays and add LEAF/COMPOSITE/INTERNAL pairs
        java.util.function.Consumer<List<Map<String, Object>>> collectRowPairs = rows -> {
            for (var r : rows) {
                String cat = str(r.get(COL_CAT));
                String key = str(r.get(COL_KEY));
                // LEAF = both empty -> one global pair ("", "")
                if ((cat == null || cat.isBlank()) && (key == null || key.isBlank())) {
                    discoveredDslPairs.add(new Pair("", "")); // single global LEAF rule
                }
                // COMPOSITE/INTERNAL = key present -> (category, key)
                else if (key != null && !key.isBlank()) {
                    String c = (cat == null ? "" : cat.trim()); // categories are lower-case in your sheets
                    discoveredDslPairs.add(new Pair(c, key.trim()));
                }
            }
        };

        // parse detail rows
        for (Object[] t : detailTables) {
            long id = ((Number) t[0]).longValue();
            String tableKey = (String) t[1];
            String json = (String) t[2];
            if (json == null || json.isBlank())
                continue;

            try {
                List<Map<String, Object>> rows = M.readValue(json, new TypeReference<List<Map<String, Object>>>() {
                });
                collectRowPairs.accept(rows);
                String domCat = OverlayHelper.dominantCategory(rows);
                if (domCat != null && !domCat.isBlank() && tableKey != null && !tableKey.isBlank()) {
                    discoveredTablePairs.add(new Pair(domCat.trim(), tableKey.trim()));
                }
            } catch (Exception e) {
                jsonErrors++;
                if (jsonErrors <= 3) {
                    System.err.println("! JSON parse failed for detail_tables.id=" + id
                            + " key=" + tableKey + " (skipping): " + e.getMessage());
                }
            }
        }

        // parse main rows
        for (Object[] t : mainTables) {
            long id = ((Number) t[0]).longValue();
            String name = (String) t[1];
            String json = (String) t[2];
            if (json == null || json.isBlank())
                continue;

            try {
                List<Map<String, Object>> rows = M.readValue(json, new TypeReference<List<Map<String, Object>>>() {
                });
                collectRowPairs.accept(rows);
                String domCat = OverlayHelper.dominantCategory(rows);
                if (domCat != null && !domCat.isBlank() && name != null && !name.isBlank()) {
                    discoveredTablePairs.add(new Pair(domCat.trim(), name.trim()));
                }
            } catch (Exception e) {
                jsonErrors++;
                if (jsonErrors <= 3) {
                    System.err.println("! JSON parse failed for tables.id=" + id
                            + " name=" + name + " (skipping): " + e.getMessage());
                }
            }
        }

        System.out.println("Discovered DSL pairs: " + discoveredDslPairs.size());
        System.out.println("Discovered table op pairs: " + discoveredTablePairs.size());

        // 3) UPSERT default formulas_json per discovered (category,key)
        int filledOrInserted = 0;
        for (var p : discoveredDslPairs) {
            final String mode = (p.category().isEmpty() && p.key().isEmpty()) ? "LEAF"
                    : (equalsIgnoreCase(p.category(), "INTERNAL")) ? "INTERNAL"
                            : "COMPOSITE";

            final String formulasJson = "{\"mode\":\"" + mode + "\"}";

            // Safe default row-level operation (satisfy NOT NULL)
            final String defaultRowOp = "SUM";

            filledOrInserted += Jpa.tx(em -> em.createNativeQuery("""
                        INSERT INTO public.calculations
                              (category, key, operation, taxes, source_table_key, formulas_json, notes)
                        VALUES (:c, :k, :op, :tx, NULL, :fjson, :notes)
                        ON CONFLICT (category, key) DO UPDATE
                            SET formulas_json = EXCLUDED.formulas_json,
                                notes = CASE
                                          WHEN public.calculations.formulas_json IS NULL THEN EXCLUDED.notes
                                          ELSE public.calculations.notes
                                        END
                          WHERE public.calculations.formulas_json IS NULL
                    """)
                    .setParameter("c", p.category())
                    .setParameter("k", p.key())
                    .setParameter("op", defaultRowOp)
                    .setParameter("tx", DEFAULT_TAXES) // <-- was NULL
                    .setParameter("fjson", formulasJson)
                    .setParameter("notes", NOTES_PREFIX_DSL + " (mode=" + mode + ")")
                    .executeUpdate());
        }
        System.out.println("Formulas filled/inserted: " + filledOrInserted);

        // 4) seed/ensure table-level operation: INTERNAL => MAX, else SUM (ONLY when op
        // IS NULL)
        int upsertsOps = 0;
        for (var p : discoveredTablePairs) {
            String op = equalsIgnoreCase(p.category(), "INTERNAL") ? "MAX" : "SUM";
            upsertsOps += Jpa.tx(em -> em
                    .createNativeQuery(
                            """
                                        INSERT INTO public.calculations (category, key, operation, taxes, source_table_key, formulas_json, notes)
                                        VALUES (:c, :k, :op, :tx, NULL, NULL, :notes)
                                        ON CONFLICT (category, key) DO UPDATE
                                            SET operation = EXCLUDED.operation,
                                                notes = CASE
                                                          WHEN public.calculations.operation IS NULL THEN EXCLUDED.notes
                                                          ELSE public.calculations.notes
                                                        END
                                          WHERE public.calculations.operation IS NULL
                                    """)
                    .setParameter("c", p.category())
                    .setParameter("k", p.key())
                    .setParameter("op", op)
                    .setParameter("tx", DEFAULT_TAXES) // <-- was NULL
                    .setParameter("notes", NOTES_PREFIX_OPS + " (op=" + op + ")")
                    .executeUpdate());
        }
        System.out.println("Table-op set (only where NULL): " + upsertsOps);

        // 5) prune ONLY our auto-seeded rows that are no longer discovered
        pruneStaleByPrefix(discoveredDslPairs, NOTES_PREFIX_DSL);
        pruneStaleByPrefix(discoveredTablePairs, NOTES_PREFIX_OPS);

        System.out.println("Seed complete.");
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

        var stale = autoSeeded.stream().filter(p -> !discoveredNow.contains(p)).toList();

        if (stale.isEmpty()) {
            System.out.println("Prune(" + notesPrefix + "): deleted=0");
            return;
        }

        int deleted = Jpa.tx(em -> {
            int sum = 0;
            for (var p : stale) {
                sum += em.createNativeQuery("""
                            DELETE FROM public.calculations
                             WHERE category = :c AND key = :k AND notes LIKE :prefix
                        """)
                        .setParameter("c", p.category())
                        .setParameter("k", p.key())
                        .setParameter("prefix", notesPrefix + "%")
                        .executeUpdate();
            }
            return sum;
        });

        System.out.println("Prune(" + notesPrefix + "): deleted=" + deleted);
    }

    // ---------- utils ----------

    public record Pair(String category, String key) {
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
            return (n(category) + "\u0000" + n(key)).hashCode();
        }
    }

    private static boolean equalsIgnoreCase(String a, String b) {
        return a == null ? b == null : a.equalsIgnoreCase(b);
    }

    private static boolean eqi(String a, String b) {
        return equalsIgnoreCase(a, b);
    }

    private static String n(String s) {
        return (s == null ? "" : s.toLowerCase(Locale.ROOT));
    }

    private static String str(Object o) {
        return (o == null) ? null : String.valueOf(o);
    }
}

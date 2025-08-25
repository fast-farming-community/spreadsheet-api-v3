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
 * - INSERTS ONLY missing pairs with formulas_json={"mode": "..."} and
 * taxes=NULL.
 * - Seeds table-level operation per table: INTERNAL => MAX, else SUM (insert or
 * update only if operation IS NULL).
 * - Does NOT overwrite existing formulas or ops unless empty.
 * - PRUNES ONLY rows previously auto-seeded by this tool (notes LIKE specific
 * prefixes).
 */
public class SeedCalculationsFromDetailTable {

    private static final String COL_CAT = "Category";
    private static final String COL_KEY = "Key";

    private static final String NOTES_PREFIX_DSL = "auto-seed:strict-dsl";
    private static final String NOTES_PREFIX_OPS = "auto-seed:table-op";

    private static final ObjectMapper M = new ObjectMapper();

    public static void run() throws Exception {
        System.out.println(
                ">>> SeedCalculations: strict DSL (insert missing) + table ops (MAX for INTERNAL, else SUM) + prune own");

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
        // 2b) discover table-level (category,key=tableName) for ops
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
                    String c = (cat == null ? "" : cat.trim()); // INTERNAL or other categories allowed
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
                // row-level discovery
                collectRowPairs.accept(rows);
                // table-level op pair (dominant category + table key)
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
                // row-level discovery
                collectRowPairs.accept(rows);
                // table-level op pair (dominant category + table name)
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

        // 3) load existing pairs (category,key)
        List<Pair> existingPairs = Jpa.tx(em -> {
            @SuppressWarnings("unchecked")
            List<Object[]> rows = em.createNativeQuery("""
                        SELECT category, key FROM public.calculations
                    """).getResultList();
            return rows.stream().map(r -> new Pair((String) r[0], (String) r[1])).toList();
        });
        var existingSet = new java.util.HashSet<>(existingPairs);

        // 4) insert ONLY missing DSL pairs with {"mode": "..."} and taxes=NULL
        int insertedDsl = 0;
        for (var p : discoveredDslPairs) {
            if (existingSet.contains(p))
                continue;

            String mode;
            if (p.category().isEmpty() && p.key().isEmpty()) { // global LEAF rule
                mode = "LEAF";
            } else if (equalsIgnoreCase(p.category(), "INTERNAL")) { // INTERNAL with key present
                mode = "INTERNAL";
            } else { // any non-INTERNAL with key present
                mode = "COMPOSITE";
            }

            String formulasJson = "{\"mode\":\"" + mode + "\"}";

            insertedDsl += Jpa.tx(em -> em.createNativeQuery("""
                        INSERT INTO public.calculations
                            (category, key, operation, taxes, source_table_key, formulas_json, notes)
                        VALUES (:c, :k, :op, :tx, :src, :fjson, :notes)
                        ON CONFLICT (category, key) DO NOTHING
                    """)
                    .setParameter("c", p.category())
                    .setParameter("k", p.key())
                    .setParameter("op", null) // no row-level op by default
                    .setParameter("tx", null) // taxes NULL; engine picks defaults
                    .setParameter("src", null)
                    .setParameter("fjson", formulasJson)
                    .setParameter("notes", NOTES_PREFIX_DSL + " (mode=" + mode + ")")
                    .executeUpdate());
        }
        System.out.println("Inserted " + insertedDsl + " new strict-DSL rows.");

        // 5) seed/ensure table-level operation (INTERNAL => MAX, else SUM), but only if
        // operation is NULL
        int upsertsOps = 0;
        for (var p : discoveredTablePairs) {
            String op = equalsIgnoreCase(p.category(), "INTERNAL") ? "MAX" : "SUM";

            // Insert if missing; if present, update operation ONLY when it's NULL.
            upsertsOps += Jpa.tx(em -> em
                    .createNativeQuery(
                            """
                                        INSERT INTO public.calculations (category, key, operation, taxes, source_table_key, formulas_json, notes)
                                        VALUES (:c, :k, :op, NULL, NULL, NULL, :notes)
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
                    .setParameter("notes", NOTES_PREFIX_OPS + " (op=" + op + ")")
                    .executeUpdate());
        }
        System.out.println("Upserted " + upsertsOps + " table-op rows (set only where operation was NULL).");

        // 6) prune ONLY our auto-seeded rows that are no longer discovered
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

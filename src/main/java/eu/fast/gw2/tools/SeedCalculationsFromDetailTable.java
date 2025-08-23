package eu.fast.gw2.tools;

import java.util.LinkedHashSet;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;

public class SeedCalculationsFromDetailTable {

    private static final String COL_CAT = "Category";
    private static final String COL_KEY = "Key";
    private static final String DEFAULT_OPERATION = "SUM"; // used only for NEW rows
    private static final int DEFAULT_TAXES = 0;

    private static final ObjectMapper M = new ObjectMapper();

    /**
     * Seed rules (INSERT ONLY, no overwrite) and prune stale rules.
     * - Existing rows in public.calculations are left as-is (operation/taxes
     * unchanged).
     * - New rows are inserted with operation=SUM and taxes=0.
     * - Any (category,key) not discovered is deleted (pruned).
     */
    public static void run() throws Exception {
        System.out.println(">>> SeedCalculationsFromDetailTables"
                + " insertOnly(op=" + DEFAULT_OPERATION + ", taxes=" + DEFAULT_TAXES + ")"
                + " + auto-prune");

        // 1) load all candidate detail_tables (id, key, rows)
        List<Object[]> tables = Jpa.tx(em -> {
            @SuppressWarnings("unchecked")
            List<Object[]> rows = em.createNativeQuery("""
                    SELECT id, key, rows
                      FROM public.detail_tables
                     ORDER BY id DESC
                    """).getResultList();
            return rows;
        });

        // 2) gather distinct (category, key) from all rows json
        Set<Pair> discovered = new LinkedHashSet<>();
        int jsonErrors = 0;

        for (Object[] t : tables) {
            long id = ((Number) t[0]).longValue();
            String key = (String) t[1];
            String json = (String) t[2];

            if (json == null || json.isBlank())
                continue;

            List<Map<String, Object>> rows;
            try {
                rows = M.readValue(json, new TypeReference<List<Map<String, Object>>>() {
                });
            } catch (Exception e) {
                jsonErrors++;
                if (jsonErrors <= 3) {
                    System.err.println("! JSON parse failed for detail_tables.id=" + id + " key=" + key
                            + " (skipping): " + e.getMessage());
                }
                continue;
            }

            for (var r : rows) {
                String cat = str(r.get(COL_CAT));
                String k = str(r.get(COL_KEY));
                if (cat == null || cat.isBlank())
                    continue;
                if (k == null || k.isBlank())
                    continue;
                discovered.add(new Pair(cat.trim(), k.trim()));
            }
        }

        System.out.println("Found " + discovered.size() + " distinct (category,key) pairs in detail_tables.");

        // 3) Insert ONLY the new ones (don't touch existing rows)
        // Load existing (category,key)
        List<Pair> existing = Jpa.tx(em -> {
            @SuppressWarnings("unchecked")
            List<Object[]> rows = em.createNativeQuery("""
                    SELECT category, key FROM public.calculations
                    """).getResultList();
            return rows.stream().map(r -> new Pair((String) r[0], (String) r[1])).toList();
        });
        var existingSet = new java.util.HashSet<>(existing);

        int inserted = 0;
        for (var p : discovered) {
            if (existingSet.contains(p))
                continue; // do not overwrite
            inserted += Jpa.tx(em -> em.createNativeQuery("""
                        INSERT INTO public.calculations(category, key, operation, taxes, notes)
                        VALUES (:c, :k, :op, :tx, :notes)
                        ON CONFLICT (category, key) DO NOTHING
                    """)
                    .setParameter("c", p.category())
                    .setParameter("k", p.key())
                    .setParameter("op", DEFAULT_OPERATION)
                    .setParameter("tx", DEFAULT_TAXES)
                    .setParameter("notes",
                            "auto-seed (default op=" + DEFAULT_OPERATION + ", taxes=" + DEFAULT_TAXES + ")")
                    .executeUpdate());
        }
        System.out.println("Inserted " + inserted + " new calculation rows.");

        // 4) Always prune stale rules (anything not discovered now)
        pruneStale(discovered);

        System.out.println("Seed complete.");
    }

    // ---------- pruning ----------

    private static void pruneStale(Set<Pair> discoveredNow) {
        // load all current rules
        List<Pair> allRules = Jpa.tx(em -> {
            @SuppressWarnings("unchecked")
            List<Object[]> rows = em.createNativeQuery("""
                    SELECT category, key FROM public.calculations
                    """).getResultList();
            return rows.stream().map(r -> new Pair((String) r[0], (String) r[1])).toList();
        });

        // anything present in calculations but not discovered is stale
        var stale = allRules.stream().filter(p -> !discoveredNow.contains(p)).toList();

        if (stale.isEmpty()) {
            System.out.println("Prune: deleted=0");
            return;
        }

        int deleted = Jpa.tx(em -> {
            int sum = 0;
            for (var p : stale) {
                sum += em.createNativeQuery("""
                                DELETE FROM public.calculations
                                WHERE category = :c AND key = :k
                        """)
                        .setParameter("c", p.category())
                        .setParameter("k", p.key())
                        .executeUpdate();
            }
            return sum;
        });

        System.out.println("Prune: deleted=" + deleted);
    }

    // ---------- utils ----------

    // Case-insensitive identity for (category, key)
    public record Pair(String category, String key) {
        @Override
        public boolean equals(Object o) {
            if (this == o)
                return true;
            if (!(o instanceof Pair p))
                return false;
            return category != null && key != null
                    && p.category != null && p.key != null
                    && category.equalsIgnoreCase(p.category)
                    && key.equalsIgnoreCase(p.key);
        }

        @Override
        public int hashCode() {
            return (category.toLowerCase(Locale.ROOT) + "\u0000" + key.toLowerCase(Locale.ROOT)).hashCode();
        }
    }

    private static String str(Object o) {
        return (o == null) ? null : String.valueOf(o);
    }
}

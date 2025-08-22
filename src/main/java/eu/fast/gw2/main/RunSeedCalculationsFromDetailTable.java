package eu.fast.gw2.main;

import java.util.ArrayList;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;

import eu.fast.gw2.tools.Jpa;

public class RunSeedCalculationsFromDetailTable {

    private static final String COL_CAT = "Category";
    private static final String COL_KEY = "Key";
    private static final ObjectMapper M = new ObjectMapper();

    // CLI:
    // --dry-run : only print inserts/updates
    // --force : also update existing rows
    // --op=SUM|AVG|MIN|MAX (default SUM)
    // --taxes=NN : default taxes for non-INTERNAL (default 15)
    // --fid=12 : only detail_feature_id=12
    // --key-like=geo : only keys ILIKE %geo%
    // --limit=100 : inspect at most N tables
    // --prune : delete rules not found in current dataset
    // --prune-dry : print stale rules but do not delete
    // --keep-internal : never delete INTERNAL rules (default false)
    public static void main(String[] args) throws Exception {
        boolean dryRun = hasArg(args, "--dry-run");
        boolean force = hasArg(args, "--force");
        boolean prune = hasArg(args, "--prune");
        boolean pruneDry = hasArg(args, "--prune-dry");
        boolean keepInternal = hasArg(args, "--keep-internal");

        String op = opt(args, "--op=", "SUM").trim().toUpperCase(Locale.ROOT);
        int taxesDef = parseInt(opt(args, "--taxes=", "15"), 15);

        Integer fidFilter = parseNullableInt(opt(args, "--fid=", null));
        String keyLike = opt(args, "--key-like=", null);
        Integer tableLimit = parseNullableInt(opt(args, "--limit=", null));

        System.out.println(">>> SeedCalculationsFromDetailTables"
                + " dryRun=" + dryRun
                + " force=" + force
                + " op=" + op
                + " taxesDef=" + taxesDef
                + (fidFilter != null ? " fid=" + fidFilter : "")
                + (keyLike != null ? " keyLike=" + keyLike : "")
                + (tableLimit != null ? " limit=" + tableLimit : "")
                + " prune=" + prune
                + " pruneDry=" + pruneDry
                + " keepInternal=" + keepInternal);

        // 1) load candidate detail_tables (id, detail_feature_id, key, rows)
        List<Object[]> tables = Jpa.tx(em -> {
            StringBuilder sql = new StringBuilder("""
                        SELECT id, detail_feature_id, key, rows
                          FROM public.detail_tables
                    """);
            List<String> where = new ArrayList<>();
            if (fidFilter != null)
                where.add("detail_feature_id = :fid");
            if (keyLike != null)
                where.add("key ILIKE :klike");
            if (!where.isEmpty())
                sql.append(" WHERE ").append(String.join(" AND ", where));
            sql.append(" ORDER BY id DESC");
            if (tableLimit != null)
                sql.append(" LIMIT ").append(tableLimit);

            var q = em.createNativeQuery(sql.toString());
            if (fidFilter != null)
                q.setParameter("fid", fidFilter);
            if (keyLike != null)
                q.setParameter("klike", "%" + keyLike + "%");

            @SuppressWarnings("unchecked")
            List<Object[]> rows = q.getResultList();
            return rows;
        });

        // 2) gather distinct (category, key) from all rows json
        Set<Pair> discovered = new LinkedHashSet<>();
        int jsonErrors = 0;

        for (Object[] t : tables) {
            Long id = ((Number) t[0]).longValue();
            String key = (String) t[2];
            String json = (String) t[3];

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

        System.out.println("Found " + discovered.size() + " distinct (category,key) pairs.");

        if (discovered.isEmpty()) {
            System.out.println("Nothing to seed.");
        } else {
            // 3) load existing calculation keys to skip (unless --force)
            Set<Pair> existing = Jpa.tx(em -> {
                @SuppressWarnings("unchecked")
                List<Object[]> rows = em.createNativeQuery("""
                                SELECT category, key FROM public.calculations
                        """).getResultList();
                return rows.stream()
                        .map(r -> new Pair((String) r[0], (String) r[1]))
                        .collect(Collectors.toSet());
            });

            // 4) build worklist and upsert
            int inserted = 0, updated = 0, skipped = 0;

            for (Pair p : discovered) {
                boolean exists = existing.contains(p);

                int taxes = "INTERNAL".equalsIgnoreCase(p.category) ? 0 : taxesDef;
                String notes = "auto-seed (" + op + ", taxes=" + taxes + ")";

                if (exists && !force) {
                    skipped++;
                    continue;
                }

                if (dryRun) {
                    System.out.printf(Locale.ROOT, "%s %s | op=%s taxes=%d%s%n",
                            (exists ? "UPDATE" : "INSERT"),
                            p, op, taxes,
                            (exists ? "  (was present)" : ""));
                } else {
                    int n = Jpa.tx(em -> em.createNativeQuery("""
                                INSERT INTO public.calculations(category, key, operation, taxes, notes)
                                VALUES (:c, :k, :op, :tx, :notes)
                                ON CONFLICT (category, key) DO UPDATE
                                  SET operation = EXCLUDED.operation,
                                      taxes     = EXCLUDED.taxes,
                                      notes     = EXCLUDED.notes,
                                      updated_at = now()
                            """)
                            .setParameter("c", p.category)
                            .setParameter("k", p.key)
                            .setParameter("op", op)
                            .setParameter("tx", taxes)
                            .setParameter("notes", notes)
                            .executeUpdate());

                    if (exists)
                        updated += n;
                    else
                        inserted += n;
                }
            }

            if (!dryRun) {
                System.out.println("Seed done. inserted=" + inserted + " updated=" + updated + " skipped=" + skipped);
            } else {
                System.out.println("Seed dry-run. wouldWrite=" + (discovered.size() - skipped) + " skipped=" + skipped);
            }
        }

        // 5) Optional pruning of stale rules
        if (prune || pruneDry) {
            pruneStale(discovered, pruneDry);
        }
    }

    private static void pruneStale(Set<Pair> discoveredNow, boolean dry) {
        // load all current rules
        List<Pair> allRules = Jpa.tx(em -> {
            @SuppressWarnings("unchecked")
            List<Object[]> rows = em.createNativeQuery("""
                            SELECT category, key FROM public.calculations
                    """).getResultList();
            return rows.stream().map(r -> new Pair((String) r[0], (String) r[1]))
                    .collect(Collectors.toList());
        });

        // anything present in calculations but not discovered is stale
        List<Pair> stale = allRules.stream()
                .filter(p -> !discoveredNow.contains(p))
                .collect(Collectors.toList());

        if (stale.isEmpty()) {
            System.out.println("Prune: nothing stale.");
            return;
        }

        if (dry) {
            System.out.println("Prune dry-run. Would delete " + stale.size() + " rows:");
            for (Pair p : stale)
                System.out.println("  DEL " + p);
            return;
        }

        int deleted = Jpa.tx(em -> {
            int sum = 0;
            for (Pair p : stale) {
                sum += em.createNativeQuery("""
                                DELETE FROM public.calculations
                                WHERE category = :c AND key = :k
                        """).setParameter("c", p.category).setParameter("k", p.key).executeUpdate();
            }
            return sum;
        });

        System.out.println("Prune done. deleted=" + deleted);
    }

    // ===== utils =====
    private record Pair(String category, String key) {
        @Override
        public String toString() {
            return "(" + category + ", " + key + ")";
        }

        @Override
        public boolean equals(Object o) {
            if (this == o)
                return true;
            if (!(o instanceof Pair p))
                return false;
            return category.equalsIgnoreCase(p.category) && key.equalsIgnoreCase(p.key);
        }

        @Override
        public int hashCode() {
            return (category.toLowerCase(Locale.ROOT) + "\u0000" + key.toLowerCase(Locale.ROOT)).hashCode();
        }
    }

    private static boolean hasArg(String[] args, String flag) {
        for (String a : args)
            if (a.equalsIgnoreCase(flag))
                return true;
        return false;
    }

    private static String opt(String[] args, String key, String def) {
        for (String a : args)
            if (a.startsWith(key))
                return a.substring(key.length());
        return def;
    }

    private static int parseInt(String s, int def) {
        try {
            return Integer.parseInt(s);
        } catch (Exception e) {
            return def;
        }
    }

    private static Integer parseNullableInt(String s) {
        if (s == null)
            return null;
        try {
            return Integer.valueOf(s.trim());
        } catch (Exception e) {
            return null;
        }
    }

    private static String str(Object o) {
        return (o == null) ? null : String.valueOf(o);
    }
}

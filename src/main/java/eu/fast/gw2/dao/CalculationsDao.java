package eu.fast.gw2.dao;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import eu.fast.gw2.tools.Jpa;

public class CalculationsDao {

    public record Config(String category,
            String key,
            String operation,
            int taxes,
            String sourceTableKey,
            String formulasJson) {
    }

    /** Existing single (category,key) lookup. */
    public static Config find(String category, String key) {
        return Jpa.tx(em -> {
            List<?> r = em.createNativeQuery("""
                      SELECT category,
                             key,
                             operation,
                             taxes,
                             source_table_key,
                             formulas_json
                        FROM public.calculations
                       WHERE category = :c
                         AND key = :k
                       ORDER BY id DESC
                       LIMIT 1
                    """)
                    .setParameter("c", category)
                    .setParameter("k", key)
                    .getResultList();

            if (r.isEmpty())
                return null;

            Object[] a = (Object[]) r.get(0);

            int taxes = 15;
            Object t = a[3];
            if (t instanceof Number n)
                taxes = n.intValue();
            else if (t != null) {
                try {
                    taxes = Integer.parseInt(t.toString());
                } catch (Exception ignored) {
                }
            }

            return new Config(
                    (String) a[0], // category
                    (String) a[1], // key
                    (String) a[2], // operation
                    taxes,
                    (String) a[4], // source_table_key
                    (String) a[5] // formulas_json
            );
        });
    }

    /** Bulk-load latest row per (category,key) in one roundtrip. */
    public static Map<String, Config> findAllLatest() {
        return Jpa.<Map<String, Config>>tx(em -> {
            List<Object[]> rows = (List<Object[]>) em.createNativeQuery("""
                        SELECT DISTINCT ON (category, key)
                               category,
                               key,
                               operation,
                               taxes,
                               source_table_key,
                               formulas_json
                          FROM public.calculations
                         ORDER BY category, key, id DESC
                    """).getResultList();

            Map<String, Config> out = new HashMap<>(Math.max(16, rows.size() * 2));

            for (Object[] a : rows) {
                String category = (String) a[0];
                String key = (String) a[1];
                String op = (String) a[2];

                int taxes = 15;
                Object t = a[3];
                if (t instanceof Number n)
                    taxes = n.intValue();
                else if (t != null) {
                    try {
                        taxes = Integer.parseInt(t.toString());
                    } catch (Exception ignored) {
                    }
                }

                String source = (String) a[4];
                String formulas = (String) a[5];

                Config cfg = new Config(category, key, op, taxes, source, formulas);

                String ck = ((category == null ? "" : category.trim().toUpperCase(java.util.Locale.ROOT))
                        + "|" + (key == null ? "" : key.trim()));
                out.put(ck, cfg);
            }
            return out;
        });
    }
}

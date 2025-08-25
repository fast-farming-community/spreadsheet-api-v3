package eu.fast.gw2.dao;

import eu.fast.gw2.tools.Jpa;

public class CalculationsDao {

    // Added formulasJson at the end; kept your existing fields.
    public record Config(String category,
            String key,
            String operation,
            int taxes,
            String sourceTableKey,
            String formulasJson) {
    }

    public static Config find(String category, String key) {
        return Jpa.tx(em -> {
            var r = em.createNativeQuery("""
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
            if (a[3] instanceof Number n)
                taxes = n.intValue();
            else if (a[3] != null)
                try {
                    taxes = Integer.parseInt(a[3].toString());
                } catch (Exception ignored) {
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
}

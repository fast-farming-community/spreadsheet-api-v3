package eu.fast.gw2.dao;

import eu.fast.gw2.jpa.Jpa;

public class CalculationsDao {
    public record Config(String category, String key, String operation, int taxes, String sourceTableKey) {
    }

    public static Config find(String category, String key) {
        return Jpa.tx(em -> {
            var r = em.createNativeQuery("""
                      SELECT category, key, operation, taxes, source_table_key
                        FROM public.calculations
                       WHERE category=:c AND key=:k
                    """).setParameter("c", category)
                    .setParameter("k", key)
                    .getResultList();
            if (r.isEmpty())
                return null;
            Object[] a = (Object[]) r.get(0);
            return new Config(
                    (String) a[0], (String) a[1],
                    (String) a[2],
                    ((Number) a[3]).intValue(),
                    (String) a[4]);
        });
    }
}

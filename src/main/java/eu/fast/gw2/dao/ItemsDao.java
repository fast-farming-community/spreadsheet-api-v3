package eu.fast.gw2.dao;

import eu.fast.gw2.jpa.Jpa;

public class ItemsDao {
    public static Integer vendorValueById(int id) {
        return Jpa.tx(em -> {
            var r = em.createNativeQuery("""
                        SELECT vendor_value
                          FROM public.items
                         WHERE id = :id
                    """).setParameter("id", id)
                    .getResultList();
            if (r.isEmpty() || r.get(0) == null)
                return null;
            return ((Number) r.get(0)).intValue();
        });
    }
}

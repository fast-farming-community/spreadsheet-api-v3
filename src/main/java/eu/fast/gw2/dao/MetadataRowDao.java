package eu.fast.gw2.dao;

import eu.fast.gw2.jpa.Jpa;

public class MetadataRowDao {
    public static Long create(String name, String data) {
        return Jpa.tx(em -> ((Number) em.createNativeQuery("""
                  INSERT INTO public.metadata(name, data, inserted_at, updated_at)
                  VALUES (:n, :d, now(), now()) RETURNING id
                """).setParameter("n", name)
                .setParameter("d", data)
                .getSingleResult()).longValue());
    }

    public static void update(long id, String name, String data) {
        Jpa.tx(em -> em.createNativeQuery("""
                  UPDATE public.metadata SET name=:n, data=:d, updated_at=now() WHERE id=:id
                """).setParameter("n", name)
                .setParameter("d", data)
                .setParameter("id", id)
                .executeUpdate());
    }
}

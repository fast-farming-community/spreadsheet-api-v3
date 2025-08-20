package eu.fast.gw2.dao;

import eu.fast.gw2.jpa.Jpa;

public class ContributorsDao {
    public static Long create(String name, String type, boolean published) {
        return Jpa.tx(em -> ((Number) em.createNativeQuery("""
                  INSERT INTO public.contributors(name, type, published, inserted_at, updated_at)
                  VALUES (:n, :t, :p, now(), now()) RETURNING id
                """).setParameter("n", name)
                .setParameter("t", type)
                .setParameter("p", published)
                .getSingleResult()).longValue());
    }

    public static void update(long id, String name, String type, boolean published) {
        Jpa.tx(em -> em.createNativeQuery("""
                  UPDATE public.contributors SET name=:n, type=:t, published=:p, updated_at=now() WHERE id=:id
                """).setParameter("n", name)
                .setParameter("t", type)
                .setParameter("p", published)
                .setParameter("id", id)
                .executeUpdate());
    }
}

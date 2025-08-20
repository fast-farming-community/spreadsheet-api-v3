package eu.fast.gw2.dao;

import eu.fast.gw2.jpa.Jpa;

public class AboutDao {
    public static Long create(String title, String content, boolean published, Integer orderIndex) {
        return Jpa.tx(em -> ((Number) em.createNativeQuery("""
                  INSERT INTO public.about(title, content, published, "order", inserted_at, updated_at)
                  VALUES (:t, :c, :p, :o, now(), now()) RETURNING id
                """).setParameter("t", title)
                .setParameter("c", content)
                .setParameter("p", published)
                .setParameter("o", orderIndex)
                .getSingleResult()).longValue());
    }

    public static void update(long id, String title, String content, boolean published, Integer orderIndex) {
        Jpa.tx(em -> em.createNativeQuery("""
                  UPDATE public.about SET title=:t, content=:c, published=:p, "order"=:o, updated_at=now() WHERE id=:id
                """).setParameter("t", title)
                .setParameter("c", content)
                .setParameter("p", published)
                .setParameter("o", orderIndex)
                .setParameter("id", id)
                .executeUpdate());
    }
}

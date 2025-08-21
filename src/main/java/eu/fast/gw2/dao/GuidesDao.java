package eu.fast.gw2.dao;

import eu.fast.gw2.tools.Jpa;

public class GuidesDao {
  public static Long create(String title, String info, String image, String farmtrain, boolean published,
      Integer orderIndex) {
    return Jpa.tx(em -> ((Number) em.createNativeQuery("""
          INSERT INTO public.guides(title, info, image, farmtrain, published, "order", inserted_at, updated_at)
          VALUES (:ti, :in, :im, :fa, :p, :o, now(), now()) RETURNING id
        """).setParameter("ti", title)
        .setParameter("in", info)
        .setParameter("im", image)
        .setParameter("fa", farmtrain)
        .setParameter("p", published)
        .setParameter("o", orderIndex)
        .getSingleResult()).longValue());
  }

  public static void update(long id, String title, String info, String image, String farmtrain, boolean published,
      Integer orderIndex) {
    Jpa.tx(em -> em.createNativeQuery("""
          UPDATE public.guides
          SET title=:ti, info=:in, image=:im, farmtrain=:fa, published=:p, "order"=:o, updated_at=now()
          WHERE id=:id
        """).setParameter("ti", title)
        .setParameter("in", info)
        .setParameter("im", image)
        .setParameter("fa", farmtrain)
        .setParameter("p", published)
        .setParameter("o", orderIndex)
        .setParameter("id", id)
        .executeUpdate());
  }
}

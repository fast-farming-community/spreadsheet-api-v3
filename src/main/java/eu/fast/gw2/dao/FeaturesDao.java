package eu.fast.gw2.dao;

import eu.fast.gw2.jpa.Jpa;

public class FeaturesDao {

  public static Long ensure(String name) {
    return Jpa.tx(em -> (Long) em.createNativeQuery("""
          INSERT INTO public.features(name, published, inserted_at, updated_at)
          VALUES (:n, false, now(), now())
          ON CONFLICT (name) DO UPDATE SET updated_at = now()
          RETURNING id
        """).setParameter("n", name).getSingleResult());
  }

  public static void setPublished(long id, boolean published) {
    Jpa.tx(em -> em.createNativeQuery("""
          UPDATE public.features SET published=:p, updated_at=now() WHERE id=:id
        """).setParameter("p", published).setParameter("id", id).executeUpdate());
  }
}

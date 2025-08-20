package eu.fast.gw2.dao;

import eu.fast.gw2.jpa.Jpa;
import eu.fast.gw2.model.DetailFeature;
import jakarta.persistence.NoResultException;

public class DetailFeaturesDao {

    public static Long ensure(String name) {
        return Jpa.tx(em -> (Long) em.createNativeQuery("""
                  INSERT INTO public.detail_features(name, published, inserted_at, updated_at)
                  VALUES (:n, false, now(), now())
                  ON CONFLICT (name) DO UPDATE SET updated_at = now()
                  RETURNING id
                """).setParameter("n", name).getSingleResult());
    }

    public static DetailFeature findById(long id) {
        return Jpa.tx(em -> em.find(DetailFeature.class, id));
    }

    public static DetailFeature findByName(String name) {
        return Jpa.tx(em -> {
            try {
                return em.createQuery("select f from DetailFeature f where f.name=:n", DetailFeature.class)
                        .setParameter("n", name)
                        .getSingleResult();
            } catch (NoResultException e) {
                return null;
            }
        });
    }
}

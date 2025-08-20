package eu.fast.gw2.dao;

import eu.fast.gw2.jpa.Jpa;

public class PagesDao {
    public static void upsert(long featureId, String name, boolean published) {
        Jpa.tx(em -> em.createNativeQuery("""
                  INSERT INTO public.pages(feature_id, name, published, inserted_at, updated_at)
                  VALUES (:fid, :nm, :pub, now(), now())
                  ON CONFLICT (feature_id, name) DO UPDATE
                    SET published = EXCLUDED.published,
                        updated_at = now()
                """)
                .setParameter("fid", featureId)
                .setParameter("nm", name)
                .setParameter("pub", published)
                .executeUpdate());
    }
}

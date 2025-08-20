package eu.fast.gw2.dao;

import eu.fast.gw2.jpa.Jpa;

public class DetailTablesDao {

    public static void upsert(long detailFeatureId, String key, String name, String range,
            String rowsJson, String descriptionOrNull) {
        Jpa.txVoid(em -> {
            em.createNativeQuery(
                    """
                              INSERT INTO public.detail_tables(detail_feature_id, key, name, range, rows, description, inserted_at, updated_at)
                              VALUES (:fid, :k, :nm, :rg, :rows, :desc, now(), now())
                              ON CONFLICT (detail_feature_id, key) DO UPDATE
                                SET name = EXCLUDED.name,
                                    range = EXCLUDED.range,
                                    rows = EXCLUDED.rows,
                                    description = EXCLUDED.description,
                                    updated_at = now()
                            """)
                    .setParameter("fid", detailFeatureId)
                    .setParameter("k", key)
                    .setParameter("nm", name)
                    .setParameter("rg", range)
                    .setParameter("rows", rowsJson)
                    .setParameter("desc", descriptionOrNull)
                    .executeUpdate();
        });
    }
}

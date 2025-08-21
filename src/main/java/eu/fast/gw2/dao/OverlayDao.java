package eu.fast.gw2.dao;

import eu.fast.gw2.tools.Jpa;

public final class OverlayDao {
    private OverlayDao() {
    }

    public static void upsertDetail(long fid, String key, String tier, String rowsJson) {
        Jpa.txVoid(em -> em.createNativeQuery("""
                    INSERT INTO public.detail_tables_overlay(detail_feature_id, key, tier, rows, updated_at)
                    VALUES (:fid,:k,:t,CAST(:rows AS jsonb), now())
                    ON CONFLICT (detail_feature_id, key, tier) DO UPDATE
                    SET rows = EXCLUDED.rows, updated_at = now()
                """).setParameter("fid", fid)
                .setParameter("k", key)
                .setParameter("t", tier)
                .setParameter("rows", rowsJson)
                .executeUpdate());
    }

    public static void upsertMain(String key, String tier, String rowsJson) {
        Jpa.txVoid(em -> em.createNativeQuery("""
                    INSERT INTO public.tables_overlay(key, tier, rows, updated_at)
                    VALUES (:k,:t,CAST(:rows AS jsonb), now())
                    ON CONFLICT (key, tier) DO UPDATE
                    SET rows = EXCLUDED.rows, updated_at = now()
                """).setParameter("k", key)
                .setParameter("t", tier)
                .setParameter("rows", rowsJson)
                .executeUpdate());
    }
}

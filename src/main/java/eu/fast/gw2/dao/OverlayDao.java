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
                    WHERE public.detail_tables_overlay.rows IS DISTINCT FROM EXCLUDED.rows
                """)
                .setParameter("fid", fid)
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
                    WHERE public.tables_overlay.rows IS DISTINCT FROM EXCLUDED.rows
                """)
                .setParameter("k", key)
                .setParameter("t", tier)
                .setParameter("rows", rowsJson)
                .executeUpdate());
    }

    // Batch adapters (used by OverlayEngine writer when
    // overlay.writerUseBatch=true)
    public static int upsertMainBatch(java.util.List<String> keys, java.util.List<String> tiers,
            java.util.List<String> jsons) {
        if (keys == null || tiers == null || jsons == null || keys.size() != tiers.size()
                || keys.size() != jsons.size())
            return 0;
        java.util.List<eu.fast.gw2.dao.MainWrite> batch = new java.util.ArrayList<>(keys.size());
        for (int i = 0; i < keys.size(); i++) {
            batch.add(new MainWrite(keys.get(i), tiers.get(i), jsons.get(i)));
        }
        return eu.fast.gw2.dao.OverlayDaoBatch.batchUpsertMain(batch);
    }

    public static int upsertDetailBatch(java.util.List<Long> fids, java.util.List<String> keys,
            java.util.List<String> tiers, java.util.List<String> jsons) {
        if (fids == null || keys == null || tiers == null || jsons == null
                || fids.size() != keys.size() || fids.size() != tiers.size() || fids.size() != jsons.size())
            return 0;
        java.util.List<eu.fast.gw2.dao.DetailWrite> batch = new java.util.ArrayList<>(fids.size());
        for (int i = 0; i < fids.size(); i++) {
            batch.add(new DetailWrite(fids.get(i), keys.get(i), tiers.get(i), jsons.get(i)));
        }
        return eu.fast.gw2.dao.OverlayDaoBatch.batchUpsertDetail(batch);
    }
}

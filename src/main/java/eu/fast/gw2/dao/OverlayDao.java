package eu.fast.gw2.dao;

import eu.fast.gw2.tools.Jpa;
import eu.fast.gw2.tools.OverlayDBAccess;

public final class OverlayDao {
    private OverlayDao() {
    }

    // -------------------------
    // DETAIL
    // -------------------------
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

    // -------------------------
    // MAIN
    // -------------------------

    /** New preferred per-row upsert using page_id + name + tier. */
    public static void upsertMain(int pageId, String name, String tier, String rowsJson) {
        String pk = OverlayDBAccess.pageNameById(pageId);
        final String pageKeyFinal = (pk == null || pk.isBlank()) ? name : pk;

        Jpa.txVoid(em -> em.createNativeQuery("""
                    INSERT INTO public.tables_overlay(page_id, key, tier, rows, updated_at)
                    VALUES (:pid,:k,:t,CAST(:rows AS jsonb), now())
                    ON CONFLICT (page_id, key, tier) DO UPDATE
                    SET rows = EXCLUDED.rows, updated_at = now()
                    WHERE public.tables_overlay.rows IS DISTINCT FROM EXCLUDED.rows
                """)
                .setParameter("pid", pageId)
                .setParameter("k", pageKeyFinal) // capture the final value
                .setParameter("t", tier)
                .setParameter("rows", rowsJson)
                .executeUpdate());
    }

    /**
     * Back-compat per-row upsert (old signature).
     * Uses page_id = 0 as a fallback.
     */
    public static void upsertMain(String key, String tier, String rowsJson) {
        upsertMain(0, key, tier, rowsJson);
    }

    // -------------------------
    // BATCH
    // -------------------------

    /**
     * New preferred batch upsert for mains using page_id + name + tier.
     * Sizes of all lists must match.
     * Returns affected rows (inserted + updated).
     */
    public static int upsertMainBatch(java.util.List<Integer> pageIds,
            java.util.List<String> names,
            java.util.List<String> tiers,
            java.util.List<String> jsons) {
        if (pageIds == null || names == null || tiers == null || jsons == null
                || pageIds.size() != names.size()
                || pageIds.size() != tiers.size()
                || pageIds.size() != jsons.size()) {
            return 0;
        }
        return OverlayDaoBatch.batchUpsertMain(pageIds, names, tiers, jsons);
    }

    /**
     * Back-compat batch upsert (old signature).
     * Delegates with page_id = 0 for each record.
     */
    public static int upsertMainBatch(java.util.List<String> keys,
            java.util.List<String> tiers,
            java.util.List<String> jsons) {
        if (keys == null || tiers == null || jsons == null
                || keys.size() != tiers.size()
                || keys.size() != jsons.size()) {
            return 0;
        }
        java.util.ArrayList<Integer> pageIds = new java.util.ArrayList<>(keys.size());
        for (int i = 0; i < keys.size(); i++)
            pageIds.add(0);
        return OverlayDaoBatch.batchUpsertMain(pageIds, keys, tiers, jsons);
    }

    // -------------------------
    // DETAIL BATCH
    // -------------------------
    public static int upsertDetailBatch(java.util.List<Long> fids,
            java.util.List<String> keys,
            java.util.List<String> tiers,
            java.util.List<String> jsons) {
        if (fids == null || keys == null || tiers == null || jsons == null
                || fids.size() != keys.size()
                || fids.size() != tiers.size()
                || fids.size() != jsons.size()) {
            return 0;
        }
        java.util.List<DetailWrite> batch = new java.util.ArrayList<>(fids.size());
        for (int i = 0; i < fids.size(); i++) {
            batch.add(new DetailWrite(fids.get(i), keys.get(i), tiers.get(i), jsons.get(i)));
        }
        return OverlayDaoBatch.batchUpsertDetail(batch);
    }
}

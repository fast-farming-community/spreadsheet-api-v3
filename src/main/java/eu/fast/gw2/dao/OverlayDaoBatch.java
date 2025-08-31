package eu.fast.gw2.dao;

import java.util.List;

import eu.fast.gw2.tools.Jpa;
import jakarta.persistence.Query;

public class OverlayDaoBatch {

    /** Returns affected rows (inserted + updated). */
    public static int batchUpsertMain(List<Integer> pageIds,
            List<String> names,
            List<String> tiers,
            List<String> jsons) {
        if (pageIds == null || names == null || tiers == null || jsons == null
                || pageIds.size() != names.size()
                || pageIds.size() != tiers.size()
                || pageIds.size() != jsons.size()
                || pageIds.isEmpty()) {
            return 0;
        }

        final StringBuilder sb = new StringBuilder(256 + pageIds.size() * 40);
        sb.append("""
                    INSERT INTO public.tables_overlay (page_id, key, tier, rows, updated_at)
                    VALUES
                """);
        for (int i = 0; i < pageIds.size(); i++) {
            if (i > 0)
                sb.append(',');
            sb.append("(?,?,?,CAST(? AS jsonb), now())");
        }
        sb.append("""
                    ON CONFLICT (page_id, key, tier) DO UPDATE
                    SET rows = EXCLUDED.rows, updated_at = now()
                    WHERE public.tables_overlay.rows IS DISTINCT FROM EXCLUDED.rows
                """);

        final String sql = sb.toString();
        return Jpa.tx(em -> {
            Query q = em.createNativeQuery(sql);
            int p = 1;
            for (int i = 0; i < pageIds.size(); i++) {
                q.setParameter(p++, pageIds.get(i));
                q.setParameter(p++, names.get(i));
                q.setParameter(p++, tiers.get(i));
                q.setParameter(p++, jsons.get(i));
            }
            return q.executeUpdate();
        });
    }

    /** Returns affected rows (inserted + updated). */
    public static int batchUpsertDetail(List<DetailWrite> batch) {
        if (batch == null || batch.isEmpty())
            return 0;

        final StringBuilder sb = new StringBuilder(256 + batch.size() * 40);
        sb.append("""
                    INSERT INTO public.detail_tables_overlay (detail_feature_id, key, tier, rows, updated_at)
                    VALUES
                """);
        for (int i = 0; i < batch.size(); i++) {
            if (i > 0)
                sb.append(',');
            sb.append("(?,?,?,CAST(? AS jsonb), now())");
        }
        sb.append("""
                    ON CONFLICT (detail_feature_id, key, tier) DO UPDATE
                    SET rows = EXCLUDED.rows, updated_at = now()
                    WHERE public.detail_tables_overlay.rows IS DISTINCT FROM EXCLUDED.rows
                """);

        final String sql = sb.toString();
        return Jpa.tx(em -> {
            Query q = em.createNativeQuery(sql);
            int p = 1;
            for (DetailWrite w : batch) {
                q.setParameter(p++, w.featureId());
                q.setParameter(p++, w.key());
                q.setParameter(p++, w.tier());
                q.setParameter(p++, w.json());
            }
            return q.executeUpdate();
        });
    }
}

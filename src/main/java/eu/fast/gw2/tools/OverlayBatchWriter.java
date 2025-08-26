package eu.fast.gw2.tools;

import java.util.ArrayList;
import java.util.List;

import jakarta.persistence.EntityManager;
import jakarta.persistence.Query;

/**
 * Batches overlay upserts into single SQL statements per flush.
 * - detail -> public.detail_tables_overlay(detail_feature_id, key, tier, rows,
 * updated_at)
 * - main -> public.tables_overlay(key, tier, rows, updated_at)
 *
 * Usage: create once, enqueue items, it auto-flushes on batch size and on
 * close().
 */
public final class OverlayBatchWriter implements AutoCloseable {

    private final int batchSize;

    private static final class Detail {
        final long fid;
        final String key;
        final String tier;
        final String json;

        Detail(long fid, String key, String tier, String json) {
            this.fid = fid;
            this.key = key;
            this.tier = tier;
            this.json = json;
        }
    }

    private static final class MainRow {
        final String key;
        final String tier;
        final String json;

        MainRow(String key, String tier, String json) {
            this.key = key;
            this.tier = tier;
            this.json = json;
        }
    }

    private final List<Detail> detailBuf;
    private final List<MainRow> mainBuf;

    public OverlayBatchWriter(int batchSize) {
        this.batchSize = Math.max(1, batchSize);
        this.detailBuf = new ArrayList<>(this.batchSize);
        this.mainBuf = new ArrayList<>(this.batchSize);
    }

    public void enqueueDetail(long fid, String key, String tier, String json) {
        detailBuf.add(new Detail(fid, key, tier, json));
        if (detailBuf.size() >= batchSize)
            flushDetails();
    }

    public void enqueueMain(String key, String tier, String json) {
        mainBuf.add(new MainRow(key, tier, json));
        if (mainBuf.size() >= batchSize)
            flushMains();
    }

    /** Manually flush both buffers (single transaction per buffer). */
    public void flush() {
        flushDetails();
        flushMains();
    }

    @Override
    public void close() {
        flush();
    }

    // ------------------ internals ------------------

    private void flushDetails() {
        if (detailBuf.isEmpty())
            return;

        final int n = detailBuf.size();
        final StringBuilder sb = new StringBuilder(256 + n * 32);
        sb.append("""
                    INSERT INTO public.detail_tables_overlay(detail_feature_id, key, tier, rows, updated_at)
                    VALUES
                """);
        for (int i = 0; i < n; i++) {
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

        Jpa.txVoid((EntityManager em) -> {
            Query q = em.createNativeQuery(sql);
            int p = 1;
            for (Detail d : detailBuf) {
                q.setParameter(p++, d.fid);
                q.setParameter(p++, d.key);
                q.setParameter(p++, d.tier);
                q.setParameter(p++, d.json);
            }
            q.executeUpdate();
        });

        detailBuf.clear();
    }

    private void flushMains() {
        if (mainBuf.isEmpty())
            return;

        final int n = mainBuf.size();
        final StringBuilder sb = new StringBuilder(256 + n * 24);
        sb.append("""
                    INSERT INTO public.tables_overlay(key, tier, rows, updated_at)
                    VALUES
                """);
        for (int i = 0; i < n; i++) {
            if (i > 0)
                sb.append(',');
            sb.append("(?,?,CAST(? AS jsonb), now())");
        }
        sb.append("""
                    ON CONFLICT (key, tier) DO UPDATE
                    SET rows = EXCLUDED.rows, updated_at = now()
                    WHERE public.tables_overlay.rows IS DISTINCT FROM EXCLUDED.rows
                """);
        final String sql = sb.toString();

        Jpa.txVoid((EntityManager em) -> {
            Query q = em.createNativeQuery(sql);
            int p = 1;
            for (MainRow m : mainBuf) {
                q.setParameter(p++, m.key);
                q.setParameter(p++, m.tier);
                q.setParameter(p++, m.json);
            }
            q.executeUpdate();
        });

        mainBuf.clear();
    }
}

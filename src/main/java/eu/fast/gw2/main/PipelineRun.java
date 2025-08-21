package eu.fast.gw2.main;

import java.util.ArrayList;
import java.util.List;
import java.util.Locale;

import eu.fast.gw2.dao.OverlayDao;
import eu.fast.gw2.dynamic.OverlayEngine;
import eu.fast.gw2.enums.Tier;
import eu.fast.gw2.google.GoogleSheetsFetcher;
import eu.fast.gw2.jpa.Jpa;

public class PipelineRun {

    private static final String SHEET_ID = "1WdwWxyP9zeJhcxoQAr-paMX47IuK6l5rqAPYDOA8mho";
    private static final int SLEEP_MS = 150;

    public static void main(String[] args) throws Exception {
        System.out.println(">>> PipelineRun (no flags) starting…");

        // 0) Refresh the most-frequent tier (5m) from GW2 API
        RefreshTierPrices.main(new String[] { "--tier=5m" });

        // 1) Fetch Sheets (named ranges) → upsert into public.tables /
        // public.detail_tables
        GoogleSheetsFetcher gs = new GoogleSheetsFetcher(SHEET_ID);
        // TODO: drive your concrete range-to-table update here (you said: always full
        // run; it’s fast)

        // 2) Seed + prune calculations
        SeedCalculationsFromDetailTable.main(new String[] { "--force", "--op=SUM", "--taxes=15" });
        SeedCalculationsFromDetailTable.main(new String[] { "--prune" });

        // 3) Recompute overlays for ALL tiers and store in overlay tables
        List<Object[]> detailTargets = selectDetailTargets();
        List<String> mainTargets = selectMainTableKeys();

        Tier[] tiers = { Tier.T5M, Tier.T10M, Tier.T15M, Tier.T60M };

        // detail_tables
        for (Tier t : tiers) {
            int ok = 0, fail = 0;
            System.out.println("Recompute detail_tables for tier " + t.name());
            for (Object[] row : detailTargets) {
                long fid = ((Number) row[0]).longValue();
                String key = (String) row[1];
                try {
                    String json = OverlayEngine.recomputeDetailJson(fid, key, t);
                    if (json != null) {
                        OverlayDao.upsertDetail(fid, key, t.label, json);
                    }
                    ok++;
                } catch (Exception e) {
                    fail++;
                    System.err
                            .println("! detail " + t.name() + " fid=" + fid + " key=" + key + " -> " + e.getMessage());
                }
                sleepQuiet(SLEEP_MS);
            }
            System.out.printf(Locale.ROOT, "  detail %s ok=%d fail=%d%n", t.name(), ok, fail);
        }

        // main tables
        for (Tier t : tiers) {
            int ok = 0, fail = 0;
            System.out.println("Recompute main tables for tier " + t.name());
            for (String key : mainTargets) {
                try {
                    String json = OverlayEngine.recomputeMainJson(key, t);
                    if (json != null) {
                        OverlayDao.upsertMain(key, t.label, json);
                    }
                    ok++;
                } catch (Exception e) {
                    fail++;
                    System.err.println("! main " + t.name() + " key=" + key + " -> " + e.getMessage());
                }
                sleepQuiet(SLEEP_MS);
            }
            System.out.printf(Locale.ROOT, "  main %s ok=%d fail=%d%n", t.name(), ok, fail);
        }

        System.out.println("PipelineRun done.");
    }

    @SuppressWarnings("unchecked")
    private static List<Object[]> selectDetailTargets() {
        return Jpa.tx(em -> em.createNativeQuery("""
                    SELECT detail_feature_id, key FROM public.detail_tables ORDER BY id DESC
                """).getResultList());
    }

    private static List<String> selectMainTableKeys() {
        return Jpa.tx(em -> {
            var r = em.createNativeQuery("""
                        SELECT key FROM public.tables ORDER BY id DESC
                    """).getResultList();
            List<String> out = new ArrayList<>(r.size());
            for (Object o : r)
                out.add((String) o);
            return out;
        });
    }

    private static void sleepQuiet(int ms) {
        try {
            Thread.sleep(ms);
        } catch (InterruptedException ignored) {
        }
    }
}

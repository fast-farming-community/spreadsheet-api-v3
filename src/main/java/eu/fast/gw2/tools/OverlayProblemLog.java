package eu.fast.gw2.tools;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public final class OverlayProblemLog {
    private int total = 0;
    private final Map<String, Integer> reasonCount = new HashMap<>();
    private final Map<String, Integer> catKeyCount = new HashMap<>();
    private final List<String> samples = new ArrayList<>();
    private final int maxSamples = 200;

    public synchronized void reset() {
        total = 0;
        reasonCount.clear();
        catKeyCount.clear();
        samples.clear();
    }

    public synchronized void record(boolean isMain, String tableKey, Long fid, int rowIndex,
            Map<String, Object> row, int taxesPct, String reason) {
        total++;
        reasonCount.merge(reason, 1, Integer::sum);
        String cat = OverlayHelper.str(row.get(OverlayHelper.COL_CAT));
        String key = OverlayHelper.str(row.get(OverlayHelper.COL_KEY));
        String catKey = ((cat == null ? "" : cat) + "|" + (key == null ? "" : key))
                .toUpperCase(java.util.Locale.ROOT);
        catKeyCount.merge(catKey, 1, Integer::sum);

        if (samples.size() < maxSamples) {
            int id = OverlayHelper.toInt(row.get(OverlayHelper.COL_ID), -1);
            String name = OverlayHelper.str(row.get(OverlayHelper.COL_NAME));

            Integer ib_tpb = OverlayHelper.toIntBoxed(row.get(OverlayHelper.COL_ITEM_BUY_TPBUY));
            Integer is_tpb = OverlayHelper.toIntBoxed(row.get(OverlayHelper.COL_ITEM_SELL_TPBUY));
            Integer ib_tps = OverlayHelper.toIntBoxed(row.get(OverlayHelper.COL_ITEM_BUY_TPSELL));
            Integer is_tps = OverlayHelper.toIntBoxed(row.get(OverlayHelper.COL_ITEM_SELL_TPSELL));

            Double avg = OverlayHelper.toDouble(row.get(OverlayHelper.COL_AVG), 0.0);

            samples.add(String.format(java.util.Locale.ROOT,
                    "{reason:%s, isMain:%s, table:'%s', fid:%s, row:%d, id:%d, name:'%s', cat:'%s', key:'%s', "
                            + "IB_TPB:%s, IS_TPB:%s, IB_TPS:%s, IS_TPS:%s, avg:%.3f, taxes:%d}",
                    reason, isMain, tableKey, (fid == null ? "null" : fid.toString()), rowIndex, id,
                    String.valueOf(name), String.valueOf(cat), String.valueOf(key),
                    String.valueOf(ib_tpb), String.valueOf(is_tpb),
                    String.valueOf(ib_tps), String.valueOf(is_tps),
                    avg, taxesPct));
        }
    }

    public synchronized void recordIfZero(boolean isMain, String tableKey, Long fid, int rowIndex,
            Map<String, Object> row, int taxesPct, String reason) {

        // Skip zero-problem logging for UNTOUCHED rows entirely.
        String cat = OverlayHelper.str(row.get(OverlayHelper.COL_CAT));
        if ("UNTOUCHED".equals(cat))
            return;

        int ib_tpb = OverlayHelper.toInt(row.get(OverlayHelper.COL_ITEM_BUY_TPBUY), 0);
        int is_tpb = OverlayHelper.toInt(row.get(OverlayHelper.COL_ITEM_SELL_TPBUY), 0);
        int ib_tps = OverlayHelper.toInt(row.get(OverlayHelper.COL_ITEM_BUY_TPSELL), 0);
        int is_tps = OverlayHelper.toInt(row.get(OverlayHelper.COL_ITEM_SELL_TPSELL), 0);

        if (ib_tpb == 0 && is_tpb == 0 && ib_tps == 0 && is_tps == 0) {
            record(isMain, tableKey, fid, rowIndex, row, taxesPct, reason);
        }
    }

    public synchronized void printSummary() {
        System.out.println("Total problem rows: " + total);
        for (var e : reasonCount.entrySet())
            System.out.println("  " + e.getKey() + ": " + e.getValue());
        if (!catKeyCount.isEmpty()) {
            System.out.println("-- top (Category|Key) --");
            List<Map.Entry<String, Integer>> list = new ArrayList<>(catKeyCount.entrySet());
            list.sort((a, b) -> Integer.compare(b.getValue(), a.getValue()));
            int lim = Math.min(15, list.size());
            for (int i = 0; i < lim; i++) {
                var e = list.get(i);
                System.out.println("  " + e.getKey() + " -> " + e.getValue());
            }
        }
        if (!samples.isEmpty()) {
            System.out.println("-- sample rows (" + samples.size() + ") --");
            for (String s : samples)
                System.out.println("  " + s);
        }
    }
}

package eu.fast.gw2.tools;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;

public final class OverlayProfiler {

    /** Controls per-table progress lines; summaries still respect `enabled`. */
    public static volatile boolean VERBOSE = false;

    private OverlayProfiler() {
    }

    /*
     * ========================= RUN (shared across all tiers)
     * =========================
     */

    public static final class Run {
        private final long runStartMs = System.currentTimeMillis();

        // run-wide counters
        private final AtomicLong tablesDetail = new AtomicLong();
        private final AtomicLong tablesMain = new AtomicLong();
        private final AtomicLong rowsDetail = new AtomicLong();
        private final AtomicLong rowsMain = new AtomicLong();
        private final AtomicLong fails = new AtomicLong(-1); // -1 = unknown / not set

        // problem log (merged)
        private final AtomicLong problemsTotal = new AtomicLong();
        private final ConcurrentHashMap<String, AtomicLong> reasonCount = new ConcurrentHashMap<>();
        private final ConcurrentHashMap<String, AtomicLong> catKeyCount = new ConcurrentHashMap<>();
        private final List<String> samples = Collections.synchronizedList(new ArrayList<>());
        private final int maxSamples;

        public Run() {
            this(200);
        }

        public Run(int maxSamples) {
            this.maxSamples = Math.max(0, maxSamples);
        }

        /* ---------- tier handle ---------- */

        public Tier newTier(String tierName, boolean enabled) {
            return new Tier(this, tierName, enabled);
        }

        /* ---------- run-wide accumulation from a tier ---------- */

        private void addFromTier(long td, long tm, long rd, long rm) {
            tablesDetail.addAndGet(td);
            tablesMain.addAndGet(tm);
            rowsDetail.addAndGet(rd);
            rowsMain.addAndGet(rm);
        }

        public void addFails(long n) {
            if (n <= 0)
                return;
            long v = fails.get();
            if (v < 0)
                v = 0;
            fails.set(v + n);
        }

        /* ---------- problem logging (was OverlayProblemLog) ---------- */

        public void recordProblem(boolean isMain, String tableKey, Long fid, int rowIndex,
                Map<String, Object> row, int taxesPct, String reason) {
            problemsTotal.incrementAndGet();
            reasonCount.computeIfAbsent(reason, k -> new AtomicLong()).incrementAndGet();

            String cat = OverlayHelper.str(row.get(OverlayHelper.COL_CAT));
            String key = OverlayHelper.str(row.get(OverlayHelper.COL_KEY));
            String catKey = ((cat == null ? "" : cat) + "|" + (key == null ? "" : key))
                    .toUpperCase(Locale.ROOT);
            catKeyCount.computeIfAbsent(catKey, k -> new AtomicLong()).incrementAndGet();

            if (samples.size() < maxSamples) {
                int id = OverlayHelper.toInt(row.get(OverlayHelper.COL_ID), -1);
                String name = OverlayHelper.str(row.get(OverlayHelper.COL_NAME));

                Integer ib_tpb = OverlayHelper.toIntBoxed(row.get(OverlayHelper.COL_ITEM_BUY_TPBUY));
                Integer is_tpb = OverlayHelper.toIntBoxed(row.get(OverlayHelper.COL_ITEM_SELL_TPBUY));
                Integer ib_tps = OverlayHelper.toIntBoxed(row.get(OverlayHelper.COL_ITEM_BUY_TPSELL));
                Integer is_tps = OverlayHelper.toIntBoxed(row.get(OverlayHelper.COL_ITEM_SELL_TPSELL));

                Double avg = OverlayHelper.toDouble(row.get(OverlayHelper.COL_AVG), 0.0);

                samples.add(String.format(Locale.ROOT,
                        "{reason:%s, isMain:%s, table:'%s', fid:%s, row:%d, id:%d, name:'%s', cat:'%s', key:'%s', "
                                + "IB_TPB:%s, IS_TPB:%s, IB_TPS:%s, IS_TPS:%s, avg:%.3f, taxes:%d}",
                        reason, isMain, tableKey, (fid == null ? "null" : fid.toString()), rowIndex, id,
                        String.valueOf(name), String.valueOf(cat), String.valueOf(key),
                        String.valueOf(ib_tpb), String.valueOf(is_tpb),
                        String.valueOf(ib_tps), String.valueOf(is_tps),
                        avg, taxesPct));
            }
        }

        public void recordProblemIfZero(boolean isMain, String tableKey, Long fid, int rowIndex,
                Map<String, Object> row, int taxesPct, String reason) {
            String cat = OverlayHelper.str(row.get(OverlayHelper.COL_CAT));
            if ("UNTOUCHED".equals(cat))
                return;

            int ib_tpb = OverlayHelper.toInt(row.get(OverlayHelper.COL_ITEM_BUY_TPBUY), 0);
            int is_tpb = OverlayHelper.toInt(row.get(OverlayHelper.COL_ITEM_SELL_TPBUY), 0);
            int ib_tps = OverlayHelper.toInt(row.get(OverlayHelper.COL_ITEM_BUY_TPSELL), 0);
            int is_tps = OverlayHelper.toInt(row.get(OverlayHelper.COL_ITEM_SELL_TPSELL), 0);

            if (ib_tpb == 0 && is_tpb == 0 && ib_tps == 0 && is_tps == 0) {
                recordProblem(isMain, tableKey, fid, rowIndex, row, taxesPct, reason);
            }
        }

        /* ---------- final print (combined tiers) ---------- */

        public void printRunSummaryAndProblems() {
            double secs = (System.currentTimeMillis() - runStartMs) / 1000.0;
            long td = tablesDetail.get();
            long tm = tablesMain.get();
            long rd = rowsDetail.get();
            long rm = rowsMain.get();
            long f = fails.get();

            System.out.printf(Locale.ROOT,
                    "Overlay RUN: tables(detail=%d, main=%d%s) rows(detail=%d, main=%d) finished in %.1fs%n",
                    td, tm, (f >= 0 ? ", fail=" + f : ""), rd, rm, secs);

            long total = problemsTotal.get();
            if (total <= 0)
                return; // quiet when no problems

            System.out.println("Total problem rows: " + total);

            if (!reasonCount.isEmpty()) {
                reasonCount.entrySet().stream()
                        .sorted((a, b) -> Long.compare(b.getValue().get(), a.getValue().get()))
                        .forEach(e -> System.out.println("  " + e.getKey() + ": " + e.getValue().get()));
            }

            if (!catKeyCount.isEmpty()) {
                System.out.println("-- top (Category|Key) --");
                catKeyCount.entrySet().stream()
                        .sorted((a, b) -> Long.compare(b.getValue().get(), a.getValue().get()))
                        .limit(15)
                        .forEach(e -> System.out.println("  " + e.getKey() + " -> " + e.getValue().get()));
            }

            if (!samples.isEmpty()) {
                System.out.println("-- sample rows (" + samples.size() + ") --");
                synchronized (samples) {
                    for (String s : samples)
                        System.out.println("  " + s);
                }
            }
        }

        /* package */ void onTierFinished(Tier t) {
            addFromTier(t.tablesDetail, t.tablesMain, t.rowsDetail, t.rowsMain);
        }
    }

    /*
     * ========================= TIER (given to each runner)
     * =========================
     */

    public static final class Tier {
        private final Run run;
        private final String tierName;
        private final boolean enabled;

        // per-tier counters (accumulated into run on finish)
        public long tablesDetail, tablesMain;
        public long rowsDetail, rowsMain;
        public long belowCutoff;
        public long fastComposite, fastItem;

        private Tier(Run run, String tierName, boolean enabled) {
            this.run = run;
            this.tierName = tierName;
            this.enabled = enabled;
        }

        public void tableBegin(String name, boolean isMain, int rowsCount, int index1Based, int totalCount) {
            if (!enabled || !VERBOSE)
                return;
            System.out.println(String.format(
                    Locale.ROOT,
                    "[%d/%d] Overlay %s %s table='%s' rows=%d",
                    index1Based, totalCount, tierName, (isMain ? "MAIN" : "DETAIL"), name, rowsCount));
        }

        /** Call once per tier at the end of the runner. */
        public void finish() {
            run.onTierFinished(this);
            // suppress per-tier summary by design (combined summary is printed by Run)
        }

        /* Convenience to bump counters from runner */
        public void incTables(boolean isMain) {
            if (isMain)
                tablesMain++;
            else
                tablesDetail++;
        }

        public void addRows(boolean isMain, long n) {
            if (isMain)
                rowsMain += n;
            else
                rowsDetail += n;
        }
    }
}

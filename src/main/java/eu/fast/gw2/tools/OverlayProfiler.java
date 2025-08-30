package eu.fast.gw2.tools;

public final class OverlayProfiler {
    public long tablesDetail, tablesMain;
    public long rowsDetail, rowsMain;
    public long belowCutoff;
    public long fastComposite, fastItem;

    /** Controls per-table progress lines; summaries still respect `enabled`. */
    public static volatile boolean VERBOSE = false;

    // scratch
    private String tierName;
    private boolean enabled;

    public OverlayProfiler(String tierName, boolean enabled) {
        this.tierName = tierName;
        this.enabled = enabled;
    }

    public void tableBegin(String name, boolean isMain, int rowsCount, int index1Based, int totalCount) {
        if (!enabled || !VERBOSE)
            return;
        System.out.println(String.format(
                java.util.Locale.ROOT,
                "[%d/%d] Overlay %s %s table='%s' rows=%d",
                index1Based, totalCount, tierName, (isMain ? "MAIN" : "DETAIL"), name, rowsCount));
    }

    public void printSummary(String tierName) {
        if (!enabled)
            return;
        System.out.printf(java.util.Locale.ROOT,
                "Overlay TIER %s SUMMARY: tables(detail=%d, main=%d) rows(detail=%d, main=%d) belowCutoff=%d fast(composite=%d,item=%d)%n",
                tierName, tablesDetail, tablesMain, rowsDetail, rowsMain,
                belowCutoff, fastComposite, fastItem);
    }
}

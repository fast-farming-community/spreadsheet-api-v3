// eu.fast.gw2.util.DebugTrace
package eu.fast.gw2.main;

import java.util.regex.Pattern;

public final class DebugTrace {
    private static boolean enabled = false;
    private static int maxRows = 200;
    private static Pattern nameFilter = null;

    public static void enable(boolean on) {
        enabled = on;
    }

    public static boolean on() {
        return enabled;
    }

    public static void limit(int n) {
        maxRows = Math.max(1, n);
    }

    public static int limit() {
        return maxRows;
    }

    public static void filter(String regex) {
        nameFilter = (regex == null || regex.isBlank()) ? null : Pattern.compile(regex, Pattern.CASE_INSENSITIVE);
    }

    public static boolean allow(Integer id, String name) {
        if (nameFilter == null)
            return true;
        String s = (name == null ? "" : name);
        return nameFilter.matcher(s).find();
    }

    public static void rowHeader(long fid, String key, int total, int tpRows) {
        System.out.printf("? Overlay Debug ?%nTable: fid=%d key=%s%nRows total: %d | rows with TP fields: %d%n",
                fid, key, total, tpRows);
    }

    public static void rowLine(int idx, Integer id, String name, String cat, String rowKey,
            Double avg, Double total, Long inBuy, Long inSell,
            String source, Integer priceBuy, Integer priceSell, int taxPct,
            Long outBuy, Long outSell) {
        System.out.printf(
                "#%03d id=%s name=%s cat=%s key=%s avg=%.4f total=%.4f in[%s/%s] src=%s raw[%s/%s] tax=%d -> out[%s/%s]%n",
                idx,
                id == null ? "-" : id.toString(),
                name, cat, rowKey,
                avg == null ? 0.0 : avg,
                total == null ? 0.0 : total,
                inBuy == null ? "-" : inBuy.toString(),
                inSell == null ? "-" : inSell.toString(),
                source,
                priceBuy == null ? "-" : priceBuy.toString(),
                priceSell == null ? "-" : priceSell.toString(),
                taxPct,
                outBuy == null ? "-" : outBuy.toString(),
                outSell == null ? "-" : outSell.toString());
    }

    public static void refSummary(String key, String op, int taxPct, long sumBuy, long sumSell, int rowCount) {
        System.out.printf("  [ref] key=%s op=%s tax=%d rows=%d -> sumBuy=%d sumSell=%d%n",
                key, op, taxPct, rowCount, sumBuy, sumSell);
    }

    public static void totalLine(long sumBuy, long sumSell) {
        System.out.printf("? end ?%nSum(TPBuyProfit)=%d  Sum(TPSellProfit)=%d%n", sumBuy, sumSell);
    }
}

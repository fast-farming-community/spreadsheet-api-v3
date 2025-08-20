package eu.fast.gw2.main;

import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

public final class DebugTrace {
    private static volatile boolean enabled = "1".equalsIgnoreCase(System.getenv().getOrDefault("TRACE_OVERLAY", "0"))
            || "true".equalsIgnoreCase(System.getenv().getOrDefault("TRACE_OVERLAY", "false"));

    private static final int LIMIT = parseInt(System.getenv("TRACE_LIMIT"), 50);
    private static final Set<Integer> IDS = parseIds(System.getenv("TRACE_IDS"));
    private static final String MATCH = safeLower(System.getenv("TRACE_MATCH"));

    private DebugTrace() {
    }

    public static boolean on() {
        return enabled;
    }

    public static int limit() {
        return LIMIT;
    }

    public static boolean allow(Integer id, String name) {
        if (!enabled)
            return false;
        if (!IDS.isEmpty() && (id == null || !IDS.contains(id)))
            return false;
        if (!MATCH.isEmpty()) {
            var n = name == null ? "" : name;
            if (!n.toLowerCase().contains(MATCH))
                return false;
        }
        return true;
    }

    public static void rowHeader(long fid, String key, int totalRows, int tpRows) {
        if (!enabled)
            return;
        System.out.println("— TRACE rows for fid=" + fid + " key=" + key +
                " | total=" + totalRows + " | withTP=" + tpRows + " —");
    }

    public static void rowLine(int idx, Integer id, String name,
            String category, String rkey,
            Number avg, Number totalAmount,
            Number tpbInput, Number tpsInput,
            String source, Integer priceBuy, Integer priceSell,
            int taxPct, Long tpbOut, Long tpsOut) {
        if (!enabled)
            return;
        System.out.printf(
                "#%03d id=%s name=%s | cat=%s key=%s | avg=%.4f total=%s | in(TPB=%s TPS=%s) | src=%s buy=%s sell=%s | tax=%d%% | out(TPB=%s TPS=%s)%n",
                idx,
                id == null ? "-" : id.toString(),
                trim(name, 60),
                dash(category),
                dash(rkey),
                asDouble(avg),
                dash(totalAmount),
                dash(tpbInput),
                dash(tpsInput),
                dash(source),
                dash(priceBuy),
                dash(priceSell),
                taxPct,
                dash(tpbOut),
                dash(tpsOut));
    }

    public static void totalLine(Long sumBuy, Long sumSell) {
        if (!enabled)
            return;
        System.out.println("= SUM => TPB=" + sumBuy + " TPS=" + sumSell);
    }

    public static void refSummary(String refKey, String op, int taxPct, long tpb, long tps, int rows) {
        if (!enabled)
            return;
        System.out.println("   ↳ via table=" + refKey + " op=" + op + " tax=" + taxPct +
                " | refSum(TPB=" + tpb + " TPS=" + tps + ") rows=" + rows);
    }

    // helpers
    private static double asDouble(Number n) {
        return n == null ? 0.0 : n.doubleValue();
    }

    private static String dash(Object o) {
        return o == null ? "-" : String.valueOf(o);
    }

    private static String trim(String s, int n) {
        if (s == null)
            return "-";
        return s.length() <= n ? s : s.substring(0, n - 1) + "…";
    }

    private static int parseInt(String s, int def) {
        try {
            return s == null ? def : Integer.parseInt(s);
        } catch (Exception e) {
            return def;
        }
    }

    private static String safeLower(String s) {
        return s == null ? "" : s.toLowerCase();
    }

    private static Set<Integer> parseIds(String s) {
        if (s == null || s.isBlank())
            return Set.of();
        return List.of(s.split(",")).stream().map(String::trim).filter(t -> !t.isEmpty()).map(Integer::valueOf)
                .collect(Collectors.toSet());
    }
}

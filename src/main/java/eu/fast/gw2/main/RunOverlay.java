package eu.fast.gw2.main;

public class RunOverlay {
    public static void main(String[] args) {
        long fid = Long.parseLong(arg(args, "--fid=", "12"));
        String key = arg(args, "--key=", "branded-geodermite");
        String tierArg = arg(args, "--tier=", "60m").toLowerCase(java.util.Locale.ROOT);
        boolean persist = Boolean.parseBoolean(arg(args, "--persist=", "false"));
        boolean trace = Boolean.parseBoolean(arg(args, "--trace=", "false")); // <— NEW

        // allow env fallback too
        if (!trace)
            trace = "1".equals(System.getenv("OVERLAY_TRACE"));

        // enable trace globally
        DebugTrace.enable(trace);
        DebugTrace.limit(200); // print up to 200 rows by default

        var tier = eu.fast.gw2.enums.Tier.parse(tierArg);
        System.out.printf(">>> RunOverlay fid=%d key=%s tier=%s persist=%s trace=%s%n",
                fid, key, tier.name(), persist, trace);

        eu.fast.gw2.tools.OverlayEngine.recompute(fid, key, tier, persist);
        System.out.println("Recompute done.");
    }

    private static String arg(String[] args, String key, String def) {
        for (String a : args)
            if (a.startsWith(key))
                return a.substring(key.length());
        return def;
    }
}

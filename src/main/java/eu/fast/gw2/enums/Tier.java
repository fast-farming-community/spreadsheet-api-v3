package eu.fast.gw2.enums;

public enum Tier {
    T2M("2m", "buy_2m", "sell_2m", "ts_2m"),
    T10M("10m", "buy_10m", "sell_10m", "ts_10m"),
    T60M("60m", "buy_60m", "sell_60m", "ts_60m");

    public final String label;
    public final String colBuy;
    public final String colSell;
    public final String colTs;

    Tier(String label, String b, String s, String t) {
        this.label = label;
        this.colBuy = b;
        this.colSell = s;
        this.colTs = t;
    }

    public static Tier parse(String s) {
        if (s == null)
            return T60M;
        switch (s.trim().toLowerCase()) {
            case "2m":
                return T2M;
            case "10m":
                return T10M;
            case "60m":
            default:
                return T60M;
        }
    }

    public String columnKey() {
        return switch (this) {
            case T2M -> "2m";
            case T10M -> "10m";
            case T60M -> "60m";
        };
    }
}

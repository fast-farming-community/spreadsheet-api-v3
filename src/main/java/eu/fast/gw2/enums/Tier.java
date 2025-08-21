package eu.fast.gw2.enums;

public enum Tier {
    T5M("buy_5m", "sell_5m", "ts_5m"),
    T10M("buy_10m", "sell_10m", "ts_10m"),
    T20M("buy_20m", "sell_20m", "ts_20m"),
    T30M("buy_30m", "sell_30m", "ts_30m"),
    T60M("buy_60m", "sell_60m", "ts_60m");

    public final String colBuy;
    public final String colSell;
    public final String colTs;

    Tier(String b, String s, String t) {
        this.colBuy = b;
        this.colSell = s;
        this.colTs = t;
    }

    public static Tier parse(String s) {
        if (s == null)
            return T60M;
        switch (s.trim().toLowerCase()) {
            case "5m":
                return T5M;
            case "10m":
                return T10M;
            case "20m":
                return T20M;
            case "30m":
                return T30M;
            case "60m":
            default:
                return T60M;
        }
    }
}

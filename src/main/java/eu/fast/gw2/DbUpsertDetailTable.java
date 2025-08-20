package eu.fast.gw2;

import java.sql.DriverManager;

public class DbUpsertDetailTable {
    public static void main(String[] args) throws Exception {
        var url = System.getenv("PG_URL");
        var user = System.getenv("PG_USER");
        var pass = System.getenv("PG_PASS");

        // config (adjust these 4 to your case)
        String featureName = "Sheets v3";
        String tableKey = "fast_materials"; // unique within this feature
        String tableName = "Fast Materials";
        String tableRange = "Materials!A2:K";

        // a tiny dummy payload (TEXT) — same shape as your real “rows” JSON array
        String rowsJson = """
                [
                  {"Id":1,"Name":"Coin","AverageAmount":192.4836409395973,"TPBuyProfit":192.4836409395973,"TPSellProfit":192.4836409395973},
                  {"Id":39223,"Name":"Unidentifiable Object","AverageAmount":0.01761744966442953,"TPBuyProfit":0.8808724832214765,"TPSellProfit":0.8808724832214765}
                ]
                """;

        try (var conn = DriverManager.getConnection(url, user, pass)) {
            conn.setAutoCommit(false);

            // resolve detail_feature_id by name
            Long featureId = null;
            try (var ps = conn.prepareStatement("SELECT id FROM detail_features WHERE name = ?")) {
                ps.setString(1, featureName);
                try (var rs = ps.executeQuery()) {
                    if (rs.next())
                        featureId = rs.getLong(1);
                }
            }
            if (featureId == null)
                throw new IllegalStateException("detail_feature not found: " + featureName);

            // upsert into detail_tables (TEXT rows, timestamps without tz)
            String sql = """
                    INSERT INTO detail_tables (detail_feature_id, key, name, range, rows, inserted_at, updated_at)
                    VALUES (?, ?, ?, ?, ?, now(), now())
                    ON CONFLICT (detail_feature_id, key) DO UPDATE SET
                      name = EXCLUDED.name,
                      range = EXCLUDED.range,
                      rows = EXCLUDED.rows,
                      updated_at = now()
                    """;
            try (var ps = conn.prepareStatement(sql)) {
                ps.setLong(1, featureId);
                ps.setString(2, tableKey);
                ps.setString(3, tableName);
                ps.setString(4, tableRange);
                ps.setString(5, rowsJson); // TEXT
                ps.executeUpdate();
            }

            conn.commit();
            System.out.println("Upserted detail_tables for key=" + tableKey + " (feature_id=" + featureId + ")");
        }
    }
}

package eu.fast.gw2;

import java.sql.DriverManager;

public class DbEnsureFeature {
  public static void main(String[] args) throws Exception {
    var url  = System.getenv("PG_URL");
    var user = System.getenv("PG_USER");
    var pass = System.getenv("PG_PASS");
    var featureName = (args.length > 0 ? args[0] : "Sheets v3");
    try (var conn = DriverManager.getConnection(url, user, pass)) {
      conn.setAutoCommit(false);
      Long id = null;
      try (var ps = conn.prepareStatement("SELECT id FROM detail_features WHERE name = ?")) {
        ps.setString(1, featureName);
        try (var rs = ps.executeQuery()) {
          if (rs.next()) id = rs.getLong(1);
        }
      }
      if (id == null) {
        try (var ps = conn.prepareStatement(
            "INSERT INTO detail_features(name, published, inserted_at, updated_at) " +
            "VALUES (?, false, now(), now()) RETURNING id")) {
          ps.setString(1, featureName);
          try (var rs = ps.executeQuery()) { rs.next(); id = rs.getLong(1); }
        }
        System.out.println("Created feature '"+featureName+"' id=" + id);
      } else {
        System.out.println("Feature exists '"+featureName+"' id=" + id);
      }
      conn.commit();
    }
  }
}

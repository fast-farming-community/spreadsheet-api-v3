package eu.fast.gw2;

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;
import java.sql.DriverManager;
import java.util.stream.Collectors;

public class DbMigrate {
  public static void main(String[] args) throws Exception {
    String url  = System.getenv("PG_URL");
    String user = System.getenv("PG_USER");
    String pass = System.getenv("PG_PASS");
    if (url==null || user==null || pass==null) {
      System.err.println("Missing PG_URL/PG_USER/PG_PASS");
      System.exit(2);
    }
    String sql = new BufferedReader(new InputStreamReader(
        DbMigrate.class.getResourceAsStream("/schema.sql"), StandardCharsets.UTF_8))
        .lines().collect(Collectors.joining("\n"));

    try (var conn = DriverManager.getConnection(url, user, pass);
         var st = conn.createStatement()) {
      // simple splitter: execute on each semicolon; ignore empty
      for (String stmt : sql.split(";")) {
        String s = stmt.trim();
        if (!s.isEmpty()) {
          st.execute(s);
        }
      }
      System.out.println("Schema applied OK.");
    }
  }
}

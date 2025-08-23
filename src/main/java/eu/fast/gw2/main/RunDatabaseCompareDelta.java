package eu.fast.gw2.main;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.Locale;
import java.util.Map;
import java.util.Objects;
import java.util.Set;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;

public class RunDatabaseCompareDelta {
    private static final ObjectMapper M = new ObjectMapper();
    // 90% tolerance for numeric comparisons
    private static final double REL_TOL = 0.90;

    public static void main(String[] args) throws Exception {
        String urlTest = "jdbc:postgresql://localhost:5433/fast_test";
        String urlProd = "jdbc:postgresql://localhost:5433/fast";
        String user = System.getenv("PG_USER");
        String pass = System.getenv("PG_PASS");

        try (Connection cTest = DriverManager.getConnection(urlTest, user, pass);
                Connection cProd = DriverManager.getConnection(urlProd, user, pass)) {

            System.out.println(">>> Checking tables");
            compare(cProd, cTest,
                    "SELECT page_id::text || ':' || COALESCE(name,'') AS k, rows FROM public.tables",
                    rs -> rs.getString("k"));

            System.out.println(">>> Checking detail_tables");
            compare(cProd, cTest,
                    "SELECT detail_feature_id || ':' || key AS k, rows FROM public.detail_tables",
                    (rs) -> rs.getString("k"));
        }
    }

    interface KeyFn {
        String key(ResultSet rs) throws SQLException;
    }

    private static Map<String, JsonNode> load(Connection c, String sql, KeyFn keyFn) throws Exception {
        Map<String, JsonNode> out = new LinkedHashMap<>();
        try (Statement st = c.createStatement(); ResultSet rs = st.executeQuery(sql)) {
            while (rs.next()) {
                String k = keyFn.key(rs);
                String j = rs.getString("rows");
                if (j != null) {
                    try {
                        out.put(k, M.readTree(j));
                    } catch (Exception e) {
                        System.err.println("bad JSON at " + k);
                    }
                }
            }
        }
        return out;
    }

    private static void compare(Connection prod, Connection test, String sql, KeyFn keyFn) throws Exception {
        Map<String, JsonNode> p = load(prod, sql, keyFn);
        Map<String, JsonNode> t = load(test, sql, keyFn);

        int ok = 0, diff = 0, missProd = 0, missTest = 0;

        // Track distinct ignored headers we encountered (normalized)
        Set<String> ignoredHeadersSeen = new LinkedHashSet<>();

        Set<String> all = new HashSet<>();
        all.addAll(p.keySet());
        all.addAll(t.keySet());

        for (String k : all) {
            JsonNode jp = p.get(k), jt = t.get(k);
            if (jp == null) {
                missProd++;
                continue;
            }
            if (jt == null) {
                missTest++;
                continue;
            }

            boolean keyHasDiff = false;

            if (jp.isArray() && jt.isArray()) {
                int max = Math.max(jp.size(), jt.size());
                for (int i = 0; i < max; i++) {
                    JsonNode a = i < jp.size() ? jp.get(i) : null;
                    JsonNode b = i < jt.size() ? jt.get(i) : null;

                    if (rowsEqualAfterIgnore(a, b, ignoredHeadersSeen)) {
                        continue;
                    }

                    if (!keyHasDiff) {
                        System.out.println("Δ key=" + k);
                        keyHasDiff = true;
                    }

                    if (a != null && b != null && a.isObject() && b.isObject()) {
                        Set<String> fields = new LinkedHashSet<>();
                        a.fieldNames().forEachRemaining(fields::add);
                        b.fieldNames().forEachRemaining(fields::add);

                        String id = a.has("Id") ? a.get("Id").asText()
                                : (b.has("Id") ? b.get("Id").asText() : "?");
                        String name = a.has("Name") ? a.get("Name").asText()
                                : (b.has("Name") ? b.get("Name").asText() : "?");

                        for (String f : fields) {
                            if (markIfIgnored(f, ignoredHeadersSeen))
                                continue;

                            JsonNode va = a.get(f);
                            JsonNode vb = b.get(f);

                            if (equalsWithTolerance(va, vb))
                                continue;

                            System.out.printf(
                                    "  line %d (Id=%s, Name=\"%s\") field %s%n    prod=%s%n    test=%s%n",
                                    i + 1, id, name, f, normalize(va), normalize(vb));
                        }
                    } else {
                        // at least one row is non-object; show when not equal within tolerance
                        if (!equalsWithTolerance(a, b)) {
                            System.out.printf("  line %d differs (non-object rows)%n", i + 1);
                        }
                    }
                }
            } else {
                if (!rowsEqualAfterIgnore(jp, jt, ignoredHeadersSeen)) {
                    System.out.println("Δ key=" + k);
                    System.out.println("  prod=" + M.writeValueAsString(jp));
                    System.out.println("  test=" + M.writeValueAsString(jt));
                    keyHasDiff = true;
                }
            }

            if (keyHasDiff)
                diff++;
            else
                ok++;
        }

        // Print summary with ignored header stats
        System.out.printf("Ignored headers matched (distinct): %d%n", ignoredHeadersSeen.size());
        if (!ignoredHeadersSeen.isEmpty()) {
            int shown = 0;
            StringBuilder sb = new StringBuilder("  e.g.: ");
            for (String h : ignoredHeadersSeen) {
                if (shown++ == 0)
                    sb.append(h);
                else
                    sb.append(", ").append(h);
                if (shown >= 15) {
                    sb.append(", …");
                    break;
                }
            }
            System.out.println(sb.toString());
        }
        System.out.printf("Summary: ok=%d diff=%d missingInTest=%d missingInProd=%d%n",
                ok, diff, missTest, missProd);
    }

    private static boolean rowsEqualAfterIgnore(JsonNode a, JsonNode b, Set<String> ignoredHeadersSeen) {
        if (a == null || a.isNull()) {
            if (b == null || b.isNull())
                return true;
            if (b.isObject())
                return objectIsEffectivelyEmpty(b, ignoredHeadersSeen);
            return normalize(b).isEmpty();
        }
        if (b == null || b.isNull()) {
            if (a.isObject())
                return objectIsEffectivelyEmpty(a, ignoredHeadersSeen);
            return normalize(a).isEmpty();
        }

        if (a.isObject() && b.isObject()) {
            Set<String> fields = new LinkedHashSet<>();
            a.fieldNames().forEachRemaining(fields::add);
            b.fieldNames().forEachRemaining(fields::add);

            for (String f : fields) {
                if (markIfIgnored(f, ignoredHeadersSeen))
                    continue;
                if (!equalsWithTolerance(a.get(f), b.get(f)))
                    return false;
            }
            return true;
        }

        // non-objects: compare with numeric tolerance if applicable
        return equalsWithTolerance(a, b);
    }

    private static boolean objectIsEffectivelyEmpty(JsonNode o, Set<String> ignoredHeadersSeen) {
        if (o == null || !o.isObject())
            return normalize(o).isEmpty();
        var it = o.fieldNames();
        while (it.hasNext()) {
            String f = it.next();
            if (markIfIgnored(f, ignoredHeadersSeen))
                continue;
            if (!equalsWithTolerance(o.get(f), null)) // null normalized == empty
                return false;
        }
        return true;
    }

    private static boolean equalsWithTolerance(JsonNode a, JsonNode b) {
        // Handle both-null / empty strings cases
        String sa = normalize(a);
        String sb = normalize(b);

        // Try numeric compare with relative tolerance if both sides are numeric
        Double da = asDouble(a);
        Double db = asDouble(b);
        if (da != null && db != null) {
            if (Double.compare(da, db) == 0)
                return true;
            double ad = Math.abs(da);
            double bd = Math.abs(db);
            double denom = Math.max(ad, bd);
            if (denom == 0.0)
                return true; // both zero (already caught above, but safe)
            double rel = Math.abs(da - db) / denom;
            return rel <= REL_TOL;
        }

        // Fallback to string equality
        return Objects.equals(sa, sb);
    }

    private static Double asDouble(JsonNode n) {
        if (n == null || n.isNull())
            return null;
        if (n.isNumber())
            return n.asDouble();
        String s = normalize(n);
        if (s.isEmpty())
            return null;
        // allow comma as decimal separator
        s = s.replace(',', '.');
        try {
            return Double.parseDouble(s);
        } catch (Exception ignored) {
            return null;
        }
    }

    private static String normalize(JsonNode n) {
        if (n == null || n.isNull())
            return "";
        String s = n.asText().trim();
        if (s.equalsIgnoreCase("null"))
            return "";
        return s;
    }

    // Record ignored headers and return true if header should be ignored.
    // NOTE: TP* is NOT ignored anymore.
    private static boolean markIfIgnored(String field, Set<String> ignoredHeadersSeen) {
        if (field == null)
            return false;
        String norm = normalizeHeaderName(field);
        boolean ignore = norm.startsWith("item")
                || norm.startsWith("bestchoice");
        if (ignore)
            ignoredHeadersSeen.add(norm);
        return ignore;
    }

    private static String normalizeHeaderName(String field) {
        return field.trim().replaceAll("[^A-Za-z0-9]", "").toLowerCase(Locale.ROOT);
    }
}

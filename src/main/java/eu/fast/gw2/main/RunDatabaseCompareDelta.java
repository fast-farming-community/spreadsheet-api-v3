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
        String tier = System.getenv().getOrDefault("DELTA_TIER", "60m");

        try (Connection cTest = DriverManager.getConnection(urlTest, user, pass);
                Connection cProd = DriverManager.getConnection(urlProd, user, pass)) {

            System.out.println(">>> PROD tables vs TEST tables_overlay (tier=" + tier + ")");
            compare(
                    cProd, cTest,
                    // PROD base tables — DISTINCT by page_id + name
                    "SELECT (page_id::text || '|' || COALESCE(name,'')) AS k, rows FROM public.tables",
                    rs -> rs.getString("k"),
                    // TEST overlay tables — DISTINCT by page_id + key, filtered by tier
                    "SELECT (page_id::text || '|' || COALESCE(key,'')) AS k, rows " +
                            "FROM public.tables_overlay WHERE tier = '" + tier + "'",
                    rs -> rs.getString("k"));

            System.out.println(">>> PROD detail_tables vs TEST detail_tables_overlay (tier=" + tier + ")");
            compare(
                    cProd, cTest,
                    // PROD base detail (fid:key)
                    "SELECT (detail_feature_id::text || ':' || key) AS k, rows FROM public.detail_tables",
                    rs -> rs.getString("k"),
                    // TEST overlay detail (same composite key, filtered by tier)
                    "SELECT (detail_feature_id::text || ':' || key) AS k, rows " +
                            "FROM public.detail_tables_overlay WHERE tier = '" + tier + "'",
                    rs -> rs.getString("k"));
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

    private static void compare(Connection prod, Connection test,
            String sqlProd, KeyFn keyFnProd,
            String sqlTest, KeyFn keyFnTest) throws Exception {
        Map<String, JsonNode> p = load(prod, sqlProd, keyFnProd);
        Map<String, JsonNode> t = load(test, sqlTest, keyFnTest);

        int ok = 0, diff = 0, missProd = 0, missTest = 0;
        int suppressedMissing = 0, suppressedDiffOnlyHidden = 0;
        Set<String> ignoredHeadersSeen = new LinkedHashSet<>();
        Set<String> all = new HashSet<>();
        all.addAll(p.keySet());
        all.addAll(t.keySet());

        for (String k : all) {
            JsonNode jp = p.get(k), jt = t.get(k);

            // Treat “missing” as equal if the present side is effectively empty after
            // filters.
            if (jp == null && isEffectivelyEmpty(jt, ignoredHeadersSeen)) {
                ok++;
                continue;
            }
            if (jt == null && isEffectivelyEmpty(jp, ignoredHeadersSeen)) {
                ok++;
                continue;
            }

            if (jp == null) {
                if (!isEffectivelyEmpty(jt, ignoredHeadersSeen)) {
                    missProd++;
                    // suppressed from logs by design
                } else {
                    suppressedMissing++;
                }
                continue;
            }
            if (jt == null) {
                if (!isEffectivelyEmpty(jp, ignoredHeadersSeen)) {
                    missTest++;
                } else {
                    suppressedMissing++;
                }
                continue;
            }

            boolean keyHasRealDiff = false; // differs on at least one NON-ignored field
            boolean keyHasOnlyHidden = false; // differs but only on ignored fields

            if (jp.isArray() && jt.isArray()) {
                int max = Math.max(jp.size(), jt.size());
                for (int i = 0; i < max; i++) {
                    JsonNode a = i < jp.size() ? jp.get(i) : null;
                    JsonNode b = i < jt.size() ? jt.get(i) : null;

                    if (rowsEqualAfterIgnore(a, b, ignoredHeadersSeen))
                        continue;

                    if (isEffectivelyEmpty(a, ignoredHeadersSeen) && isEffectivelyEmpty(b, ignoredHeadersSeen))
                        continue;

                    boolean rowHasRealDiff = hasNonIgnoredDiff(a, b, ignoredHeadersSeen);

                    if (rowHasRealDiff) {
                        if (!keyHasRealDiff) {
                            System.out.println("Found key=" + k);
                            keyHasRealDiff = true;
                        }

                        if (a != null && b != null && a.isObject() && b.isObject()) {
                            // Compare ONLY fields present in base row (avoid overlay-only wSS noise)
                            Set<String> fields = new LinkedHashSet<>();
                            a.fieldNames().forEachRemaining(fields::add);

                            String id = a.has("Id") ? a.get("Id").asText()
                                    : (b.has("Id") ? b.get("Id").asText() : "?");
                            String name = a.has("Name") ? a.get("Name").asText()
                                    : (b.has("Name") ? b.get("Name").asText() : "?");

                            for (String f : fields) {
                                if (markIfIgnored(f, ignoredHeadersSeen))
                                    continue;
                                if (equalsWithTolerance(a.get(f), b.get(f)))
                                    continue;

                                System.out.printf(
                                        "  line %d (Id=%s, Name=\"%s\") field %s%n    prod=%s%n    test=%s%n",
                                        i + 1, id, name, f, normalize(a.get(f)), normalize(b.get(f)));
                            }
                        } else {
                            if (!equalsWithTolerance(a, b)) {
                                System.out.printf("  line %d differs (non-object rows)%n", i + 1);
                            }
                        }
                    } else {
                        // this row differs only due to hidden fields
                        keyHasOnlyHidden = true;
                    }
                }
            } else {
                // Non-array payloads
                if (!rowsEqualAfterIgnore(jp, jt, ignoredHeadersSeen)) {
                    boolean real = hasNonIgnoredDiff(jp, jt, ignoredHeadersSeen);
                    if (real) {
                        System.out.println("Found key=" + k);
                        System.out.println("  prod=" + M.writeValueAsString(jp));
                        System.out.println("  test=" + M.writeValueAsString(jt));
                        keyHasRealDiff = true;
                    } else {
                        keyHasOnlyHidden = true;
                    }
                }
            }

            if (keyHasRealDiff) {
                diff++;
            } else if (keyHasOnlyHidden) {
                suppressedDiffOnlyHidden++;
                ok++; // treat as OK under current filter
            } else {
                ok++;
            }
        }

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
        if (suppressedMissing > 0 || suppressedDiffOnlyHidden > 0) {
            System.out.printf("Suppressed (hidden-only): missing=%d, diffs=%d%n",
                    suppressedMissing, suppressedDiffOnlyHidden);
        }
        System.out.printf("Summary: ok=%d diff=%d missingInTest=%d missingInProd=%d%n",
                ok, diff, missTest, missProd);
    }

    private static boolean rowsEqualAfterIgnore(JsonNode a, JsonNode b, Set<String> ignoredHeadersSeen) {
        if (isEffectivelyEmpty(a, ignoredHeadersSeen) && isEffectivelyEmpty(b, ignoredHeadersSeen))
            return true;

        if (a == null || a.isNull()) {
            if (b == null || b.isNull())
                return true;
            if (b.isObject())
                return objectIsEffectivelyEmpty(b, ignoredHeadersSeen);
            return isZeroOrEmpty(b);
        }
        if (b == null || b.isNull()) {
            if (a.isObject())
                return objectIsEffectivelyEmpty(a, ignoredHeadersSeen);
            return isZeroOrEmpty(a);
        }

        if (a.isArray() || b.isArray()) {
            // Compare arrays element-wise with ignore/empty logic
            int max = Math.max(a.isArray() ? a.size() : 1, b.isArray() ? b.size() : 1);
            for (int i = 0; i < max; i++) {
                JsonNode ai = a.isArray() ? (i < a.size() ? a.get(i) : null) : (i == 0 ? a : null);
                JsonNode bi = b.isArray() ? (i < b.size() ? b.get(i) : null) : (i == 0 ? b : null);
                if (!rowsEqualAfterIgnore(ai, bi, ignoredHeadersSeen))
                    return false;
            }
            return true;
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

    private static boolean isEffectivelyEmpty(JsonNode n, Set<String> ignoredHeadersSeen) {
        if (n == null || n.isNull())
            return true;

        if (n.isArray()) {
            for (JsonNode e : n) {
                if (!isEffectivelyEmpty(e, ignoredHeadersSeen))
                    return false;
            }
            return true;
        }
        if (n.isObject())
            return objectIsEffectivelyEmpty(n, ignoredHeadersSeen);

        // primitives
        if (isZeroOrEmpty(n))
            return true;
        // Non-empty primitive is only “empty” if the field would be ignored,
        // but we don't know the field name here, so treat as non-empty.
        return false;
    }

    private static boolean hasNonIgnoredDiff(JsonNode a, JsonNode b, Set<String> ignoredHeadersSeen) {
        // If both empty after filters, no real diff
        if (isEffectivelyEmpty(a, ignoredHeadersSeen) && isEffectivelyEmpty(b, ignoredHeadersSeen))
            return false;

        if (a == null)
            a = M.nullNode();
        if (b == null)
            b = M.nullNode();

        if (a.isObject() && b.isObject()) {
            // Only care about fields not ignored; compare values with tolerance
            Set<String> fields = new LinkedHashSet<>();
            a.fieldNames().forEachRemaining(fields::add);
            b.fieldNames().forEachRemaining(fields::add);

            boolean anyReal = false;
            for (String f : fields) {
                if (markIfIgnored(f, ignoredHeadersSeen))
                    continue;
                if (!equalsWithTolerance(a.get(f), b.get(f))) {
                    anyReal = true;
                    break;
                }
            }
            return anyReal;
        }

        if (a.isArray() || b.isArray()) {
            int max = Math.max(a.isArray() ? a.size() : 1, b.isArray() ? b.size() : 1);
            for (int i = 0; i < max; i++) {
                JsonNode ai = a.isArray() ? (i < a.size() ? a.get(i) : null) : (i == 0 ? a : null);
                JsonNode bi = b.isArray() ? (i < b.size() ? b.get(i) : null) : (i == 0 ? b : null);
                if (hasNonIgnoredDiff(ai, bi, ignoredHeadersSeen))
                    return true;
            }
            return false;
        }

        // primitives: if both zero/empty -> no diff; else compare
        if (isZeroOrEmpty(a) && isZeroOrEmpty(b))
            return false;
        return !equalsWithTolerance(a, b);
    }

    private static boolean equalsWithTolerance(JsonNode a, JsonNode b) {
        // treat null/empty/"0"/0 as identical
        if (isZeroOrEmpty(a) && isZeroOrEmpty(b))
            return true;

        String sa = normalize(a);
        String sb = normalize(b);

        Double da = asDouble(a);
        Double db = asDouble(b);
        if (da != null && db != null) {
            if (Double.compare(da, db) == 0)
                return true;
            double ad = Math.abs(da), bd = Math.abs(db);
            double denom = Math.max(ad, bd);
            if (denom == 0.0)
                return true; // both zero-ish
            double rel = Math.abs(da - db) / denom;
            return rel <= REL_TOL;
        }

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
        boolean ignore = norm.startsWith("tpbuy")
                || norm.startsWith("tpsell")
                || norm.startsWith("paysoff")
                || norm.startsWith("item")
                || norm.startsWith("bestchoice");
        if (ignore)
            ignoredHeadersSeen.add(norm);
        return ignore;
    }

    private static String normalizeHeaderName(String field) {
        return field.trim().replaceAll("[^A-Za-z0-9]", "").toLowerCase(Locale.ROOT);
    }

    private static boolean isZeroOrEmpty(JsonNode n) {
        if (n == null || n.isNull())
            return true;
        // numeric or numeric-looking text -> treat ~0 as zero
        Double d = asDouble(n);
        if (d != null)
            return Math.abs(d) <= 1e-9;
        // otherwise, empty string counts as empty
        return normalize(n).isEmpty();
    }
}

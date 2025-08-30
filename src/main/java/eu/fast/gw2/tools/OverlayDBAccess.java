package eu.fast.gw2.tools;

import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

public class OverlayDBAccess {

    // ---- NEW small caches for name lookups ----
    private static final java.util.concurrent.ConcurrentHashMap<Integer, String> FEATURE_NAME_BY_PAGE = new java.util.concurrent.ConcurrentHashMap<>();
    private static final java.util.concurrent.ConcurrentHashMap<Long, String> DETAIL_FEATURE_NAME_BY_ID = new java.util.concurrent.ConcurrentHashMap<>();
    private static final java.util.concurrent.ConcurrentHashMap<String, String> DETAIL_FEATURE_NAME_BY_KEY = new java.util.concurrent.ConcurrentHashMap<>();

    public static String getMainRowsJson(String compositeKey /* "pageId|name" */) {
        int bar = compositeKey.indexOf('|');
        int pageId = Integer.parseInt(compositeKey.substring(0, bar));
        String name = compositeKey.substring(bar + 1);
        return Jpa.tx(em -> {
            var query = em.createNativeQuery("""
                        SELECT rows
                          FROM public.tables
                         WHERE page_id = :pid AND name = :k
                         ORDER BY id DESC
                         LIMIT 1
                    """).setParameter("pid", pageId)
                    .setParameter("k", name)
                    .getResultList();
            return query.isEmpty() ? null : (String) query.get(0);
        });
    }

    public static String getDetailRowsJson(long fid, String key) {
        return Jpa.tx(em -> {
            var query = em.createNativeQuery("""
                        SELECT rows FROM public.detail_tables
                         WHERE detail_feature_id = :fid AND key = :k
                    """).setParameter("fid", fid).setParameter("k", key).getResultList();
            return query.isEmpty() ? null : (String) query.get(0);
        });
    }

    public static void updateMain(String compositeKey, String rowsJson) {
        int bar = compositeKey.indexOf('|');
        int pageId = Integer.parseInt(compositeKey.substring(0, bar));
        String name = compositeKey.substring(bar + 1);
        Jpa.txVoid(em -> em.createNativeQuery("""
                    UPDATE public.tables
                       SET rows = :rows, updated_at = now()
                     WHERE page_id = :pid AND name = :k
                """).setParameter("rows", rowsJson)
                .setParameter("pid", pageId)
                .setParameter("k", name)
                .executeUpdate());
    }

    public static void updateDetail(long fid, String key, String rowsJson) {
        Jpa.txVoid(em -> em.createNativeQuery("""
                    UPDATE public.detail_tables
                       SET rows = :rows, updated_at = now()
                     WHERE detail_feature_id = :fid AND key = :k
                """).setParameter("rows", rowsJson)
                .setParameter("fid", fid)
                .setParameter("k", key)
                .executeUpdate());
    }

    /** (fid, key) pairs for detail tables. */
    public static List<Object[]> listDetailTargets() {
        return Jpa.tx(em -> em.createNativeQuery("""
                    SELECT detail_feature_id, key
                      FROM public.detail_tables
                     ORDER BY id DESC
                """).getResultList());
    }

    /** Distinct main table names. */
    public static List<String> listMainTargets() {
        return Jpa.tx(em -> (java.util.List<Object[]>) em.createNativeQuery("""
                    SELECT DISTINCT page_id, name
                      FROM public.tables
                     ORDER BY page_id, name
                """).getResultList())
                .stream()
                .map(r -> ((Number) r[0]).intValue() + "|" + (String) r[1]) // "pageId|name"
                .toList();
    }

    /** Latest rows JSON for each detail key in one (chunked) query. */
    public static Map<String, String> fetchLatestDetailRowsByKeys(Collection<String> keys) {
        Map<String, String> out = new HashMap<>();
        if (keys == null || keys.isEmpty())
            return out;

        final int CHUNK = Integer.getInteger("overlay.detailBatch", 500);
        List<String> list = keys.stream().filter(k -> k != null && !k.isBlank()).distinct().toList();

        for (int i = 0; i < list.size(); i += CHUNK) {
            List<String> chunk = list.subList(i, Math.min(i + CHUNK, list.size()));
            String inList = chunk.stream()
                    .map(k -> "'" + k.replace("'", "''") + "'")
                    .collect(Collectors.joining(","));

            String sql = """
                    SELECT DISTINCT ON (key) key, rows
                      FROM public.detail_tables
                     WHERE key IN (%s)
                     ORDER BY key, id DESC
                    """.formatted(inList);

            List<Object[]> rs = Jpa.tx(em -> em.createNativeQuery(sql).getResultList());

            Set<String> returned = new HashSet<>();
            for (Object[] r : rs) {
                String k = (String) r[0];
                String rowsJson = (String) r[1];
                out.put(k, rowsJson);
                returned.add(k);
            }
            for (String k : chunk) {
                out.putIfAbsent(k, null);
            }
        }
        return out;
    }

    /** Latest rows JSON for a single detail key. */
    public static String getLatestDetailRowsByKey(String key) {
        return Jpa.tx(em -> {
            var query = em.createNativeQuery("""
                        SELECT rows FROM public.detail_tables
                         WHERE key = :k
                         ORDER BY id DESC
                         LIMIT 1
                    """).setParameter("k", key).getResultList();
            return query.isEmpty() ? null : (String) query.get(0);
        });
    }

    public static java.util.Set<Integer> listIgnoredItemIdsByGroups(java.util.Collection<String> groups) {
        if (groups == null || groups.isEmpty())
            return java.util.Set.of();
        String in = groups.stream()
                .filter(g -> g != null && !g.isBlank())
                .map(g -> "'" + g.replace("'", "''") + "'")
                .collect(java.util.stream.Collectors.joining(","));
        String sql = """
                SELECT item_id
                  FROM public.overlay_ignored_items
                 WHERE group_label IN (%s)
                """.formatted(in);
        java.util.List<Number> rows = Jpa.tx(em -> em.createNativeQuery(sql).getResultList());
        java.util.Set<Integer> out = new java.util.HashSet<>(rows.size());
        for (Number n : rows)
            out.add(n.intValue());
        return out;
    }

    // -------- NEW deterministic-lookups used by computeRow --------

    /** pages.id -> features.name */
    public static String featureNameByPageId(int pageId) {
        if (pageId <= 0)
            return null;
        String cached = FEATURE_NAME_BY_PAGE.get(pageId);
        if (cached != null)
            return cached;
        String name = Jpa.tx(em -> {
            var r = em.createNativeQuery("""
                        SELECT f.name
                          FROM public.pages p
                          JOIN public.features f ON f.id = p.feature_id
                         WHERE p.id = :pid
                    """).setParameter("pid", pageId).getResultList();
            return (r.isEmpty() ? null : String.valueOf(r.get(0)));
        });
        if (name != null)
            FEATURE_NAME_BY_PAGE.put(pageId, name);
        return name;
    }

    /** parse "pageId|pageName" → pageId (or 0) */
    public static int pageIdFromComposite(String compositeKey) {
        if (compositeKey == null)
            return 0;
        int bar = compositeKey.indexOf('|');
        if (bar <= 0)
            return 0;
        try {
            return Integer.parseInt(compositeKey.substring(0, bar));
        } catch (Exception ignored) {
            return 0;
        }
    }

    /** parse "pageId|pageName" → pageName (never null) */
    public static String pageNameFromComposite(String compositeKey) {
        if (compositeKey == null)
            return "";
        int bar = compositeKey.indexOf('|');
        return (bar < 0 ? compositeKey : compositeKey.substring(bar + 1));
    }

    /** detail_features.id -> name */
    public static String detailFeatureNameById(long fid) {
        if (fid <= 0)
            return null;
        String cached = DETAIL_FEATURE_NAME_BY_ID.get(fid);
        if (cached != null)
            return cached;
        String name = Jpa.tx(em -> {
            var r = em.createNativeQuery("""
                        SELECT name FROM public.detail_features WHERE id = :fid
                    """).setParameter("fid", fid).getResultList();
            return (r.isEmpty() ? null : String.valueOf(r.get(0)));
        });
        if (name != null)
            DETAIL_FEATURE_NAME_BY_ID.put(fid, name);
        return name;
    }

    /** detail_tables.key -> detail_features.name (latest by key) */
    public static String detailFeatureNameByKey(String key) {
        if (key == null || key.isBlank())
            return null;
        String cached = DETAIL_FEATURE_NAME_BY_KEY.get(key);
        if (cached != null)
            return cached;
        String name = Jpa.tx(em -> {
            var r = em.createNativeQuery("""
                        SELECT df.name
                          FROM public.detail_tables dt
                          JOIN public.detail_features df ON df.id = dt.detail_feature_id
                         WHERE dt.key = :k
                         ORDER BY dt.id DESC
                         LIMIT 1
                    """).setParameter("k", key).getResultList();
            return (r.isEmpty() ? null : String.valueOf(r.get(0)));
        });
        if (name != null)
            DETAIL_FEATURE_NAME_BY_KEY.put(key, name);
        return name;
    }
}

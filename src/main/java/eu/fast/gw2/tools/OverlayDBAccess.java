package eu.fast.gw2.tools;

import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

public class OverlayDBAccess {

    public static String getMainRowsJson(String tableKey) {
        return Jpa.tx(em -> {
            var query = em.createNativeQuery("""
                        SELECT rows
                          FROM public.tables
                         WHERE name = :k
                         ORDER BY id DESC
                         LIMIT 1
                    """).setParameter("k", tableKey).getResultList();
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

    public static void updateMain(String tableKey, String rowsJson) {
        Jpa.txVoid(em -> em.createNativeQuery("""
                    UPDATE public.tables
                       SET rows = :rows, updated_at = now()
                     WHERE name = :k
                """).setParameter("rows", rowsJson)
                .setParameter("k", tableKey)
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
    @SuppressWarnings("unchecked")
    public static List<Object[]> listDetailTargets() {
        return Jpa.tx(em -> em.createNativeQuery("""
                    SELECT detail_feature_id, key
                      FROM public.detail_tables
                     ORDER BY id DESC
                """).getResultList());
    }

    /** Distinct main table names. */
    @SuppressWarnings("unchecked")
    public static List<String> listMainTargets() {
        return Jpa.tx(em -> (java.util.List<String>) em.createNativeQuery("""
                    SELECT DISTINCT name
                      FROM public.tables
                     ORDER BY name
                """).getResultList());
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
            // mark non-found as null (caller will cache empty list)
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
}

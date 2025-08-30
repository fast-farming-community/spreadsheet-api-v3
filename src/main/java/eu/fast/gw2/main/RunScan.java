package eu.fast.gw2.main;

import java.text.Normalizer;
import java.text.Normalizer.Form;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;

import eu.fast.gw2.tools.Jpa;

public final class RunScan {

    private static String normKey(String k) {
        if (k == null)
            return null;
        // Unicode normalize, trim, and fold case to catch “same” keys
        String s = Normalizer.normalize(k, Form.NFKC).trim();
        // If your DB collation is case-sensitive, using a single case here helps
        return s.toUpperCase(Locale.ROOT);
    }

    private static String normCat(String c) {
        if (c == null)
            return "(UNKNOWN)";
        return Normalizer.normalize(c, Form.NFKC).trim().toUpperCase(Locale.ROOT);
    }

    public static void main(String[] args) {
        // Pull the latest row per (detail_feature_id, key), and read the category from
        // detail_features
        List<Object[]> rs = Jpa.tx(em -> em.createNativeQuery("""
                    SELECT DISTINCT ON (dt.detail_feature_id, dt.key)
                           dt.detail_feature_id,
                           dt.key,
                           dt.id,
                           df.name AS category_name
                      FROM public.detail_tables dt
                      JOIN public.detail_features df
                        ON df.id = dt.detail_feature_id
                     WHERE dt.key IS NOT NULL AND btrim(dt.key) <> ''
                     ORDER BY dt.detail_feature_id, dt.key, dt.id DESC
                """).getResultList());

        // keyNorm -> (categoryNorm -> fids)
        Map<String, Map<String, List<Long>>> byKey = new LinkedHashMap<>();

        for (Object[] r : rs) {
            long fid = ((Number) r[0]).longValue();
            String keyRaw = (String) r[1];
            String catRaw = (String) r[3];

            String key = normKey(keyRaw);
            String cat = normCat(catRaw);

            byKey
                    .computeIfAbsent(key, k -> new LinkedHashMap<>())
                    .computeIfAbsent(cat, c -> new ArrayList<>())
                    .add(fid);
        }

        int collisions = 0;
        for (Map.Entry<String, Map<String, List<Long>>> e : byKey.entrySet()) {
            String keyNorm = e.getKey();
            Map<String, List<Long>> cats = e.getValue();
            if (cats.size() <= 1)
                continue; // no cross-category collision

            collisions++;
            Set<String> catSet = new LinkedHashSet<>(cats.keySet());
            System.out.printf("KEY '%s' COLLISION: categories=%s%n", keyNorm, catSet);
            for (Map.Entry<String, List<Long>> byCat : cats.entrySet()) {
                System.out.printf("  - category=%s, fids=%s%n", byCat.getKey(), byCat.getValue());
            }
            System.out.println();
        }

        System.out.printf("Total keys with category collisions: %d%n", collisions);
    }
}

package eu.fast.gw2.tools;

import java.util.*;
import eu.fast.gw2.dao.Gw2PricesDao;

public class OverlayCache {

    private static final int DETAIL_CACHE_MAX = Integer.getInteger("overlay.detailCacheMax", 4096);

    /** LRU cache for detail_tables.rows by key. */
    private static final LinkedHashMap<String, List<Map<String, Object>>> DETAIL_ROWS_CACHE = new LinkedHashMap<>(256,
            0.75f, true) {
        @Override
        protected boolean removeEldestEntry(Map.Entry<String, List<Map<String, Object>>> eldest) {
            return size() > DETAIL_CACHE_MAX;
        }
    };

    /** Cache for vendor values by item id. Allows null values. */
    private static final Map<Integer, Integer> VENDOR_CACHE = new HashMap<>();

    /** LRU cache for EV of detail tables per (tier|taxes|detailKey). */
    private static final LinkedHashMap<String, int[]> EV_CACHE = new LinkedHashMap<>(512, 0.75f, true) {
        @Override
        protected boolean removeEldestEntry(Map.Entry<String, int[]> eldest) {
            return size() > 4096;
        }
    };

    public static Integer vendorValueCached(int itemId) {
        if (VENDOR_CACHE.containsKey(itemId))
            return VENDOR_CACHE.get(itemId);
        Integer v = Gw2PricesDao.vendorValueById(itemId);
        VENDOR_CACHE.put(itemId, v); // may be null
        return v;
    }

    public static int[] getEv(String ck) {
        return EV_CACHE.get(ck);
    }

    public static void putEv(String ck, int[] ev) {
        EV_CACHE.put(ck, ev);
    }

    /** Get (and lazily load) rows by detail key. */
    public static List<Map<String, Object>> getDetailRowsCached(String key) {
        if (key == null || key.isBlank())
            return List.of();
        var cached = DETAIL_ROWS_CACHE.get(key);
        if (cached != null)
            return cached;

        String json = OverlayDBAccess.getLatestDetailRowsByKey(key);
        if (json == null || json.isBlank()) {
            DETAIL_ROWS_CACHE.put(key, List.of());
            return List.of();
        }
        var rows = OverlayJson.parseRows(json);
        DETAIL_ROWS_CACHE.put(key, rows);
        return rows;
    }

    /** Batch preload detail rows for keys; caches empty lists for not-found. */
    public static void preloadDetailRows(Collection<String> keys) {
        if (keys == null || keys.isEmpty())
            return;

        List<String> missing = keys.stream()
                .filter(k -> k != null && !k.isBlank() && !DETAIL_ROWS_CACHE.containsKey(k))
                .toList();
        if (missing.isEmpty())
            return;

        Map<String, String> jsons = OverlayDBAccess.fetchLatestDetailRowsByKeys(missing);
        Set<String> seen = new HashSet<>();
        for (var e : jsons.entrySet()) {
            String k = e.getKey();
            String j = e.getValue();
            List<Map<String, Object>> rows = (j == null || j.isBlank()) ? List.of() : OverlayJson.parseRows(j);
            DETAIL_ROWS_CACHE.put(k, rows);
            seen.add(k);
        }
        // any gap is cached as empty
        for (String k : missing) {
            if (!seen.contains(k))
                DETAIL_ROWS_CACHE.put(k, List.of());
        }
    }
}

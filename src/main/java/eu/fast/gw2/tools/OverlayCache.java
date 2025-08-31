package eu.fast.gw2.tools;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

import eu.fast.gw2.dao.Gw2PricesDao;
import eu.fast.gw2.enums.Tier;

public class OverlayCache {

    private static final int DETAIL_CACHE_MAX = Integer.getInteger("overlay.detailCacheMax", 4096);

    // ---------- detail rows LRU ----------
    private static final LinkedHashMap<String, List<Map<String, Object>>> DETAIL_ROWS_CACHE = new LinkedHashMap<>(256,
            0.75f, true) {
        @Override
        protected boolean removeEldestEntry(Map.Entry<String, List<Map<String, Object>>> eldest) {
            return size() > DETAIL_CACHE_MAX;
        }
    };

    // ---------- EV CHM ----------
    private static final java.util.concurrent.ConcurrentHashMap<String, int[]> EV_CACHE = new java.util.concurrent.ConcurrentHashMap<>();

    // ---------- vendor cache ----------
    private static final Map<Integer, Integer> VENDOR_CACHE = new HashMap<>();

    // ---------- price/image/rarity caches (shared by whole run) ----------
    private static final ConcurrentHashMap<String, ConcurrentHashMap<Integer, int[]>> PRICE_CACHE_BY_TIER = new ConcurrentHashMap<>();
    private static final ConcurrentHashMap<Integer, String> IMAGE_CACHE = new ConcurrentHashMap<>();
    private static final ConcurrentHashMap<Integer, String> RARITY_CACHE = new ConcurrentHashMap<>();

    // ---------- base rows caches for the run ----------
    private static final Map<String, List<Map<String, Object>>> MAIN_ROWS_BASE = new ConcurrentHashMap<>();
    private static final Map<String, List<Map<String, Object>>> DETAIL_ROWS_BASE = new ConcurrentHashMap<>();

    // ----- EV cache accessors -----
    public static int[] getEv(String ck) {
        return EV_CACHE.get(ck);
    }

    public static void putEv(String ck, int[] ev) {
        EV_CACHE.put(ck, ev);
    }

    // ----- vendor -----
    public static Integer vendorValueCached(int itemId) {
        if (VENDOR_CACHE.containsKey(itemId))
            return VENDOR_CACHE.get(itemId);
        Integer v = Gw2PricesDao.vendorValueById(itemId);
        VENDOR_CACHE.put(itemId, v); // may be null
        return v;
    }

    // ----- detail rows preload / fetch -----
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
            DETAIL_ROWS_BASE.put(k, rows); // mirror into BASE for run usage
            seen.add(k);
        }
        for (String k : missing) {
            if (!seen.contains(k)) {
                DETAIL_ROWS_CACHE.put(k, List.of());
                DETAIL_ROWS_BASE.put(k, List.of());
            }
        }
    }

    // ----- preload main rows for a set of table names -----
    public static int preloadMainRows(Collection<String> mainKeys) {
        if (mainKeys == null || mainKeys.isEmpty())
            return 0;
        int loaded = 0;
        for (String key : mainKeys) {
            if (key == null || key.isBlank())
                continue;
            if (MAIN_ROWS_BASE.containsKey(key)) {
                loaded++;
                continue;
            }
            String rowsJson = OverlayDBAccess.getMainRowsJson(key);
            MAIN_ROWS_BASE.put(key,
                    (rowsJson == null || rowsJson.isBlank()) ? List.of() : OverlayJson.parseRows(rowsJson));
            loaded++;
        }
        return loaded;
    }

    public static List<Map<String, Object>> getBaseMainRows(String compositeKey) {
        return MAIN_ROWS_BASE.get(compositeKey);
    }

    public static List<Map<String, Object>> getBaseDetailRows(String key) {
        return DETAIL_ROWS_BASE.computeIfAbsent(key, OverlayCache::getDetailRowsCached);
    }

    // ----- collect all item ids from preloaded bases -----
    public static Set<Integer> collectAllItemIdsFromPreloaded() {
        Set<Integer> all = new HashSet<>(16384);
        // from mains
        for (var e : MAIN_ROWS_BASE.entrySet()) {
            for (var row : e.getValue()) {
                String cat = OverlayHelper.str(row.get(OverlayHelper.COL_CAT));
                String key = OverlayHelper.str(row.get(OverlayHelper.COL_KEY));
                if (OverlayHelper.isCompositeRef(cat, key) || OverlayHelper.isInternal(cat))
                    continue;
                int id = OverlayHelper.toInt(row.get(OverlayHelper.COL_ID), -1);
                if (id > 0)
                    all.add(id);
            }
        }
        // from details
        for (var e : DETAIL_ROWS_BASE.entrySet()) {
            all.addAll(OverlayHelper.extractIds(e.getValue()));
        }
        return all;
    }

    // ----- price/image/rarity cache fillers -----
    public static Map<Integer, int[]> getOrFillPriceCache(Set<Integer> ids, Tier tier) {
        var cache = PRICE_CACHE_BY_TIER.computeIfAbsent(tier.columnKey(), k -> new ConcurrentHashMap<>());
        if (ids != null && !ids.isEmpty()) {
            ArrayList<Integer> missing = null;
            for (Integer id : ids) {
                if (id == null || id <= 0)
                    continue;
                if (!cache.containsKey(id)) {
                    if (missing == null)
                        missing = new ArrayList<>();
                    missing.add(id);
                }
            }
            if (missing != null && !missing.isEmpty()) {
                cache.putAll(Gw2PricesDao.loadTier(missing, tier.columnKey()));
            }
        }
        return cache;
    }

    public static Map<Integer, String> getOrFillImageCache(Set<Integer> ids) {
        if (ids != null && !ids.isEmpty()) {
            ArrayList<Integer> missing = null;
            for (Integer id : ids) {
                if (id == null || id <= 0)
                    continue;
                if (!IMAGE_CACHE.containsKey(id)) {
                    if (missing == null)
                        missing = new ArrayList<>();
                    missing.add(id);
                }
            }
            if (missing != null && !missing.isEmpty()) {
                // split: currencies (<200) vs items (>=200)
                java.util.Set<Integer> cur = new java.util.HashSet<>();
                java.util.Set<Integer> itm = new java.util.HashSet<>();
                for (Integer id : missing) {
                    if (id < 200)
                        cur.add(id);
                    else
                        itm.add(id);
                }
                if (!itm.isEmpty()) {
                    IMAGE_CACHE.putAll(Gw2PricesDao.loadImageUrlsByIds(new java.util.HashSet<>(itm)));
                }
                if (!cur.isEmpty()) {
                    // fetch proper currency icons
                    IMAGE_CACHE.putAll(Gw2PricesDao.loadCurrencyIconsByIds(new java.util.HashSet<>(cur)));
                }
            }
        }
        return IMAGE_CACHE;
    }

    public static Map<Integer, String> getOrFillRarityCache(Set<Integer> ids) {
        if (ids != null && !ids.isEmpty()) {
            ArrayList<Integer> missing = null;
            for (Integer id : ids) {
                if (id == null || id <= 0)
                    continue;
                if (!RARITY_CACHE.containsKey(id)) {
                    if (missing == null)
                        missing = new ArrayList<>();
                    missing.add(id);
                }
            }
            if (missing != null && !missing.isEmpty()) {
                // split again: no rarity for currencies (<200)
                java.util.Set<Integer> cur = new java.util.HashSet<>();
                java.util.Set<Integer> itm = new java.util.HashSet<>();
                for (Integer id : missing) {
                    if (id < 200)
                        cur.add(id);
                    else
                        itm.add(id);
                }
                if (!itm.isEmpty()) {
                    RARITY_CACHE.putAll(Gw2PricesDao.loadRaritiesByIds(new java.util.HashSet<>(itm)));
                }
            }
        }
        return RARITY_CACHE;
    }
}

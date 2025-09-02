package eu.fast.gw2.dao;

import java.io.IOException;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;

import eu.fast.gw2.tools.Jpa;

public class Gw2PricesDao {

    // For JSON + HTTP (Java 11+ standard client, no extra deps)
    private static final ObjectMapper M = new ObjectMapper();
    private static final HttpClient HTTP = HttpClient.newHttpClient();

    public static Map<Integer, int[]> loadTier(List<Integer> ids, String tierKey) {
        if (ids == null || ids.isEmpty())
            return Collections.emptyMap();

        final String buyCol, sellCol;
        switch (tierKey) {
            case "2m" -> {
                buyCol = "buy_2m";
                sellCol = "sell_2m";
            }
            case "10m" -> {
                buyCol = "buy_10m";
                sellCol = "sell_10m";
            }
            default -> {
                buyCol = "buy_60m";
                sellCol = "sell_60m";
            }
        }

        String sql = """
                    SELECT item_id, COALESCE(%s,0) AS b, COALESCE(%s,0) AS s
                      FROM public.gw2_prices_tiers
                     WHERE item_id IN (:ids)
                """.formatted(buyCol, sellCol);

        List<Object[]> rows = Jpa.tx(em -> em.createNativeQuery(sql).setParameter("ids", ids).getResultList());

        Map<Integer, int[]> out = new HashMap<>();
        for (Object[] r : rows) {
            out.put(((Number) r[0]).intValue(),
                    new int[] { ((Number) r[1]).intValue(), ((Number) r[2]).intValue() });
        }
        for (Integer id : ids)
            out.putIfAbsent(id, new int[] { 0, 0 });
        return out;
    }

    public static Integer vendorValueById(int itemId) {
        return Jpa.tx(em -> {
            var r = em.createNativeQuery("""
                        SELECT vendor_value FROM public.gw2_prices WHERE item_id = :id
                    """).setParameter("id", itemId).getResultList();
            if (r.isEmpty())
                return null;
            Object o = r.get(0);
            return (o == null) ? null : ((Number) o).intValue();
        });
    }

    /**
     * Idempotent upsert: only updates when vendor_value actually changes. Returns
     * updated row count (approx = input size, minus no-op conflicts).
     */
    public static int upsertVendorValuesIfChanged(Map<Integer, Integer> vendorMap) {
        if (vendorMap == null || vendorMap.isEmpty())
            return 0;

        StringBuilder sb = new StringBuilder(256 + vendorMap.size() * 32);
        sb.append("""
                    INSERT INTO public.gw2_prices(item_id, buy, sell, vendor_value, updated_at, ts)
                    VALUES
                """);

        boolean first = true;
        for (var e : vendorMap.entrySet()) {
            if (!first)
                sb.append(',');
            first = false;
            sb.append("(").append(e.getKey()).append(",0,0,").append(e.getValue()).append(",now(),now())");
        }

        // Only fire update when the value is different (IS DISTINCT FROM handles NULLs)
        sb.append("""
                    ON CONFLICT (item_id) DO UPDATE
                      SET vendor_value = EXCLUDED.vendor_value,
                          updated_at   = now()
                      WHERE gw2_prices.vendor_value IS DISTINCT FROM EXCLUDED.vendor_value
                """);

        final String sql = sb.toString();
        return Jpa.tx(em -> em.createNativeQuery(sql).executeUpdate());
    }

    // Kept for compatibility; still works but updates even on same value.
    public static void upsertVendorValues(Map<Integer, Integer> vendorMap) {
        if (vendorMap == null || vendorMap.isEmpty())
            return;
        Jpa.txVoid(em -> {
            var sb = new StringBuilder();
            sb.append("""
                        INSERT INTO public.gw2_prices(item_id, buy, sell, vendor_value, updated_at, ts)
                        VALUES
                    """);
            boolean first = true;
            for (var e : vendorMap.entrySet()) {
                if (!first)
                    sb.append(',');
                first = false;
                sb.append("(").append(e.getKey()).append(",0,0,").append(e.getValue()).append(",now(),now())");
            }
            sb.append("""
                        ON CONFLICT (item_id) DO UPDATE
                          SET vendor_value = EXCLUDED.vendor_value,
                              updated_at   = now()
                    """);
            em.createNativeQuery(sb.toString()).executeUpdate();
        });
    }

    public static void updateImagesIfChanged(Map<Integer, String> images) {
        if (images == null || images.isEmpty())
            return;
        Jpa.txVoid(em -> {
            for (var e : images.entrySet()) {
                em.createNativeQuery("""
                            UPDATE public.gw2_prices
                               SET image = :img
                             WHERE item_id = :id
                               AND (image IS DISTINCT FROM :img)
                        """).setParameter("id", e.getKey())
                        .setParameter("img", e.getValue())
                        .executeUpdate();
            }
        });
    }

    public static void updateRaritiesIfChanged(Map<Integer, String> rarities) {
        if (rarities == null || rarities.isEmpty())
            return;
        Jpa.txVoid(em -> {
            for (var e : rarities.entrySet()) {
                em.createNativeQuery("""
                            UPDATE public.gw2_prices
                               SET rarity = :rar
                             WHERE item_id = :id
                               AND (rarity IS DISTINCT FROM :rar)
                        """).setParameter("id", e.getKey())
                        .setParameter("rar", e.getValue())
                        .executeUpdate();
            }
        });
    }

    public static Map<Integer, String> loadImageUrlsByIds(java.util.Collection<Integer> ids) {
        if (ids == null || ids.isEmpty())
            return java.util.Map.of();
        String in = ids.stream().map(String::valueOf).collect(java.util.stream.Collectors.joining(","));
        return Jpa.tx(em -> {
            var rs = em.createNativeQuery("""
                    SELECT item_id, image
                      FROM public.gw2_prices
                     WHERE item_id IN (""" + in + ")").getResultList();
            java.util.Map<Integer, String> out = new java.util.HashMap<>();
            for (Object row : rs) {
                Object[] a = (Object[]) row;
                Integer id = ((Number) a[0]).intValue();
                String url = (String) a[1];
                if (url != null && !url.isBlank())
                    out.put(id, url);
            }
            return out;
        });
    }

    public static Map<Integer, String> loadRaritiesByIds(java.util.Collection<Integer> ids) {
        if (ids == null || ids.isEmpty())
            return java.util.Map.of();
        String in = ids.stream().map(String::valueOf).collect(java.util.stream.Collectors.joining(","));
        return Jpa.tx(em -> {
            var rs = em.createNativeQuery("""
                    SELECT item_id, rarity
                      FROM public.gw2_prices
                     WHERE item_id IN (""" + in + ")").getResultList();
            java.util.Map<Integer, String> out = new java.util.HashMap<>();
            for (Object row : rs) {
                Object[] a = (Object[]) row;
                Integer id = ((Number) a[0]).intValue();
                String rarity = (String) a[1];
                if (rarity != null && !rarity.isBlank())
                    out.put(id, rarity);
            }
            return out;
        });
    }

    /**
     * Return icon URLs for GW2 currencies (ids < 200) by calling the official API.
     */
    public static Map<Integer, String> loadCurrencyIconsByIds(Set<Integer> ids) {
        if (ids == null || ids.isEmpty())
            return Collections.emptyMap();

        try {
            // Build query string: e.g. ids=1,2,3
            StringBuilder sb = new StringBuilder("https://api.guildwars2.com/v2/currencies?ids=");
            boolean first = true;
            for (Integer id : ids) {
                if (!first)
                    sb.append(',');
                sb.append(id);
                first = false;
            }
            URI uri = URI.create(sb.toString());

            HttpRequest req = HttpRequest.newBuilder(uri).GET().build();
            HttpResponse<String> resp = HTTP.send(req, HttpResponse.BodyHandlers.ofString());
            int code = resp.statusCode();
            if (code != 200 && code != 206) {
                System.err.println("GW2 API currencies failed: HTTP " + resp.statusCode());
                return Collections.emptyMap();
            }

            JsonNode arr = M.readTree(resp.body());
            Map<Integer, String> out = new HashMap<>();
            if (arr != null && arr.isArray()) {
                for (JsonNode n : arr) {
                    JsonNode idN = n.get("id");
                    JsonNode iconN = n.get("icon");
                    if (idN != null && iconN != null && !iconN.isNull()) {
                        out.put(idN.asInt(), iconN.asText());
                    }
                }
            }
            return out;
        } catch (IOException | InterruptedException e) {
            e.printStackTrace();
            return Collections.emptyMap();
        }
    }
}

package eu.fast.gw2.tools;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;

import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.time.Duration;
import java.util.*;

public class Gw2ApiClient {
    private static final int MAX_RETRIES = 3;
    private static final long BASE_BACKOFF_MS = 400L;

    private static final HttpClient HTTP = HttpClient.newBuilder()
            .connectTimeout(Duration.ofSeconds(10))
            .build();
    private static final ObjectMapper M = new ObjectMapper();

    /** Matches `/v2/commerce/prices` JSON shape and your runner's expectations. */
    @JsonIgnoreProperties(ignoreUnknown = true)
    public record PriceEntry(int id, Side buys, Side sells) {
        @JsonIgnoreProperties(ignoreUnknown = true)
        public record Side(
                @JsonProperty("unit_price") int unitPrice,
                int quantity) {
        }
    }

    @JsonIgnoreProperties(ignoreUnknown = true)
    public record Item(int id, java.util.List<String> flags, Integer vendor_value) {
    }

    /** Public: fetch raw TP prices for given IDs, chunked and merged. */
    public static Map<Integer, int[]> fetchPrices(Collection<Integer> ids) throws Exception {
        if (ids == null || ids.isEmpty())
            return Map.of();

        Map<Integer, int[]> out = new HashMap<>();
        for (List<Integer> batch : chunks(ids, 200)) {
            List<PriceEntry> prices = sendWithRetry(() -> fetchPricesBatch(batch));
            for (PriceEntry p : prices) {
                int buy = (p.buys() == null ? 0 : Math.max(0, p.buys().unitPrice()));
                int sell = (p.sells() == null ? 0 : Math.max(0, p.sells().unitPrice()));
                out.put(p.id(), new int[] { buy, sell });
            }
        }
        return out;
    }

    /** Single-call batch fetch (≤200 ids) used by the runner. */
    public static List<PriceEntry> fetchPricesBatch(List<Integer> ids) throws Exception {
        if (ids == null || ids.isEmpty())
            return List.of();
        if (ids.size() > 200)
            throw new IllegalArgumentException("max 200 ids per batch");

        String url = "https://api.guildwars2.com/v2/commerce/prices?ids=" + join(ids);
        HttpRequest req = HttpRequest.newBuilder(URI.create(url))
                .timeout(Duration.ofSeconds(15))
                .header("Accept", "application/json")
                .GET().build();
        var res = HTTP.send(req, HttpResponse.BodyHandlers.ofString());
        if (res.statusCode() >= 400)
            throw new RuntimeException("GW2 prices HTTP " + res.statusCode());
        return M.readValue(res.body(), new TypeReference<List<PriceEntry>>() {
        });
    }

    public static List<Item> fetchItemsBatch(List<Integer> ids) throws Exception {
        if (ids == null || ids.isEmpty())
            return List.of();
        if (ids.size() > 200)
            throw new IllegalArgumentException("max 200 ids per batch");

        String url = "https://api.guildwars2.com/v2/items?ids=" + join(ids);
        HttpRequest req = HttpRequest.newBuilder(URI.create(url))
                .timeout(Duration.ofSeconds(15))
                .header("Accept", "application/json")
                .GET().build();
        var res = HTTP.send(req, HttpResponse.BodyHandlers.ofString());
        if (res.statusCode() >= 400)
            throw new RuntimeException("GW2 items HTTP " + res.statusCode());
        return M.readValue(res.body(), new TypeReference<List<Item>>() {
        });
    }

    public static Map<Integer, Boolean> accountBoundMap(List<Item> items) {
        Map<Integer, Boolean> out = new HashMap<>();
        for (Item it : items) {
            boolean bound = it.flags() != null && it.flags().stream().anyMatch("AccountBound"::equalsIgnoreCase);
            out.put(it.id(), bound);
        }
        return out;
    }

    public static Map<Integer, Integer> vendorValueMap(List<Item> items) {
        Map<Integer, Integer> out = new HashMap<>();
        for (Item it : items) {
            if (it.vendor_value() != null)
                out.put(it.id(), it.vendor_value());
        }
        return out;
    }

    private static String join(List<Integer> ids) {
        StringBuilder sb = new StringBuilder(ids.size() * 6);
        for (int i = 0; i < ids.size(); i++) {
            if (i > 0)
                sb.append(',');
            sb.append(ids.get(i));
        }
        return sb.toString();
    }

    @FunctionalInterface
    private interface IoCall<T> {
        T run() throws Exception;
    }

    /** Simple retry wrapper for transient I/O. */
    private static <T> T sendWithRetry(IoCall<T> call) throws Exception {
        int attempt = 0;
        while (true) {
            try {
                return call.run();
            } catch (RuntimeException re) {
                // Bubble up non-HTTP runtime errors from call-site if any
                throw re;
            } catch (Exception ex) {
                if (++attempt > MAX_RETRIES)
                    throw ex;
                Thread.sleep(BASE_BACKOFF_MS * attempt);
            }
        }
    }

    /** Make ≤size chunks from a Collection while preserving iteration order. */
    private static List<List<Integer>> chunks(Collection<Integer> ids, int size) {
        List<List<Integer>> out = new ArrayList<>();
        List<Integer> cur = new ArrayList<>(size);
        for (Integer id : ids) {
            if (cur.size() == size) {
                out.add(cur);
                cur = new ArrayList<>(size);
            }
            cur.add(id);
        }
        if (!cur.isEmpty())
            out.add(cur);
        return out;
    }
}

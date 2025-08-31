package eu.fast.gw2.patreon;

import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.time.Duration;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Optional;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;

final class PatreonClient {

    static Map<String, Integer> fetchCurrentPatrons() {
        String mode = System.getenv().getOrDefault("PATREON_SYNC_MODE", "MOCK").toUpperCase(Locale.ROOT);
        return switch (mode) {
            case "REAL" -> fetchFromPatreon();
            default -> fetchMock();
        };
    }

    // MOCK mode: read from env comma lists
    private static Map<String, Integer> fetchMock() {
        Map<String, Integer> out = new HashMap<>();
        String legion = System.getenv().getOrDefault("PATREON_SYNC_MOCK_LEGIONNAIRE", "");
        String premium = System.getenv().getOrDefault("PATREON_SYNC_MOCK_PREMIUM", "");
        for (String e : split(legion))
            out.put(e, 100); // 1.00€
        for (String e : split(premium))
            out.put(e, 1000); // 10.00€
        System.out.println("[Patreon MOCK] patrons=" + out.size());
        return out;
    }

    private static List<String> split(String s) {
        if (s == null || s.isBlank())
            return List.of();
        String[] arr = s.split(",");
        List<String> list = new ArrayList<>(arr.length);
        for (String a : arr) {
            String x = a.trim().toLowerCase(Locale.ROOT);
            if (!x.isEmpty())
                list.add(x);
        }
        return list;
    }

    // REAL mode: minimal Patreon v2 call (members + included users for emails)
    private static Map<String, Integer> fetchFromPatreon() {
        String token = System.getenv("PATREON_API_KEY");
        String campaign = System.getenv("PATREON_CAMPAIGN");
        if (token == null || campaign == null)
            throw new IllegalStateException("Missing PATREON_API_KEY or PATREON_CAMPAIGN");

        HttpClient http = HttpClient.newBuilder()
                .version(HttpClient.Version.HTTP_2)
                .connectTimeout(Duration.ofSeconds(15)).build();

        // We need: member relationships → user (email), and
        // currently_entitled_amount_cents
        String base = "https://www.patreon.com/api/oauth2/v2/campaigns/" + campaign + "/members";
        String fields = "fields%5Bmember%5D=currently_entitled_amount_cents,patron_status";
        String include = "include=user";
        String fieldsUser = "fields%5Buser%5D=email,full_name";
        String pageSize = "page%5Bsize%5D=1000";

        String url = base + "?" + fields + "&" + include + "&" + fieldsUser + "&" + pageSize;
        Map<String, Integer> out = new HashMap<>();
        ObjectMapper M = new ObjectMapper();

        String next = url;
        try {
            while (next != null) {
                HttpRequest req = HttpRequest.newBuilder(URI.create(next))
                        .timeout(Duration.ofSeconds(20))
                        .header("Authorization", "Bearer " + token)
                        .header("Accept", "application/json")
                        .GET().build();

                HttpResponse<String> res = http.send(req, HttpResponse.BodyHandlers.ofString());
                if (res.statusCode() >= 400) {
                    throw new RuntimeException("Patreon HTTP " + res.statusCode() + ": " + res.body());
                }

                JsonNode root = M.readTree(res.body());
                Map<String, String> userIdToEmail = new HashMap<>();

                // Build userId -> email map from "included"
                JsonNode included = root.get("included");
                if (included != null && included.isArray()) {
                    for (JsonNode inc : included) {
                        if ("user".equals(inc.path("type").asText())) {
                            String id = inc.path("id").asText();
                            String email = inc.path("attributes").path("email").asText(null);
                            if (email != null && !email.isBlank()) {
                                userIdToEmail.put(id, email.toLowerCase(Locale.ROOT));
                            }
                        }
                    }
                }

                // Walk members
                JsonNode data = root.get("data");
                if (data != null && data.isArray()) {
                    for (JsonNode m : data) {
                        String status = m.path("attributes").path("patron_status").asText("");
                        if (!"active_patron".equals(status))
                            continue;

                        int cents = m.path("attributes").path("currently_entitled_amount_cents").asInt(0);

                        // member → relationships.user.data.id
                        String userId = Optional
                                .ofNullable((ObjectNode) m.path("relationships").path("user").path("data"))
                                .map(n -> n.path("id").asText(null)).orElse(null);
                        String email = userId == null ? null : userIdToEmail.get(userId);
                        if (email != null) {
                            out.put(email, cents);
                        }
                    }
                }

                // pagination
                JsonNode nextNode = root.path("links").path("next");
                next = nextNode.isMissingNode() || nextNode.isNull() ? null : nextNode.asText(null);
            }
        } catch (Exception e) {
            throw new RuntimeException("Patreon fetch failed: " + e.getMessage(), e);
        }

        System.out.println("[Patreon REAL] patrons=" + out.size());
        return out;
    }

    private PatreonClient() {
    }
}

package eu.fast.gw2.patreon;

import java.net.URI;
import java.net.http.*;
import java.time.Duration;
import java.util.*;
import com.fasterxml.jackson.databind.*;
import com.fasterxml.jackson.databind.node.ObjectNode;

final class PatreonClient {

    /** Returns email -> currently_entitled_amount_cents for active patrons. */
    static Map<String, Integer> fetchCurrentPatrons() {
        String token = System.getenv("PATREON_API_KEY");
        String campaign = System.getenv("PATREON_CAMPAIGN");
        if (token == null || campaign == null || token.isBlank() || campaign.isBlank()) {
            throw new IllegalStateException("Missing PATREON_API_KEY or PATREON_CAMPAIGN");
        }

        HttpClient http = HttpClient.newBuilder()
                .version(HttpClient.Version.HTTP_2)
                .connectTimeout(Duration.ofSeconds(15))
                .build();

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
                        .timeout(Duration.ofSeconds(25))
                        .header("Authorization", "Bearer " + token)
                        .header("Accept", "application/json")
                        .GET().build();

                HttpResponse<String> res = http.send(req, HttpResponse.BodyHandlers.ofString());
                if (res.statusCode() >= 400) {
                    throw new RuntimeException("Patreon HTTP " + res.statusCode() + ": " + res.body());
                }

                JsonNode root = M.readTree(res.body());

                // Build userId -> email map from "included"
                Map<String, String> userIdToEmail = new HashMap<>();
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
                next = (nextNode.isMissingNode() || nextNode.isNull()) ? null : nextNode.asText(null);
            }
        } catch (Exception e) {
            throw new RuntimeException("Patreon fetch failed: " + e.getMessage(), e);
        }

        System.out.println("[Patreon] active patrons fetched: " + out.size());
        return out;
    }

    private PatreonClient() {
    }
}

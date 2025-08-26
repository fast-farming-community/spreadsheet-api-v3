package eu.fast.gw2.tools;

import java.util.List;
import java.util.Map;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;

public class OverlayJson {
    private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();

    public static List<Map<String, Object>> parseRows(String json) {
        try {
            return OBJECT_MAPPER.readValue(json, new TypeReference<List<Map<String, Object>>>() {
            });
        } catch (Exception e) {
            throw new RuntimeException("Failed to parse rows JSON", e);
        }
    }

    public static String toJson(List<Map<String, Object>> rows) {
        try {
            return OBJECT_MAPPER.writeValueAsString(rows);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }
}

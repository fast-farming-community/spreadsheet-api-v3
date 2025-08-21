package eu.fast.gw2.google;

import java.io.FileInputStream;
import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.api.client.http.HttpTransport;
import com.google.api.client.http.javanet.NetHttpTransport;
import com.google.api.client.json.JsonFactory;
import com.google.api.client.json.jackson2.JacksonFactory;
import com.google.api.services.sheets.v4.Sheets;
import com.google.api.services.sheets.v4.SheetsScopes;
import com.google.api.services.sheets.v4.model.BatchGetValuesResponse;
import com.google.api.services.sheets.v4.model.ValueRange;
import com.google.auth.http.HttpCredentialsAdapter;
import com.google.auth.oauth2.GoogleCredentials;

public class GoogleSheetsFetcher {
    private static final ObjectMapper M = new ObjectMapper();

    private final Sheets sheets;
    private final String spreadsheetId;

    public GoogleSheetsFetcher(String spreadsheetId) throws Exception {
        String credPath = System.getenv("GOOGLE_APPLICATION_CREDENTIALS");
        if (credPath == null || credPath.isBlank()) {
            throw new IllegalStateException("GOOGLE_APPLICATION_CREDENTIALS env var not set");
        }

        GoogleCredentials creds;
        try (FileInputStream in = new FileInputStream(credPath)) {
            creds = GoogleCredentials.fromStream(in)
                    .createScoped(Collections.singletonList(SheetsScopes.SPREADSHEETS_READONLY));
        }

        HttpTransport transport = new NetHttpTransport();
        JsonFactory jsonFactory = JacksonFactory.getDefaultInstance();

        this.sheets = new Sheets.Builder(transport, jsonFactory, new HttpCredentialsAdapter(creds))
                .setApplicationName("fast-gw2")
                .build();

        this.spreadsheetId = spreadsheetId;
    }

    /** Fetch multiple named ranges in one request. */
    public Map<String, List<List<Object>>> fetchRanges(List<String> ranges) throws Exception {
        if (ranges == null || ranges.isEmpty())
            return Map.of();

        BatchGetValuesResponse resp = sheets.spreadsheets().values()
                .batchGet(spreadsheetId)
                .setRanges(ranges)
                .execute();

        Map<String, List<List<Object>>> out = new LinkedHashMap<>();
        if (resp.getValueRanges() != null) {
            for (ValueRange vr : resp.getValueRanges()) {
                out.put(vr.getRange(), vr.getValues() == null ? List.of() : vr.getValues());
            }
        }
        return out;
    }

    /** Turn 2D values + headers → JSON array of objects (for DB rows). */
    public String toJson(List<List<Object>> rows, List<String> headers) throws Exception {
        List<Map<String, Object>> list = new ArrayList<>();
        for (List<Object> r : rows) {
            Map<String, Object> obj = new LinkedHashMap<>();
            for (int i = 0; i < headers.size(); i++) {
                Object v = (i < r.size()) ? r.get(i) : null;
                obj.put(headers.get(i), v);
            }
            list.add(obj);
        }
        return M.writeValueAsString(list);
    }
}

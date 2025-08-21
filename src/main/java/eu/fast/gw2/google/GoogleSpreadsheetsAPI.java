package eu.fast.gw2.google;

import eu.fast.gw2.jpa.Jpa;
import java.util.*;

public class GoogleSpreadsheetsAPI {
    private final GoogleSheetsFetcher fetcher;

    public GoogleSpreadsheetsAPI(String spreadsheetId) throws Exception {
        this.fetcher = new GoogleSheetsFetcher(spreadsheetId);
    }

    public void fetchTables(List<String> tableKeys) throws Exception {
        Map<String, List<List<Object>>> data = fetcher.fetchRanges(tableKeys);
        for (String key : data.keySet()) {
            List<List<Object>> rows = data.get(key);
            if (rows == null || rows.isEmpty())
                continue;

            List<String> headers = rows.get(0).stream().map(Object::toString).toList();
            List<List<Object>> body = rows.subList(1, rows.size());

            String json = fetcher.toJson(body, headers);

            Jpa.txVoid(em -> em.createNativeQuery("""
                        INSERT INTO public.tables (key, rows, updated_at)
                        VALUES (:k, :rows, now())
                        ON CONFLICT (key) DO UPDATE
                          SET rows = EXCLUDED.rows,
                              updated_at = now()
                    """).setParameter("k", key).setParameter("rows", json).executeUpdate());
        }
    }

    public void fetchDetailedTables(List<String> detailKeys) throws Exception {
        Map<String, List<List<Object>>> data = fetcher.fetchRanges(detailKeys);
        for (String key : data.keySet()) {
            List<List<Object>> rows = data.get(key);
            if (rows == null || rows.isEmpty())
                continue;

            List<String> headers = rows.get(0).stream().map(Object::toString).toList();
            List<List<Object>> body = rows.subList(1, rows.size());

            String json = fetcher.toJson(body, headers);

            Jpa.txVoid(em -> em.createNativeQuery("""
                        INSERT INTO public.detail_tables (key, rows, updated_at)
                        VALUES (:k, :rows, now())
                        ON CONFLICT (key) DO UPDATE
                          SET rows = EXCLUDED.rows,
                              updated_at = now()
                    """).setParameter("k", key).setParameter("rows", json).executeUpdate());
        }
    }
}

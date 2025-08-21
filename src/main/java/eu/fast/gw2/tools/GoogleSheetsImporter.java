package eu.fast.gw2.tools;

import java.io.FileInputStream;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.stream.Collectors;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.api.client.googleapis.javanet.GoogleNetHttpTransport;
import com.google.api.client.http.HttpBackOffIOExceptionHandler;
import com.google.api.client.http.HttpBackOffUnsuccessfulResponseHandler;
import com.google.api.client.http.HttpRequest;
import com.google.api.client.http.HttpRequestInitializer;
import com.google.api.client.util.ExponentialBackOff;
import com.google.api.services.sheets.v4.Sheets;
import com.google.api.services.sheets.v4.SheetsScopes;
import com.google.api.services.sheets.v4.model.BatchGetValuesResponse;
import com.google.api.services.sheets.v4.model.ValueRange;
import com.google.auth.http.HttpCredentialsAdapter;
import com.google.auth.oauth2.GoogleCredentials;

public class GoogleSheetsImporter {

    private static final ObjectMapper M = new ObjectMapper();
    private final Sheets sheets;
    private final String spreadsheetId;

    private static final int CONNECT_TIMEOUT_MS = 10_000;
    private static final int READ_TIMEOUT_MS = 300_000;
    private static final int MAX_RETRIES = 5;
    private static final int RANGES_CHUNK_SIZE = 60;

    private static synchronized void increment(int[] box) {
        box[0]++;
    }

    private static synchronized void add(int[] box, int n) {
        box[0] += n;
    }

    private static String sheetOf(String a1) {
        int bang = a1.indexOf('!');
        if (bang <= 0)
            return ""; // unknown sheet -> sort first
        return a1.substring(0, bang);
    }

    // used only to hint number parsing when a value is actually present
    private static final Set<String> NUMERIC_HEADERS = new LinkedHashSet<>(Arrays.asList(
            "Id", "AverageAmount", "TotalAmount", "TPBuyProfit", "TPSellProfit",
            "TPBuyProfitHr", "TPSellProfitHr", "Duration", "Price", "Buy", "Sell",
            "BuyPrice", "SellPrice", "Quantity", "Amount"));

    public GoogleSheetsImporter(String spreadsheetId) throws Exception {
        this.spreadsheetId = spreadsheetId;
        this.sheets = buildSheetsClient();
    }

    private Sheets buildSheetsClient() throws Exception {
        var transport = GoogleNetHttpTransport.newTrustedTransport();

        GoogleCredentials creds = GoogleCredentials
                .fromStream(new FileInputStream(System.getenv("GOOGLE_APPLICATION_CREDENTIALS")))
                .createScoped(SheetsScopes.SPREADSHEETS_READONLY);

        HttpRequestInitializer auth = new HttpCredentialsAdapter(creds);

        HttpRequestInitializer withTimeoutsAndBackoff = (HttpRequest req) -> {
            auth.initialize(req);
            req.setConnectTimeout(CONNECT_TIMEOUT_MS);
            req.setReadTimeout(READ_TIMEOUT_MS);
            req.setNumberOfRetries(MAX_RETRIES);

            var backoff = new ExponentialBackOff.Builder()
                    .setInitialIntervalMillis(500)
                    .setMaxElapsedTimeMillis(120_000)
                    .setMaxIntervalMillis(8_000)
                    .setMultiplier(2.0)
                    .setRandomizationFactor(0.3)
                    .build();

            req.setIOExceptionHandler(new HttpBackOffIOExceptionHandler(backoff));
            req.setUnsuccessfulResponseHandler(new HttpBackOffUnsuccessfulResponseHandler(backoff));
        };

        return new Sheets.Builder(
                transport,
                com.google.api.client.json.jackson2.JacksonFactory.getDefaultInstance(),
                withTimeoutsAndBackoff).setApplicationName("fast-gw2").build();
    }

    /** Import only ranges present in DB (detail_tables + tables). */
    public void runFullImport() throws Exception {
        List<String> names = loadRangesFromDb();
        if (names.isEmpty()) {
            System.out.println("No ranges in DB (public.detail_tables / public.tables). Nothing to import.");
            return;
        }

        System.out.printf(Locale.ROOT, "Starting Sheets import: %d ranges in DB%n", names.size());

        // Optionally group by sheet (ranges like "SheetName!A1:B5")
        names = names.stream()
                .filter(this::looksReasonable)
                .sorted((a, b) -> sheetOf(a).compareTo(sheetOf(b)))
                .toList();

        var chunks = partition(names, RANGES_CHUNK_SIZE);

        int threads = Math.min(4, Math.max(1, Runtime.getRuntime().availableProcessors() / 2));
        ExecutorService pool = Executors.newFixedThreadPool(threads);

        // Shared counters
        final int[] updDetail = { 0 }, updMain = { 0 }, insMain = { 0 }, skipped = { 0 };

        List<Future<?>> futures = new ArrayList<>();
        for (int ci = 0; ci < chunks.size(); ci++) {
            final int chunkIndex = ci;
            final List<String> chunk = chunks.get(ci);

            futures.add(pool.submit(() -> {
                System.out.printf(Locale.ROOT, "Fetching chunk #%d (%d ranges) on %s...%n",
                        chunkIndex, chunk.size(), Thread.currentThread().getName());
                System.out.println(" ? Calling Google API for ranges: " + chunk);

                long t0 = System.currentTimeMillis();
                BatchGetValuesResponse batch;
                try {
                    batch = sheets.spreadsheets().values()
                            .batchGet(spreadsheetId)
                            .setRanges(chunk)
                            .setValueRenderOption("UNFORMATTED_VALUE")
                            .setDateTimeRenderOption("SERIAL_NUMBER")
                            .setFields("valueRanges(range,values)")
                            .execute();
                } catch (Exception e) {
                    System.err.printf(Locale.ROOT, " ! API call failed for chunk #%d: %s%n", chunkIndex,
                            e.getMessage());
                    return;
                }
                long t1 = System.currentTimeMillis();
                System.out.printf(Locale.ROOT, " ← API returned for chunk #%d in %.1f s%n",
                        chunkIndex, (t1 - t0) / 1000.0);

                List<ValueRange> vrs = Optional.ofNullable(batch.getValueRanges()).orElse(List.of());

                for (int i = 0; i < chunk.size(); i++) {
                    String namedRange = chunk.get(i);
                    System.out.println("   Processing range: " + namedRange);
                    ValueRange vr = (i < vrs.size()) ? vrs.get(i) : null;

                    if (vr == null) {
                        System.err.println("   ! Range not returned: " + namedRange);
                        increment(skipped);
                        continue;
                    }

                    List<List<Object>> grid = (vr.getValues() == null) ? List.of() : vr.getValues();
                    if (grid.isEmpty()) {
                        System.err.println("   ! Range has no values: " + namedRange);
                        increment(skipped);
                        continue;
                    }

                    List<Object> headerRow = grid.get(0);
                    if (headerRow == null || headerRow.isEmpty()) {
                        System.err.println("   ! Header row empty: " + namedRange);
                        increment(skipped);
                        continue;
                    }

                    List<String> headers = headerRow.stream()
                            .map(o -> String.valueOf(o == null ? "" : o).trim())
                            .collect(Collectors.toList());

                    List<Map<String, Object>> shaped = new ArrayList<>();
                    for (int r = 1; r < grid.size(); r++) {
                        List<Object> row = grid.get(r);
                        if (row == null || row.isEmpty())
                            continue;

                        int limit = Math.min(headers.size(), row.size());
                        Map<String, Object> obj = new LinkedHashMap<>(limit);
                        for (int c = 0; c < limit; c++) {
                            String h = headers.get(c);
                            Object v = row.get(c);
                            if (v == null)
                                continue;
                            if (isNumericHeader(h)) {
                                Number n = tryParseNumber(v);
                                obj.put(h, n != null ? n : v);
                            } else {
                                obj.put(h, v);
                            }
                        }
                        if (!obj.isEmpty())
                            shaped.add(obj);
                    }

                    String json;
                    try {
                        json = M.writeValueAsString(shaped);
                    } catch (Exception e) {
                        System.err.println("   ! JSON encode failed for " + namedRange + ": " + e.getMessage());
                        increment(skipped);
                        continue;
                    }

                    int nDetail = updateDetailByRange(namedRange, json);
                    if (nDetail > 0) {
                        add(updDetail, nDetail);
                        continue;
                    }

                    int nMain = updateMainByRange(namedRange, json);
                    if (nMain > 0) {
                        add(updMain, nMain);
                        continue;
                    }

                    insertMainByRange(namedRange, json);
                    increment(insMain);
                }
            }));
        }

        // wait for all
        for (Future<?> f : futures) {
            try {
                f.get();
            } catch (Exception e) {
                System.err.println("Worker failed: " + e.getMessage());
            }
        }
        pool.shutdown();

        System.out.printf(Locale.ROOT,
                "Sheets import done: detail=%d | main=%d | inserted=%d | skipped=%d%n",
                updDetail[0], updMain[0], insMain[0], skipped[0]);
    }

    // ---------- DB helpers ----------

    private static <T> List<List<T>> partition(List<T> src, int size) {
        List<List<T>> out = new ArrayList<>((src.size() + size - 1) / size);
        for (int i = 0; i < src.size(); i += size) {
            out.add(src.subList(i, Math.min(i + size, src.size())));
        }
        return out;
    }

    private List<String> loadRangesFromDb() {
        return Jpa.tx(em -> {
            var ranges = new LinkedHashSet<String>();

            var d = em.createNativeQuery("""
                        SELECT "range" FROM public.detail_tables WHERE "range" IS NOT NULL
                    """).getResultList();
            for (Object o : d)
                ranges.add(String.valueOf(o));

            var t = em.createNativeQuery("""
                        SELECT "range" FROM public.tables WHERE "range" IS NOT NULL
                    """).getResultList();
            for (Object o : t)
                ranges.add(String.valueOf(o));

            return new ArrayList<>(ranges);
        });
    }

    // ---------- basic guard for pathological ranges ----------
    private boolean looksReasonable(String a1) {
        if (a1 == null || a1.isBlank())
            return false;
        if (a1.matches("^[A-Za-z]+:[A-Za-z]+$"))
            return false; // whole-column A:B
        if (a1.matches("^\\d+:\\d+$"))
            return false; // whole-row 1:2
        return true;
    }

    // ---------- numeric hinting (no placeholders) ----------
    private static boolean isNumericHeader(String h) {
        if (h == null)
            return false;
        String k = h.trim();
        if (NUMERIC_HEADERS.contains(k))
            return true;
        String lower = k.toLowerCase(Locale.ROOT);
        return lower.endsWith("amount") || lower.endsWith("price") || lower.endsWith("profit")
                || lower.equals("buy") || lower.equals("sell") || lower.equals("duration")
                || lower.equals("qty") || lower.equals("quantity");
    }

    private static Number tryParseNumber(Object v) {
        if (v instanceof Number n)
            return n;
        try {
            String s = String.valueOf(v).trim().replace(',', '.');
            if (s.isEmpty())
                return null;
            if (s.matches("^-?\\d+$"))
                return Integer.parseInt(s);
            return Double.parseDouble(s);
        } catch (Exception ignored) {
            return null;
        }
    }

    // ---------- DB writes ----------
    private int updateDetailByRange(String range, String jsonRows) {
        return Jpa.tx(em -> em.createNativeQuery("""
                    UPDATE public.detail_tables
                       SET rows = :rows, updated_at = now()
                     WHERE "range" = :r
                """).setParameter("rows", jsonRows).setParameter("r", range).executeUpdate());
    }

    private int updateMainByRange(String range, String jsonRows) {
        return Jpa.tx(em -> em.createNativeQuery("""
                    UPDATE public.tables
                       SET rows = :rows, updated_at = now()
                     WHERE "range" = :r
                """).setParameter("rows", jsonRows).setParameter("r", range).executeUpdate());
    }

    private void insertMainByRange(String range, String jsonRows) {
        Jpa.txVoid(em -> em.createNativeQuery("""
                    INSERT INTO public.tables("range", rows, inserted_at, updated_at)
                    VALUES (:r, :rows, now(), now())
                """).setParameter("r", range).setParameter("rows", jsonRows).executeUpdate());
    }
}

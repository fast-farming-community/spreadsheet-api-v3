package eu.fast.gw2.tools;

import java.io.FileInputStream;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
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
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.regex.Pattern;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.api.client.googleapis.javanet.GoogleNetHttpTransport;
import com.google.api.client.http.HttpBackOffIOExceptionHandler;
import com.google.api.client.http.HttpBackOffUnsuccessfulResponseHandler;
import com.google.api.client.http.HttpRequest;
import com.google.api.client.http.HttpRequestInitializer;
import com.google.api.client.json.jackson2.JacksonFactory;
import com.google.api.client.util.ExponentialBackOff;
import com.google.api.services.sheets.v4.Sheets;
import com.google.api.services.sheets.v4.SheetsScopes;
import com.google.api.services.sheets.v4.model.BatchGetValuesResponse;
import com.google.api.services.sheets.v4.model.ValueRange;
import com.google.auth.http.HttpCredentialsAdapter;
import com.google.auth.oauth2.GoogleCredentials;

public class GoogleSheetsImporter implements AutoCloseable {

    private static final ObjectMapper M = new ObjectMapper();
    private final Sheets sheets;
    private final String spreadsheetId;

    private static final int CONNECT_TIMEOUT_MS = 10_000;
    private static final int READ_TIMEOUT_MS = 300_000;
    private static final int MAX_RETRIES = 5;
    private static final int RANGES_CHUNK_SIZE = 80;
    private static final int THREADS = 6;

    // used only to hint number parsing when a value is actually present
    private static final Set<String> NUMERIC_HEADERS = new LinkedHashSet<>(Arrays.asList(
            "Id", "AverageAmount", "TotalAmount", "TPBuyProfit", "TPSellProfit",
            "TPBuyProfitHr", "TPSellProfitHr", "Duration", "Price", "Buy", "Sell",
            "BuyPrice", "SellPrice", "Quantity", "Amount"));

    // Precompiled patterns to avoid re-compilation on hot paths
    private static final Pattern NON_ALNUM = Pattern.compile("[^A-Za-z0-9]");
    private static final Pattern TRAILING_NUM_SUFFIX = Pattern.compile("_(\\d+)$");

    // ---------- new concurrency state ----------
    private ExecutorService pool;
    private final List<Future<?>> futures = new ArrayList<>();
    private final AtomicInteger updDetail = new AtomicInteger(0);
    private final AtomicInteger updMain = new AtomicInteger(0);
    private final AtomicInteger insMain = new AtomicInteger(0);
    private final AtomicInteger skipped = new AtomicInteger(0);
    private volatile boolean started = false;

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
                JacksonFactory.getDefaultInstance(),
                withTimeoutsAndBackoff).setApplicationName("fast-gw2").build();
    }

    /**
     * Import only ranges present in DB (detail_tables + tables). Submits work and
     * returns immediately.
     */
    public void runFullImport() throws Exception {
        if (started) {
            System.out.println("GoogleSheetsImporter: runFullImport() already started; ignoring duplicate call.");
            return;
        }
        started = true;

        List<String> names = loadRangesFromDb();
        if (names.isEmpty()) {
            System.out.println("No ranges in DB (public.detail_tables / public.tables). Nothing to import.");
            return;
        }

        // Self-heal: drop DB rows that point to missing named ranges
        Set<String> existingNamedRanges = listNamedRanges();
        List<String> missingNamed = names.stream()
                .filter(n -> n != null && !n.isBlank())
                .filter(n -> !n.contains("!"))
                .filter(n -> !existingNamedRanges.contains(n))
                .toList();

        if (!missingNamed.isEmpty()) {
            int removed = deleteRangesInDb(missingNamed);
            System.out.printf(Locale.ROOT,
                    "Pruned %d DB rows for missing named ranges: %s%n", removed, missingNamed);
            names = names.stream().filter(n -> !missingNamed.contains(n)).toList();
        }

        final int totalRanges = names.size();
        System.out.printf(Locale.ROOT, "Starting Sheets import: %d ranges in DB%n", totalRanges);

        // sort/group
        names = names.stream()
                .filter(this::looksReasonable)
                .sorted((a, b) -> sheetOf(a).compareTo(sheetOf(b)))
                .toList();

        var chunks = partition(names, RANGES_CHUNK_SIZE);

        this.pool = Executors.newFixedThreadPool(THREADS);

        final AtomicInteger startedRanges = new AtomicInteger(0);

        for (int ci = 0; ci < chunks.size(); ci++) {
            final int chunkIndex = ci;
            final List<String> chunk = chunks.get(ci);

            futures.add(pool.submit(() -> {
                int soFar = startedRanges.addAndGet(chunk.size());
                System.out.printf(Locale.ROOT, "[%d / %d] Fetching chunk #%d on %s...%n",
                        soFar, totalRanges, chunkIndex, Thread.currentThread().getName());

                long t0 = System.currentTimeMillis();
                BatchGetValuesResponse batch;
                try {
                    batch = sheets.spreadsheets().values()
                            .batchGet(spreadsheetId)
                            .setRanges(chunk)
                            .setValueRenderOption("UNFORMATTED_VALUE")
                            .setDateTimeRenderOption("FORMATTED_STRING")
                            .setFields("valueRanges(range,values)")
                            .execute();
                } catch (Exception e) {
                    System.err.printf(Locale.ROOT, " ! API call failed for chunk #%d: %s%n", chunkIndex,
                            e.getMessage());
                    return;
                }
                long t1 = System.currentTimeMillis();
                System.out.printf(Locale.ROOT, " ← Chunk #%d done in %.1f s%n",
                        chunkIndex, (t1 - t0) / 1000.0);

                List<ValueRange> vrs = Optional.ofNullable(batch.getValueRanges()).orElse(Collections.emptyList());

                for (int i = 0; i < chunk.size(); i++) {
                    String namedRange = chunk.get(i);
                    ValueRange vr = (i < vrs.size()) ? vrs.get(i) : null;

                    if (vr == null) {
                        System.err.println("   ! Range not returned: " + namedRange);
                        skipped.incrementAndGet();
                        continue;
                    }

                    List<List<Object>> grid = (vr.getValues() == null) ? List.of() : vr.getValues();
                    if (grid.isEmpty()) {
                        System.err.println("   ! Range has no values: " + namedRange);
                        skipped.incrementAndGet();
                        continue;
                    }

                    List<Object> headerRow = grid.get(0);
                    if (headerRow == null || headerRow.isEmpty()) {
                        System.err.println("   ! Header row empty: " + namedRange);
                        skipped.incrementAndGet();
                        continue;
                    }

                    // headers
                    final int hdrSize = headerRow.size();
                    List<String> headers = new ArrayList<>(hdrSize);
                    for (int k = 0; k < hdrSize; k++) {
                        Object cell = headerRow.get(k);
                        String raw = String.valueOf(cell == null ? "" : cell).trim();
                        headers.add(normalizeHeader(raw));
                    }

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

                            String base = baseHeader(h);

                            // SPECIAL CASE: Duration / Notes as Google day fraction -> hh:mm:ss
                            if (("Duration".equals(base) || "Notes".equals(base)) && isPureNumber(v)) {
                                Double df = asDouble(v);
                                obj.put(h, (df != null) ? dayFractionToHms(df) : v);
                                continue;
                            }

                            // numeric hinting
                            if (isNumericHeaderBase(base)) {
                                Number n = tryParseNumber(v, base);
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
                        skipped.incrementAndGet();
                        continue;
                    }

                    int nDetail = updateDetailByRange(namedRange, json);
                    if (nDetail > 0) {
                        updDetail.addAndGet(nDetail);
                        continue;
                    }

                    int nMain = updateMainByRange(namedRange, json);
                    if (nMain > 0) {
                        updMain.addAndGet(nMain);
                        continue;
                    }
                    insMain.incrementAndGet();
                }
            }));
        }
        // NOTE: do NOT wait/print here. The caller will call awaitCompletion().
    }

    /** Block until all submitted chunks finished, then print the final summary. */
    public void awaitCompletion() {
        // Fast path: nothing was started
        if (!started) {
            return;
        }

        // Wait on task futures
        for (Future<?> f : futures) {
            try {
                f.get();
            } catch (Exception e) {
                System.err.println("Worker failed: " + e.getMessage());
            }
        }

        // Shut down the pool cleanly
        if (pool != null) {
            pool.shutdown();
            try {
                if (!pool.awaitTermination(60, TimeUnit.SECONDS)) {
                    pool.shutdownNow();
                }
            } catch (InterruptedException ie) {
                Thread.currentThread().interrupt();
                pool.shutdownNow();
            }
        }

        // Final summary
        System.out.printf(Locale.ROOT,
                "Sheets import done: detail=%d | main=%d | inserted=%d | skipped=%d%n",
                updDetail.get(), updMain.get(), insMain.get(), skipped.get());
    }

    @Override
    public void close() {
        // Safety valve: make sure we don't leak a pool if caller forgets to await.
        if (pool != null && !pool.isShutdown()) {
            pool.shutdownNow();
        }
    }

    // TEMP: mimic legacy header normalization (strip non-alphanumerics).
    private static String normalizeHeader(String h) {
        if (h == null)
            return "";
        return NON_ALNUM.matcher(h.trim()).replaceAll("");
    }

    private static String sheetOf(String a1) {
        int bang = a1.indexOf('!');
        if (bang <= 0)
            return ""; // unknown sheet -> sort first
        return a1.substring(0, bang);
    }

    // ---------- DB helpers ----------
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
    private static boolean isNumericHeaderBase(String k) {
        if (k == null)
            return false;
        String key = k.trim();
        if (NUMERIC_HEADERS.contains(key))
            return true;
        String lower = key.toLowerCase(Locale.ROOT);
        return lower.endsWith("amount") || lower.endsWith("price") || lower.endsWith("profit")
                || lower.equals("buy") || lower.equals("sell") || lower.equals("duration")
                || lower.equals("qty") || lower.equals("quantity");
    }

    private static Number tryParseNumber(Object v, String header) {
        if (v instanceof Number n) {
            if ("Id".equalsIgnoreCase(header)) {
                return n.longValue();
            }
            return n;
        }
        try {
            String s = String.valueOf(v).trim().replace(',', '.');
            if (s.isEmpty())
                return null;

            if ("Id".equalsIgnoreCase(header)) {
                double d = Double.parseDouble(s);
                return (long) d;
            }

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

    private static <T> List<List<T>> partition(List<T> src, int size) {
        List<List<T>> out = new ArrayList<>((src.size() + size - 1) / size);
        for (int i = 0; i < src.size(); i += size) {
            out.add(src.subList(i, Math.min(i + size, src.size())));
        }
        return out;
    }

    private static String baseHeader(String h) {
        if (h == null)
            return "";
        return TRAILING_NUM_SUFFIX.matcher(h).replaceAll("");
    }

    private static boolean isPureNumber(Object v) {
        if (v instanceof Number)
            return true;
        if (v == null)
            return false;
        String s = String.valueOf(v).trim().replace(',', '.');
        return s.matches("^-?\\d+(\\.\\d+)?$");
    }

    private static Double asDouble(Object v) {
        if (v instanceof Number n)
            return n.doubleValue();
        if (v == null)
            return null;
        try {
            String s = String.valueOf(v).trim().replace(',', '.');
            if (s.isEmpty())
                return null;
            return Double.parseDouble(s);
        } catch (Exception ignored) {
            return null;
        }
    }

    private static String dayFractionToHms(double df) {
        long totalSeconds = Math.round(df * 86400.0);
        long h = totalSeconds / 3600;
        long m = (totalSeconds % 3600) / 60;
        long s = (totalSeconds % 60);
        return String.format(Locale.ROOT, "%02d:%02d:%02d", h, m, s);
    }

    // List all named ranges in the spreadsheet (names only)
    private Set<String> listNamedRanges() throws Exception {
        var meta = sheets.spreadsheets().get(spreadsheetId)
                .setFields("namedRanges(name)")
                .execute();
        var nrs = Optional.ofNullable(meta.getNamedRanges()).orElse(Collections.emptyList());
        Set<String> names = new java.util.HashSet<>(nrs.size());
        for (var nr : nrs) {
            if (nr.getName() != null && !nr.getName().isBlank()) {
                names.add(nr.getName());
            }
        }
        return names;
    }

    // Remove rows whose "range" matches any of the given values (from both tables)
    private int deleteRangesInDb(java.util.Collection<String> ranges) {
        if (ranges == null || ranges.isEmpty())
            return 0;
        return Jpa.tx(em -> {
            int sum = 0;
            for (String r : ranges) {
                sum += em.createNativeQuery("""
                            DELETE FROM public.detail_tables WHERE "range" = :r
                        """).setParameter("r", r).executeUpdate();
                sum += em.createNativeQuery("""
                            DELETE FROM public.tables       WHERE "range" = :r
                        """).setParameter("r", r).executeUpdate();
            }
            return sum;
        });
    }
}

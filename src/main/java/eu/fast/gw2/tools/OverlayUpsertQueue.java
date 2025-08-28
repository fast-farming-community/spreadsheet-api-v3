package eu.fast.gw2.tools;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;

import eu.fast.gw2.dao.OverlayDao;

public final class OverlayUpsertQueue implements AutoCloseable {

    private static final int DEFAULT_BATCH = 512;
    private static final int DEFAULT_QUEUE_CAP = 4096;
    private static final int COALESCE_MS = 250;
    private static final boolean DEFAULT_USE_BATCH = true;

    private final LinkedBlockingQueue<Upsert> q;
    private final int batch;
    private final boolean useBatch;

    private volatile boolean stop = false;
    private Thread thread;

    private static final class Upsert {
        final boolean isMain;
        final long fid;
        final String keyOrName; // detail: key, main: "pageId|name"
        final String tier;
        final String json;

        Upsert(boolean isMain, long fid, String keyOrName, String tier, String json) {
            this.isMain = isMain;
            this.fid = fid;
            this.keyOrName = keyOrName;
            this.tier = tier;
            this.json = json;
        }

        String dedupeKey() {
            // keep composite key for mains to dedupe correctly across pages
            return isMain ? ("M|" + keyOrName + "|" + tier) : ("D|" + fid + "|" + keyOrName + "|" + tier);
        }
    }

    private OverlayUpsertQueue(int batch, boolean useBatch, int capacity) {
        this.batch = Math.max(1, batch);
        this.useBatch = useBatch;
        this.q = new LinkedBlockingQueue<>(Math.max(1024, capacity));
    }

    public static OverlayUpsertQueue startDefault() {
        OverlayUpsertQueue w = new OverlayUpsertQueue(DEFAULT_BATCH, DEFAULT_USE_BATCH, DEFAULT_QUEUE_CAP);
        w.start();
        return w;
    }

    private void start() {
        thread = new Thread(() -> {
            long t0 = System.currentTimeMillis();
            int flushed = 0;
            final ArrayList<Upsert> buf = new ArrayList<>(batch);

            try {
                while (!stop || !q.isEmpty()) {
                    Upsert first = q.poll(COALESCE_MS, TimeUnit.MILLISECONDS);
                    if (first == null)
                        continue;
                    buf.add(first);
                    q.drainTo(buf, batch - buf.size());

                    // in-batch de-dupe (last wins)
                    LinkedHashMap<String, Upsert> uniq = new LinkedHashMap<>(buf.size() * 2);
                    for (Upsert u : buf)
                        uniq.put(u.dedupeKey(), u);

                    if (useBatch) {
                        // --- batch buffers ---
                        List<Integer> mainPageIds = new ArrayList<>();
                        List<String> mainNames = new ArrayList<>();
                        List<String> mainTiers = new ArrayList<>();
                        List<String> mainJsons = new ArrayList<>();

                        List<Long> detFids = new ArrayList<>();
                        List<String> detKeys = new ArrayList<>();
                        List<String> detTiers = new ArrayList<>();
                        List<String> detJsons = new ArrayList<>();

                        for (Upsert u : uniq.values()) {
                            if (u.isMain) {
                                // keyOrName is "pageId|name"
                                int bar = (u.keyOrName == null) ? -1 : u.keyOrName.indexOf('|');
                                int pageId;
                                String name;
                                if (bar > 0) {
                                    pageId = safeParseInt(u.keyOrName.substring(0, bar), 0);
                                    name = u.keyOrName.substring(bar + 1);
                                } else {
                                    // fallback: unknown page → 0, keep entire as name
                                    pageId = 0;
                                    name = u.keyOrName;
                                }
                                mainPageIds.add(pageId);
                                mainNames.add(name);
                                mainTiers.add(u.tier);
                                mainJsons.add(u.json);
                            } else {
                                detFids.add(u.fid);
                                detKeys.add(u.keyOrName);
                                detTiers.add(u.tier);
                                detJsons.add(u.json);
                            }
                        }

                        try {
                            if (!mainNames.isEmpty())
                                // REQUIRE: OverlayDao has overload that accepts pageIds
                                flushed += OverlayDao.upsertMainBatch(mainPageIds, mainNames, mainTiers, mainJsons);
                            if (!detKeys.isEmpty())
                                flushed += OverlayDao.upsertDetailBatch(detFids, detKeys, detTiers, detJsons);
                        } catch (Throwable batchEx) {
                            System.err
                                    .println("Overlay WRITER: batch failed; fallback per-row: " + batchEx.getMessage());
                            for (Upsert u : uniq.values()) {
                                try {
                                    if (u.isMain) {
                                        int bar = (u.keyOrName == null) ? -1 : u.keyOrName.indexOf('|');
                                        int pageId;
                                        String name;
                                        if (bar > 0) {
                                            pageId = safeParseInt(u.keyOrName.substring(0, bar), 0);
                                            name = u.keyOrName.substring(bar + 1);
                                        } else {
                                            pageId = 0;
                                            name = u.keyOrName;
                                        }
                                        // REQUIRE: OverlayDao has per-row overload with pageId
                                        OverlayDao.upsertMain(pageId, name, u.tier, u.json);
                                    } else {
                                        OverlayDao.upsertDetail(u.fid, u.keyOrName, u.tier, u.json);
                                    }
                                    flushed++;
                                } catch (Exception e) {
                                    System.err.printf("Overlay WRITER: upsert failed (%s/%s): %s%n",
                                            u.isMain ? "main" : "detail", u.keyOrName, e.getMessage());
                                }
                            }
                        }
                    } else {
                        for (Upsert u : uniq.values()) {
                            try {
                                if (u.isMain) {
                                    int bar = (u.keyOrName == null) ? -1 : u.keyOrName.indexOf('|');
                                    int pageId;
                                    String name;
                                    if (bar > 0) {
                                        pageId = safeParseInt(u.keyOrName.substring(0, bar), 0);
                                        name = u.keyOrName.substring(bar + 1);
                                    } else {
                                        pageId = 0;
                                        name = u.keyOrName;
                                    }
                                    // REQUIRE: OverlayDao has per-row overload with pageId
                                    OverlayDao.upsertMain(pageId, name, u.tier, u.json);
                                } else {
                                    OverlayDao.upsertDetail(u.fid, u.keyOrName, u.tier, u.json);
                                }
                                flushed++;
                            } catch (Exception e) {
                                System.err.printf("Overlay WRITER: upsert failed (%s/%s): %s%n",
                                        u.isMain ? "main" : "detail", u.keyOrName, e.getMessage());
                            }
                        }
                    }

                    buf.clear();
                }
            } catch (InterruptedException ie) {
                Thread.currentThread().interrupt();
            } finally {
                long ms = System.currentTimeMillis() - t0;
                System.out.printf(java.util.Locale.ROOT,
                        "Overlay WRITER: flushed %d upserts in %.1fs (batch=%d, useBatch=%s)%n",
                        flushed, ms / 1000.0, batch, useBatch);
            }
        }, "overlay-upsert-writer");
        thread.setDaemon(true);
        thread.start();
    }

    public void enqueueMain(String compositeKey /* 'pageId|name' */, String tierLabel, String json) {
        q.offer(new Upsert(true, 0L, compositeKey, tierLabel, json));
    }

    public void enqueueDetail(long fid, String key, String tierLabel, String json) {
        q.offer(new Upsert(false, fid, key, tierLabel, json));
    }

    @Override
    public void close() {
        stop = true;
        if (thread != null) {
            try {
                thread.join();
            } catch (InterruptedException ie) {
                Thread.currentThread().interrupt();
            }
        }
    }

    // ---------- helpers ----------

    private static int safeParseInt(String s, int def) {
        try {
            return Integer.parseInt(s.trim());
        } catch (Exception ignored) {
            return def;
        }
    }
}

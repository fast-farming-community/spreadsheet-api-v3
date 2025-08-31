package eu.fast.gw2.main;

import java.util.concurrent.atomic.AtomicBoolean;

import eu.fast.gw2.http.HttpApi;
import eu.fast.gw2.tools.Jpa;

public class RunService {
    // default: 2 minutes
    private static final long PERIOD_MS = 2 * 60_000L;
    private static final AtomicBoolean running = new AtomicBoolean(false);
    private static volatile boolean stop = false;

    public static void main(String[] args) {
        // Confirm DB at startup
        try {
            String info = Jpa.tx(em -> (String) em.createNativeQuery(
                    "select current_database() || ' @ ' || inet_server_addr() || ':' || inet_server_port()")
                    .getSingleResult());
            System.out.println("[INFO] DB CONNECTED: " + info);
        } catch (Exception e) {
            System.err.println("[INFO] DB CHECK FAILED: " + e.getMessage());
        }

        // --- start HTTP API (non-blocking) ---
        try {
            HttpApi.start(); // listens on API_BIND:API_PORT
        } catch (Exception e) {
            System.err.println("[ERROR] HTTP API START FAILED: " + e.getMessage());
            // safe hard fail
            System.exit(1);
        }

        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            stop = true;
            try {
                HttpApi.stop();
            } catch (Exception ignored) {
            }
        }, "gw2-shutdown"));

        long next = alignToNextTick(System.currentTimeMillis(), PERIOD_MS);
        System.out.println("[INFO] GW2 Service: started, period=" + (PERIOD_MS / 1000.0) + " s");

        while (!stop) {
            long now = System.currentTimeMillis();
            if (now < next) {
                sleep(Math.min(1000L, next - now));
                continue;
            }

            if (running.compareAndSet(false, true)) {
                long started = System.currentTimeMillis();
                try {
                    RunPrices.runPrices();
                } catch (Throwable t) {
                    t.printStackTrace(System.err);
                } finally {
                    running.set(false);
                }
                next += PERIOD_MS; // fixed cadence; no catch-up burst
                double durationSec = (System.currentTimeMillis() - started) / 1000.0;
                System.out.println("[INFO] GW2 Service: run finished in " + String.format("%.1f", durationSec) + " s");
            } else {
                System.out.println("[INFO] GW2 Service: skip tick (previous run still in progress)");
                next += PERIOD_MS;
            }
        }
        System.out.println("[INFO] GW2 Service: stopping.");
    }

    private static long alignToNextTick(long now, long period) {
        return now - (now % period) + period; // align to :00, :02, :04, ...
    }

    private static void sleep(long ms) {
        try {
            Thread.sleep(ms);
        } catch (InterruptedException ie) {
            Thread.currentThread().interrupt();
        }
    }
}

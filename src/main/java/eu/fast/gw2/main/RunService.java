package eu.fast.gw2.main;

import java.util.concurrent.atomic.AtomicBoolean;

import eu.fast.gw2.tools.Jpa;

public class RunService {
    // default: 5 minutes (override with -Dservice.period.ms=300000)
    private static final long PERIOD_MS = Long.getLong("service.period.ms", 5 * 60_000L);
    private static final AtomicBoolean running = new AtomicBoolean(false);
    private static volatile boolean stop = false;

    public static void main(String[] args) {
        // Confirm DB at startup (optional but helpful)
        try {
            String info = Jpa.tx(em -> (String) em.createNativeQuery(
                    "select current_database() || ' @ ' || inet_server_addr() || ':' || inet_server_port()")
                    .getSingleResult());
            System.out.println("DB CONNECTED: " + info);
        } catch (Exception e) {
            System.err.println("DB CHECK FAILED: " + e.getMessage());
        }

        Runtime.getRuntime().addShutdownHook(new Thread(() -> stop = true, "gw2-shutdown"));

        long next = alignToNextTick(System.currentTimeMillis(), PERIOD_MS);
        System.out.println("GW2 Service: started, period=" + PERIOD_MS + " ms");

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
                System.out.println("GW2 Service: run finished in " + (System.currentTimeMillis() - started) + " ms");
            } else {
                System.out.println("GW2 Service: skip tick (previous run still in progress)");
                next += PERIOD_MS;
            }
        }
        System.out.println("GW2 Service: stopping.");
    }

    private static long alignToNextTick(long now, long period) {
        return now - (now % period) + period; // align to :00, :05, :10, ...
    }

    private static void sleep(long ms) {
        try {
            Thread.sleep(ms);
        } catch (InterruptedException ie) {
            Thread.currentThread().interrupt();
        }
    }
}

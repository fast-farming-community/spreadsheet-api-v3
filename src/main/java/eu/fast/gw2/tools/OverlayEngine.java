package eu.fast.gw2.tools;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import eu.fast.gw2.enums.Tier;

public class OverlayEngine {

    // Public entry: recompute & persist overlays for all tiers
    public static void recomputeAndPersistAllOverlays() {
        final boolean PROFILE = true;
        final int TIER_THREADS = 3;
        final Tier[] TIERS = { Tier.T2M, Tier.T10M, Tier.T60M };

        // single run profiler (aggregates tiers + problem log)
        OverlayProfiler.Run run = new OverlayProfiler.Run();

        // writer with batching & in-queue de-dupe (own thread)
        try (OverlayUpsertQueue writer = OverlayUpsertQueue.startDefault()) {

            // Preload & plan once for the full run (fills caches for all tiers)
            OverlayRunPlanner.Plan plan = OverlayRunPlanner.plan(TIERS);

            ExecutorService pool = Executors.newFixedThreadPool(Math.min(TIER_THREADS, Math.max(1, TIERS.length)));
            try {
                for (Tier t : TIERS) {
                    pool.submit(new OverlayTierRunner(
                            t,
                            plan.detailTargets(),
                            plan.mainTargets(),
                            writer,
                            run,
                            PROFILE));
                }
            } finally {
                pool.shutdown();
                try {
                    pool.awaitTermination(7, TimeUnit.MINUTES);
                } catch (InterruptedException ie) {
                    Thread.currentThread().interrupt();
                }
            }
        }

        // One combined line + problems (only if any)
        run.printRunSummaryAndProblems();
    }
}

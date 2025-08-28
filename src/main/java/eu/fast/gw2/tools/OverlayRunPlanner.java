package eu.fast.gw2.tools;

import java.util.HashSet;
import java.util.List;
import java.util.Set;

import eu.fast.gw2.enums.Tier;

public final class OverlayRunPlanner {

    public record Plan(List<Object[]> detailTargets, List<String> mainTargets) {
    }

    /** Preloads calcs, targets, base rows, and warms caches for all tiers. */
    public static Plan plan(Tier[] tiers) {
        // Preload formulas
        OverlayCalc.preloadAll();
        var detailTargets = OverlayDBAccess.listDetailTargets();
        var mainTargets = OverlayDBAccess.listMainTargets();

        // preload
        Set<String> allDetailKeys = new HashSet<>();
        for (Object[] r : detailTargets) {
            String key = (String) r[1];
            if (key != null && !key.isBlank())
                allDetailKeys.add(key);
        }
        OverlayCache.preloadDetailRows(allDetailKeys);
        OverlayCache.preloadMainRows(mainTargets);

        // Warm shared caches (images/rarities once; prices per tier)
        Set<Integer> allIds = OverlayCache.collectAllItemIdsFromPreloaded();
        OverlayCache.getOrFillImageCache(allIds);
        OverlayCache.getOrFillRarityCache(allIds);
        for (Tier t : tiers) {
            OverlayCache.getOrFillPriceCache(allIds, t);
        }

        return new Plan(detailTargets, mainTargets);
    }

    private OverlayRunPlanner() {
    }
}

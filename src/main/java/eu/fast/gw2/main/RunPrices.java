package eu.fast.gw2.main;

import java.util.Optional;

import eu.fast.gw2.enums.Tier;
import eu.fast.gw2.tools.OverlayEngine;
import eu.fast.gw2.tools.RefreshTierPrices;

public class RunPrices {

    private static final int OVERLAY_SLEEP_MS = Integer
            .parseInt(Optional.ofNullable(System.getenv("OVERLAY_SLEEP_MS")).orElse("150"));

    private static final int GW2API_SLEEP_MS = Integer
            .parseInt(Optional.ofNullable(System.getenv("GW2API_SLEEP_MS")).orElse("150"));

    private static final Tier[] TIERS = { Tier.T5M, Tier.T15M, Tier.T60M };

    public static void main(String[] args) throws Exception {
        System.out.println(">>> RunPrices starting…");

        // Refresh GW2 tier prices
        RefreshTierPrices.refresh(null, GW2API_SLEEP_MS);

        // Recompute overlays (detail_tables + main tables) for all tiers
        OverlayEngine.recomputeAndPersistAllOverlays(TIERS, OVERLAY_SLEEP_MS);

        System.out.println("RunPrices done.");
    }
}

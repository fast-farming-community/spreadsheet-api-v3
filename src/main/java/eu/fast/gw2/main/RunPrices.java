package eu.fast.gw2.main;

import java.util.Optional;

import eu.fast.gw2.enums.Tier;
import eu.fast.gw2.tools.OverlayEngine;

public class RunPrices {

    private static final int SLEEP_MS = Integer
            .parseInt(Optional.ofNullable(System.getenv("OVERLAY_SLEEP_MS")).orElse("150"));

    private static final Tier[] TIERS = { Tier.T5M, Tier.T10M, Tier.T15M, Tier.T60M };

    public static void main(String[] args) throws Exception {
        System.out.println(">>> RunPrices starting…");

        // Comment out for reusing cached DB data.
        RunRefreshTierPrices.main(new String[] {});

        // detail_tables + main tables for all tiers.
        OverlayEngine.recomputeAndPersistAllOverlays(TIERS, SLEEP_MS);

        System.out.println("RunPrices done.");
    }

}

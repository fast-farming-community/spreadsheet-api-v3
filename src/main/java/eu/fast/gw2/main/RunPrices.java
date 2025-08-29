package eu.fast.gw2.main;

import java.util.Optional;

import eu.fast.gw2.tools.OverlayEngine;
import eu.fast.gw2.tools.RefreshTierPrices;

public class RunPrices {
    private static final int GW2API_SLEEP_MS = Integer
            .parseInt(Optional.ofNullable(System.getenv("GW2API_SLEEP_MS")).orElse("150"));

    public static void main(String[] args) throws Exception {
        runPrices();
    }

    public static void runPrices() throws Exception {
        RefreshTierPrices.refresh(null, GW2API_SLEEP_MS);
        OverlayEngine.recomputeAndPersistAllOverlays();
    }

}

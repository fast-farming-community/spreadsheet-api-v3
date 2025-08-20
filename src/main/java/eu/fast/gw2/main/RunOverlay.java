package eu.fast.gw2.main;

import eu.fast.gw2.dynamic.OverlayEngine;

public class RunOverlay {
    public static void main(String[] args) {

        if (args.length < 2) {
            System.err.println("Usage: RunOverlay <detailFeatureId> <tableKey>");
            System.exit(2);
        }
        long fid = Long.parseLong(args[0]);
        String key = args[1];
        
        System.out.println(">>> RunOverlay starting (fid=" + fid + ", key=" + key + ")");

        OverlayEngine.recompute(fid, key);

        // Debug
        OverlayDebug.printStats(fid, key);

        System.out.println("Recompute done for fid=" + fid + " key=" + key);
    }
}

package eu.fast.gw2.main;

import eu.fast.gw2.dynamic.OverlayEngine;
import eu.fast.gw2.jpa.HibernateUtil;

public class RunOverlay {
    public static void main(String[] args) {

        if (args.length < 2) {
            System.err.println("Usage: RunOverlay <detailFeatureId> <tableKey>");
            System.exit(2);
        }
        long fid = Long.parseLong(args[0]);
        String key = args[1];

        System.out.println(">>> RunOverlay starting (fid=" + fid + ", key=" + key + ")");
        System.out.println(">>> RunOverlay starting (fid=" + args[0] + ", key=" + args[1] + ")");
        System.out.println(">>> Using HibernateUtil from: " + HibernateUtil.class.getName());

        // force EMF init now so we see the bootstrap path
        HibernateUtil.emf();

        OverlayEngine.recompute(fid, key);

        // Debug
        DebugOverlay.printStats(fid, key);

        System.out.println("Recompute done for fid=" + fid + " key=" + key);
    }
}

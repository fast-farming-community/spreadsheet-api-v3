package eu.fast.gw2.main;

import eu.fast.gw2.patreon.PatreonSyncService;

public class RunPatreonSync {
    public static void main(String[] args) {
        try {
            var changed = PatreonSyncService.runSync();
            System.out.printf("Patreon sync done: upgraded=%d, downgraded=%d%n", changed.upgraded(),
                    changed.downgraded());
        } catch (Exception e) {
            System.err.println("Patreon sync failed: " + e.getMessage());
            e.printStackTrace(System.err);
            System.exit(1);
        }
    }
}

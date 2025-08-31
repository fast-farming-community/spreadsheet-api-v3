package eu.fast.gw2.patreon;

import java.util.Locale;
import java.util.Map;
import java.util.Set;

import eu.fast.gw2.tools.Jpa;
import jakarta.persistence.EntityManager;

public final class PatreonSyncService {

    public record Result(int upgraded, int downgraded) {
    }

    public static Result runSync() {
        final Map<String, Integer> patrons = PatreonClient.fetchCurrentPatrons();

        final int premiumMin = Integer.parseInt(System.getenv().getOrDefault("PATREON_PREMIUM_MIN_CENTS", "500"));
        final Set<String> emails = patrons.keySet();

        return Jpa.tx(em -> {
            int up = 0, down = 0;

            // 1) Upgrade or set roles for current patrons
            for (Map.Entry<String, Integer> e : patrons.entrySet()) {
                String email = e.getKey().toLowerCase(Locale.ROOT).trim();
                int cents = e.getValue() == null ? 0 : e.getValue();

                String targetRole = (cents >= premiumMin) ? "tribune" : "legionnaire";
                up += applyRoleIfNeeded(em, email, targetRole);
            }

            // 2) Downgrade ex-patrons to soldier
            down += downgradeMissing(em, emails);

            return new Result(up, down);
        });
    }

    private static int applyRoleIfNeeded(EntityManager em, String email, String role) {
        String sql = """
                    UPDATE public.users u
                    SET role_id = :role, updated_at = now()
                    WHERE lower(u.email) = :email
                      AND (u.role_id IS DISTINCT FROM :role)
                """;
        int n = em.createNativeQuery(sql)
                .setParameter("role", role)
                .setParameter("email", email)
                .executeUpdate();
        if (n == 0) {
            // user might not exist yet → do nothing (or insert a stub if you want)
            return 0;
        }
        return n;
    }

    private static int downgradeMissing(EntityManager em, Set<String> currentEmails) {
        if (currentEmails.isEmpty()) {
            // if no patrons at all, downgrade everyone in paid roles
            String sql = """
                        UPDATE public.users
                        SET role_id = 'soldier', updated_at = now()
                        WHERE role_id IN ('legionnaire','tribune','khan-ur','champion')
                    """;
            return em.createNativeQuery(sql).executeUpdate();
        } else {
            String sql = """
                        UPDATE public.users u
                        SET role_id = 'soldier', updated_at = now()
                        WHERE u.role_id IN ('legionnaire','tribune','khan-ur','champion')
                          AND lower(u.email) NOT IN (:emails)
                    """;
            return em.createNativeQuery(sql)
                    .setParameter("emails", currentEmails.stream().map(s -> s.toLowerCase(Locale.ROOT).trim()).toList())
                    .executeUpdate();
        }
    }

    private PatreonSyncService() {
    }
}

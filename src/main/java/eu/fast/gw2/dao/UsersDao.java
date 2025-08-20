package eu.fast.gw2.dao;

import eu.fast.gw2.jpa.Jpa;

public class UsersDao {

    public static Long upsertByEmail(String email, String password, String token,
            boolean verified, String roleNameOrNull) {
        return Jpa.tx(em -> ((Number) em.createNativeQuery("""
                  INSERT INTO public.users(email, password, token, verified, role_id, inserted_at, updated_at)
                  VALUES (:e, :p, :t, :v, :r, now(), now())
                  ON CONFLICT (email) DO UPDATE
                    SET password = EXCLUDED.password,
                        token    = EXCLUDED.token,
                        verified = EXCLUDED.verified,
                        role_id  = EXCLUDED.role_id,
                        updated_at = now()
                  RETURNING id
                """)
                .setParameter("e", email)
                .setParameter("p", password)
                .setParameter("t", token)
                .setParameter("v", verified)
                .setParameter("r", roleNameOrNull)
                .getSingleResult()).longValue());
    }
}

package eu.fast.gw2.dao;

import eu.fast.gw2.tools.Jpa;

public class RolesDao {
    public static void ensure(String name) {
        Jpa.tx(em -> em.createNativeQuery("""
                  INSERT INTO public.roles(name, inserted_at, updated_at)
                  VALUES (:n, now(), now())
                  ON CONFLICT (name) DO UPDATE SET updated_at=now()
                """).setParameter("n", name).executeUpdate());
    }
}

package eu.fast.gw2.dao;

import eu.fast.gw2.tools.Jpa;

public class SchemaMigrationsDao {
    public static void ensureApplied(long version) {
        Jpa.tx(em -> em.createNativeQuery("""
                  INSERT INTO public.schema_migrations(version, inserted_at)
                  VALUES (:v, now())
                  ON CONFLICT (version) DO NOTHING
                """).setParameter("v", version).executeUpdate());
    }
}

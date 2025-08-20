package eu.fast.gw2.dao;

import eu.fast.gw2.jpa.Jpa;

public class TablesDao {
    public static void upsert(long pageId, String name, Integer orderIndex,
            boolean published, String range, String rowsJson, String descriptionOrNull) {
        Jpa.tx(em -> em
                .createNativeQuery(
                        """
                                  INSERT INTO public.tables(page_id, name, "order", published, range, rows, description, inserted_at, updated_at)
                                  VALUES (:pid, :nm, :ord, :pub, :rg, :rows, :desc, now(), now())
                                  ON CONFLICT (page_id, name) DO UPDATE
                                    SET "order"   = EXCLUDED."order",
                                        published = EXCLUDED.published,
                                        range     = EXCLUDED.range,
                                        rows      = EXCLUDED.rows,
                                        description = EXCLUDED.description,
                                        updated_at = now()
                                """)
                .setParameter("pid", pageId)
                .setParameter("nm", name)
                .setParameter("ord", orderIndex)
                .setParameter("pub", published)
                .setParameter("rg", range)
                .setParameter("rows", rowsJson)
                .setParameter("desc", descriptionOrNull)
                .executeUpdate());
    }
}

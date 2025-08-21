package eu.fast.gw2.dao;

import java.util.HashSet;
import java.util.List;
import java.util.Set;

import eu.fast.gw2.jpa.Jpa;

public class TrackedIdsDao {
    /** Distinct positive Ids from the latest row per key */
    public static Set<Integer> collectFromDetailTables() {
        return Jpa.tx(em -> {
            // Pull all latest rows blobs (fast enough; index helps)
            @SuppressWarnings("unchecked")
            List<String> jsons = em.createNativeQuery("""
                        SELECT dt.rows
                        FROM public.detail_tables dt
                        WHERE dt.id IN (
                          SELECT DISTINCT ON (key) id
                          FROM public.detail_tables
                          ORDER BY key, id DESC
                        )
                    """).getResultList();

            Set<Integer> ids = new HashSet<>();
            for (String js : jsons) {
                if (js == null || js.isBlank())
                    continue;
                // quick scan: split on "Id": to avoid full JSON walk here
                int idx = 0;
                while ((idx = js.indexOf("\"Id\":", idx)) >= 0) {
                    idx += 5;
                    int j = idx;
                    while (j < js.length() && Character.isWhitespace(js.charAt(j)))
                        j++;
                    int sign = 1;
                    if (j < js.length() && js.charAt(j) == '-') {
                        sign = -1;
                        j++;
                    }
                    int val = 0;
                    boolean any = false;
                    while (j < js.length() && Character.isDigit(js.charAt(j))) {
                        any = true;
                        val = val * 10 + (js.charAt(j) - '0');
                        j++;
                    }
                    if (any) {
                        int id = sign * val;
                        if (id > 0)
                            ids.add(id);
                    }
                    idx = j;
                }
            }
            return ids;
        });
    }
}

package eu.fast.gw2.main;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;

import eu.fast.gw2.jpa.Jpa;

import java.util.*;

public class DebugOverlay {
    private static final ObjectMapper M = new ObjectMapper();

    // Column constants (same as OverlayEngine)
    private static final String COL_TPB = "TPBuyProfit";
    private static final String COL_TPS = "TPSellProfit";
    private static final String COL_KEY = "Key";
    private static final String COL_NAME = "Name";

    /** Prints a quick summary of the table after recompute. */
    public static void printStats(long detailFeatureId, String tableKey) {
        String json = Jpa.tx(em -> {
            var r = em.createNativeQuery("""
                        SELECT rows
                          FROM public.detail_tables
                         WHERE detail_feature_id = :fid AND key = :k
                         ORDER BY updated_at DESC
                         LIMIT 1
                    """).setParameter("fid", detailFeatureId)
                    .setParameter("k", tableKey)
                    .getResultList();
            return r.isEmpty() ? null : (String) r.get(0);
        });

        if (json == null || json.isBlank()) {
            System.out.println("[OverlayDebug] No rows found for fid=" + detailFeatureId + " key=" + tableKey);
            return;
        }

        List<Map<String, Object>> rows = parse(json);
        int totalRows = rows.size();
        int updated = 0;
        long sumBuy = 0, sumSell = 0;
        Map<String, Object> totalRow = null;

        for (var r : rows) {
            Integer b = asInt(r.get(COL_TPB));
            Integer s = asInt(r.get(COL_TPS));
            if (b != null || s != null) {
                updated++;
                sumBuy += (b == null ? 0 : b);
                sumSell += (s == null ? 0 : s);
            }
            String k = str(r.get(COL_KEY));
            String n = str(r.get(COL_NAME));
            if ("TOTAL".equalsIgnoreCase(k) || "TOTAL".equalsIgnoreCase(n)) {
                totalRow = r;
            }
        }

        System.out.println("— Overlay Debug —");
        System.out.println("Table: fid=" + detailFeatureId + " key=" + tableKey);
        System.out.println("Rows total: " + totalRows + " | rows with TP fields: " + updated);
        System.out.println("Sum(TPBuyProfit)  = " + sumBuy);
        System.out.println("Sum(TPSellProfit) = " + sumSell);
        if (totalRow != null) {
            System.out.println("TOTAL row present:");
            System.out.println("  TPBuyProfit=" + asInt(totalRow.get(COL_TPB)) +
                    "  TPSellProfit=" + asInt(totalRow.get(COL_TPS)));
        } else {
            System.out.println("TOTAL row: (none)");
        }
        System.out.println("— end —");
    }

    // helpers
    private static List<Map<String, Object>> parse(String json) {
        try {
            return M.readValue(json, new TypeReference<List<Map<String, Object>>>() {
            });
        } catch (Exception e) {
            throw new RuntimeException("Failed to parse detail_tables.rows JSON", e);
        }
    }

    private static Integer asInt(Object v) {
        if (v == null)
            return null;
        if (v instanceof Number n)
            return n.intValue();
        try {
            return (int) Math.floor(Double.parseDouble(String.valueOf(v).replace(',', '.')));
        } catch (Exception e) {
            return null;
        }
    }

    private static String str(Object o) {
        return o == null ? null : String.valueOf(o);
    }
}

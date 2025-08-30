package eu.fast.gw2.main;

import eu.fast.gw2.tools.GoogleSheetsImporter;
import eu.fast.gw2.tools.SeedCalculations;

public class RunGoogleSheets {

    private static final String SHEET_ID = "1WdwWxyP9zeJhcxoQAr-paMX47IuK6l5rqAPYDOA8mho";

    public static void main(String[] args) throws Exception {
        // 1) Import Google Sheets → upsert into public.tables/detail_tables
        try (GoogleSheetsImporter importer = new GoogleSheetsImporter(SHEET_ID)) {
            importer.runFullImport();
            importer.awaitCompletion();
        } catch (Exception e) {
            System.err.println("! GoogleSheets import failed: " + e.getMessage());
            return;
        }

        // 2) Seed AFTER importer finished
        SeedCalculations.run();
    }
}

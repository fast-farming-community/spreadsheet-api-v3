package eu.fast.gw2;

import eu.fast.gw2.dao.DetailFeaturesDao;
import eu.fast.gw2.dao.DetailTablesDao;
import eu.fast.gw2.dao.FeaturesDao;
import eu.fast.gw2.dao.PagesDao;

public class Bootstrap {
  public static void main(String[] args) {
    long dfId = DetailFeaturesDao.ensure("Sheets v3");
    long featureId = FeaturesDao.ensure("Frontend Sheets");
    PagesDao.upsert(featureId, "Materials Overview", true);

    String rowsJson = """
          [{"Id":1,"Name":"Coin","AverageAmount":192.48,"TPBuyProfit":192.48,"TPSellProfit":192.48}]
        """;
    DetailTablesDao.upsert(dfId, "fast_materials", "Fast Materials", "Materials!A2:K", rowsJson, null);

    System.out.println("Bootstrap OK");
  }
}

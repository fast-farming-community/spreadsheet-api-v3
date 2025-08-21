package eu.fast.gw2.tools;

import java.util.HashMap;
import java.util.Map;

import javax.sql.DataSource;

import org.hibernate.jpa.HibernatePersistenceProvider;

import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;

import jakarta.persistence.EntityManagerFactory;

public final class HibernateUtil {
    private static EntityManagerFactory emf;

    private HibernateUtil() {
    }

    public static synchronized EntityManagerFactory emf() {
        if (emf != null)
            return emf;

        String url = System.getenv("PG_URL");
        String user = System.getenv("PG_USER");
        String pass = System.getenv("PG_PASS");
        if (url == null || user == null || pass == null) {
            throw new IllegalStateException("Missing PG_URL/PG_USER/PG_PASS");
        }

        System.out.println(">> HibernateUtil: programmatic bootstrap (no persistence.xml)");

        DataSource ds = dataSource(url, user, pass);

        Map<String, Object> hashMap = new HashMap<>();
        hashMap.put("hibernate.connection.datasource", ds); // programmatic DataSource
        hashMap.put("hibernate.dialect", "org.hibernate.dialect.PostgreSQLDialect");
        hashMap.put("hibernate.hbm2ddl.auto", "validate");
        hashMap.put("hibernate.show_sql", "false");
        hashMap.put("hibernate.format_sql", "false");
        hashMap.put("hibernate.jdbc.time_zone", "UTC");
        hashMap.put("hibernate.archive.autodetection", "none");
        // Register your @Entity classes explicitly:
        hashMap.put(org.hibernate.cfg.AvailableSettings.LOADED_CLASSES, java.util.List.of(
                eu.fast.gw2.model.About.class,
                eu.fast.gw2.model.Contributor.class,
                eu.fast.gw2.model.DetailFeature.class,
                eu.fast.gw2.model.DetailTable.class,
                eu.fast.gw2.model.Feature.class,
                eu.fast.gw2.model.Page.class,
                eu.fast.gw2.model.Guide.class,
                eu.fast.gw2.model.MetadataRow.class,
                eu.fast.gw2.model.Role.class,
                eu.fast.gw2.model.SchemaMigration.class,
                eu.fast.gw2.model.TableEntry.class,
                eu.fast.gw2.model.User.class));

        // IMPORTANT: this is the programmatic path; it does NOT look for
        // persistence.xml
        emf = new HibernatePersistenceProvider().createEntityManagerFactory("gw2PU", hashMap);

        if (emf == null) {
            throw new IllegalStateException("Hibernate EMF bootstrap failed (returned null).");
        }
        System.out.println(">> HibernateUtil: EMF initialized");
        return emf;
    }

    private static DataSource dataSource(String url, String user, String pass) {
        var cfg = new HikariConfig();
        cfg.setJdbcUrl(url);
        cfg.setUsername(user);
        cfg.setPassword(pass);
        cfg.setMaximumPoolSize(5);
        cfg.setPoolName("hibernate-hikari");
        return new HikariDataSource(cfg);
    }
}

package eu.fast.gw2.tools;

import java.util.HashMap;
import java.util.Map;

import javax.sql.DataSource;

import org.hibernate.jpa.HibernatePersistenceProvider;

import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;

import jakarta.persistence.EntityManagerFactory;

/**
 * Programmatic Hibernate bootstrap:
 * - Uses Hikari DataSource (no persistence.xml needed)
 * - Reads DB creds from env: PG_URL / PG_USER / PG_PASS
 * - Registers entity classes explicitly to avoid classpath scanning
 */
public final class HibernateUtil {
    private static volatile EntityManagerFactory EMF;

    private HibernateUtil() {
    }

    public static synchronized EntityManagerFactory emf() {
        if (EMF != null)
            return EMF;

        // Read credentials from environment
        final String url = System.getenv("PG_URL");
        final String user = System.getenv("PG_USER");
        final String pass = System.getenv("PG_PASS");
        if (url == null || user == null || pass == null) {
            throw new IllegalStateException("Missing PG_URL/PG_USER/PG_PASS");
        }

        System.out.println(">> HibernateUtil: programmatic bootstrap (no persistence.xml)");

        // Build Hikari DataSource
        DataSource ds = dataSource(url, user, pass);

        // Properties override any persistence.xml PU named "gw2PU"
        Map<String, Object> props = new HashMap<>();
        props.put("hibernate.connection.datasource", ds);
        props.put("hibernate.hbm2ddl.auto", "validate");
        props.put("hibernate.show_sql", "false");
        props.put("hibernate.format_sql", "false");
        props.put("hibernate.jdbc.time_zone", "UTC");
        props.put("hibernate.archive.autodetection", "none");

        // Explicit entity registration
        props.put(org.hibernate.cfg.AvailableSettings.LOADED_CLASSES, java.util.List.of(
                eu.fast.gw2.model.About.class,
                eu.fast.gw2.model.Contributor.class,
                eu.fast.gw2.model.DetailFeature.class,
                eu.fast.gw2.model.DetailTable.class,
                eu.fast.gw2.model.Feature.class,
                eu.fast.gw2.model.Page.class,
                eu.fast.gw2.model.Guide.class,
                eu.fast.gw2.model.MetadataRow.class,
                eu.fast.gw2.model.Role.class,
                eu.fast.gw2.model.TableEntry.class,
                eu.fast.gw2.model.User.class));

        // Create EMF overriding "gw2PU"
        EMF = new HibernatePersistenceProvider().createEntityManagerFactory("gw2PU", props);
        if (EMF == null) {
            throw new IllegalStateException("Hibernate EMF bootstrap failed (returned null).");
        }
        System.out.println(">> HibernateUtil: EMF initialized");
        return EMF;
    }

    private static DataSource dataSource(String url, String user, String pass) {
        HikariConfig cfg = new HikariConfig();
        cfg.setJdbcUrl(url);
        cfg.setUsername(user);
        cfg.setPassword(pass);
        cfg.setMaximumPoolSize(5);
        cfg.setPoolName("hibernate-hikari");
        return new HikariDataSource(cfg);
    }
}

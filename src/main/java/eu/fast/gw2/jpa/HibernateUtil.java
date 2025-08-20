package eu.fast.gw2.jpa;

import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;
import jakarta.persistence.EntityManagerFactory;
import org.hibernate.jpa.HibernatePersistenceProvider;

import javax.sql.DataSource;
import java.util.HashMap;
import java.util.Map;

public final class HibernateUtil {
    private static EntityManagerFactory emf;

    private HibernateUtil() {
    }

    public static EntityManagerFactory emf() {
        if (emf == null) {
            String url = System.getenv("PG_URL");
            String user = System.getenv("PG_USER");
            String pass = System.getenv("PG_PASS");
            if (url == null || user == null || pass == null) {
                throw new IllegalStateException("Missing PG_URL/PG_USER/PG_PASS");
            }

            DataSource ds = dataSource(url, user, pass);

            Map<String, Object> p = new HashMap<>();
            // === JPA/Hibernate settings (Hibernate 6.x) ===
            p.put("hibernate.connection.datasource", ds); // Option B: provide our Hikari DataSource
            p.put("hibernate.dialect", "org.hibernate.dialect.PostgreSQLDialect");
            p.put("hibernate.hbm2ddl.auto", "validate"); // never create/alter prod schema
            p.put("hibernate.show_sql", "true");
            p.put("hibernate.format_sql", "true");
            p.put("hibernate.jdbc.time_zone", "UTC");
            p.put("hibernate.archive.autodetection", "none"); // we explicitly list classes

            // Register all annotated @Entity classes here
            p.put(org.hibernate.cfg.AvailableSettings.LOADED_CLASSES, java.util.List.of(
                    eu.fast.gw2.model.About.class,
                    eu.fast.gw2.model.Contributor.class,
                    eu.fast.gw2.model.DetailFeature.class,
                    eu.fast.gw2.model.DetailTable.class,
                    eu.fast.gw2.model.Feature.class,
                    eu.fast.gw2.model.Page.class,
                    eu.fast.gw2.model.Guide.class,
                    eu.fast.gw2.model.Item.class,
                    eu.fast.gw2.model.MetadataRow.class,
                    eu.fast.gw2.model.Role.class,
                    eu.fast.gw2.model.SchemaMigration.class,
                    eu.fast.gw2.model.TableEntry.class,
                    eu.fast.gw2.model.User.class));

            // Build an EntityManagerFactory (JPA)
            emf = new HibernatePersistenceProvider().createEntityManagerFactory("default", p);
        }
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

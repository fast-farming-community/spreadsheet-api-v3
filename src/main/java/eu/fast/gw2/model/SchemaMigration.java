package eu.fast.gw2.model;

import jakarta.persistence.*;
import java.time.LocalDateTime;

@Entity
@Table(name = "schema_migrations", schema = "public")
public class SchemaMigration {
    @Id
    public Long version;

    @Column(name = "inserted_at")
    public LocalDateTime insertedAt;
}

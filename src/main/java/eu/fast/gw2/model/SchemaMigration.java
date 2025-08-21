package eu.fast.gw2.model;

import java.time.LocalDateTime;

import jakarta.persistence.Column;
import jakarta.persistence.Entity;
import jakarta.persistence.Id;
import jakarta.persistence.Table;

@Entity
@Table(name = "schema_migrations", schema = "public")
public class SchemaMigration {
    @Id
    public Long version;

    @Column(name = "inserted_at")
    public LocalDateTime insertedAt;
}

package eu.fast.gw2.model;

import java.time.LocalDateTime;

import jakarta.persistence.Column;
import jakarta.persistence.Entity;
import jakarta.persistence.GeneratedValue;
import jakarta.persistence.GenerationType;
import jakarta.persistence.Id;
import jakarta.persistence.Lob;
import jakarta.persistence.Table;

@Entity
@Table(name = "metadata", schema = "public")
public class MetadataRow {
    @Id
    @GeneratedValue(strategy = GenerationType.IDENTITY)
    public Long id;

    @Lob
    @Column(columnDefinition = "text")
    public String data;
    @Column(length = 255)
    public String name;

    @Column(name = "inserted_at", nullable = false)
    public LocalDateTime insertedAt;
    @Column(name = "updated_at", nullable = false)
    public LocalDateTime updatedAt;
}

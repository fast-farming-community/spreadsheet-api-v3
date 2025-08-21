package eu.fast.gw2.model;

import java.time.LocalDateTime;

import jakarta.persistence.Column;
import jakarta.persistence.Entity;
import jakarta.persistence.FetchType;
import jakarta.persistence.ForeignKey;
import jakarta.persistence.GeneratedValue;
import jakarta.persistence.GenerationType;
import jakarta.persistence.Id;
import jakarta.persistence.JoinColumn;
import jakarta.persistence.ManyToOne;
import jakarta.persistence.Table;
import jakarta.persistence.UniqueConstraint;

@Entity
@Table(name = "pages", schema = "public", uniqueConstraints = @UniqueConstraint(name = "pages_unique_id", columnNames = {
        "feature_id", "name" }))
public class Page {
    @Id
    @GeneratedValue(strategy = GenerationType.IDENTITY)
    public Long id;

    @ManyToOne(fetch = FetchType.LAZY, optional = false)
    @JoinColumn(name = "feature_id", foreignKey = @ForeignKey(name = "pages_feature_id_fkey"))
    public Feature feature;

    @Column(nullable = false, length = 255)
    public String name;

    @Column
    public Boolean published;

    @Column(name = "inserted_at", nullable = false)
    public LocalDateTime insertedAt;

    @Column(name = "updated_at", nullable = false)
    public LocalDateTime updatedAt;
}

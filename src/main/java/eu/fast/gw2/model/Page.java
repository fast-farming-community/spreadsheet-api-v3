package eu.fast.gw2.model;

import jakarta.persistence.*;
import java.time.LocalDateTime;

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

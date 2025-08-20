package eu.fast.gw2.model;

import jakarta.persistence.*;
import java.time.LocalDateTime;

@Entity
@Table(name = "features", schema = "public", uniqueConstraints = @UniqueConstraint(name = "features_unique_id", columnNames = {
        "name" }))
public class Feature {
    @Id
    @GeneratedValue(strategy = GenerationType.IDENTITY)
    public Long id;

    @Column(nullable = false, length = 255)
    public String name;

    @Column
    public Boolean published;

    @Column(name = "inserted_at", nullable = false)
    public LocalDateTime insertedAt;

    @Column(name = "updated_at", nullable = false)
    public LocalDateTime updatedAt;
}

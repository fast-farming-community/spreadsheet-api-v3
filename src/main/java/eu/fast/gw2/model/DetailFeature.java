package eu.fast.gw2.model;

import jakarta.persistence.*;
import java.time.LocalDateTime;

@Entity
@Table(name = "detail_features", schema = "public",
       uniqueConstraints = @UniqueConstraint(name="detail_features_unique_id", columnNames = {"name"}))
public class DetailFeature {
  @Id
  @GeneratedValue(strategy = GenerationType.IDENTITY)
  public Long id;

  @Column(nullable=false, length=255)
  public String name;

  @Column public Boolean published;

  @Column(name="inserted_at", nullable=false)
  public LocalDateTime insertedAt;

  @Column(name="updated_at", nullable=false)
  public LocalDateTime updatedAt;
}

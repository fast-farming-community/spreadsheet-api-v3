package eu.fast.gw2.model;

import java.time.LocalDateTime;

import jakarta.persistence.Column;
import jakarta.persistence.Entity;
import jakarta.persistence.GeneratedValue;
import jakarta.persistence.GenerationType;
import jakarta.persistence.Id;
import jakarta.persistence.Table;
import jakarta.persistence.UniqueConstraint;

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

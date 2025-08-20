package eu.fast.gw2.model;

import jakarta.persistence.*;
import java.time.LocalDateTime;

@Entity
@Table(name = "detail_tables", schema = "public", uniqueConstraints = @UniqueConstraint(name = "detail_tables_unique_id", columnNames = {
    "detail_feature_id", "key" }))
public class DetailTable {
  @Id
  @GeneratedValue(strategy = GenerationType.IDENTITY)
  public Long id;

  @Column(columnDefinition = "text")
  public String description;

  @ManyToOne(fetch = FetchType.LAZY)
  @JoinColumn(name = "detail_feature_id", foreignKey = @ForeignKey(name = "detail_tables_detail_feature_id_fkey"))
  public DetailFeature feature;

  @Column(length = 255)
  public String key;
  @Column(length = 255)
  public String name;
  @Column(length = 255)
  public String range;

  @Lob
  @Column(columnDefinition = "text", nullable = false)
  public String rows; // JSON as TEXT

  @Column(name = "inserted_at", nullable = false)
  public LocalDateTime insertedAt;

  @Column(name = "updated_at", nullable = false)
  public LocalDateTime updatedAt;
}

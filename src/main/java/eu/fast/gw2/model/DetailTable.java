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
import jakarta.persistence.Lob;
import jakarta.persistence.ManyToOne;
import jakarta.persistence.Table;
import jakarta.persistence.UniqueConstraint;

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

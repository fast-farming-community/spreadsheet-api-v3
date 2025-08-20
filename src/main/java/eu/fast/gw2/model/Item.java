package eu.fast.gw2.model;

import jakarta.persistence.*;
import java.time.LocalDateTime;

@Entity
@Table(name = "items", schema = "public")
public class Item {
  @Id
  public Integer id;

  public Integer buy;
  @Column(name = "chat_link")
  public String chatLink;
  public String icon;
  public Integer level;
  public String name;
  public String rarity;
  public Integer sell;
  public Boolean tradable;
  public String type;
  @Column(name = "vendor_value")
  public Integer vendorValue;

  @Column(name = "inserted_at", nullable = false)
  public LocalDateTime insertedAt;
  @Column(name = "updated_at", nullable = false)
  public LocalDateTime updatedAt;
}

package eu.fast.gw2.model;

import jakarta.persistence.*;
import java.time.LocalDateTime;

@Entity
@Table(name = "guides", schema = "public")
public class Guide {
    @Id
    @GeneratedValue(strategy = GenerationType.IDENTITY)
    public Long id;

    @Column(length = 255)
    public String farmtrain;
    @Column(length = 255)
    public String image;
    @Lob
    @Column(columnDefinition = "text")
    public String info;

    public Boolean published;
    @Column(length = 255)
    public String title;

    @Column(name = "inserted_at", nullable = false)
    public LocalDateTime insertedAt;
    @Column(name = "updated_at", nullable = false)
    public LocalDateTime updatedAt;

    @Column(name = "order")
    public Integer orderIndex;
}

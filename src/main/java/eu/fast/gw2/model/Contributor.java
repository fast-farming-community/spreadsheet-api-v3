package eu.fast.gw2.model;

import jakarta.persistence.*;
import java.time.LocalDateTime;

@Entity
@Table(name = "contributors", schema = "public")
public class Contributor {
    @Id
    @GeneratedValue(strategy = GenerationType.IDENTITY)
    public Long id;

    @Column(length = 255)
    public String name;
    public Boolean published;
    @Column(length = 255)
    public String type;

    @Column(name = "inserted_at", nullable = false)
    public LocalDateTime insertedAt;
    @Column(name = "updated_at", nullable = false)
    public LocalDateTime updatedAt;
}

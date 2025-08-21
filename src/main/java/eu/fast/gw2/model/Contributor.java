package eu.fast.gw2.model;

import java.time.LocalDateTime;

import jakarta.persistence.Column;
import jakarta.persistence.Entity;
import jakarta.persistence.GeneratedValue;
import jakarta.persistence.GenerationType;
import jakarta.persistence.Id;
import jakarta.persistence.Table;

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

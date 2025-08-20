package eu.fast.gw2.model;

import jakarta.persistence.*;
import java.time.LocalDateTime;

@Entity
@Table(name = "tables", schema = "public", uniqueConstraints = @UniqueConstraint(name = "tables_unique_id", columnNames = {
        "page_id", "name" }))
public class TableEntry {
    @Id
    @GeneratedValue(strategy = GenerationType.IDENTITY)
    public Long id;

    @Lob
    @Column(columnDefinition = "text")
    public String description;

    @ManyToOne(fetch = FetchType.LAZY)
    @JoinColumn(name = "page_id", foreignKey = @ForeignKey(name = "tables_page_id_fkey"))
    public Page page;

    @Column(length = 255)
    public String name;
    @Column(name = "order")
    public Integer orderIndex;
    public Boolean published;
    @Column(length = 255)
    public String range;

    @Lob
    @Column(columnDefinition = "text")
    public String rows;

    @Column(name = "inserted_at", nullable = false)
    public LocalDateTime insertedAt;
    @Column(name = "updated_at", nullable = false)
    public LocalDateTime updatedAt;
}

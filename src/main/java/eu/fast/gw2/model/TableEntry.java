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

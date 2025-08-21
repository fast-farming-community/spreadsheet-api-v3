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
import jakarta.persistence.ManyToOne;
import jakarta.persistence.Table;
import jakarta.persistence.UniqueConstraint;

@Entity
@Table(name = "users", schema = "public", uniqueConstraints = @UniqueConstraint(name = "users_unique_id", columnNames = {
        "email" }))
public class User {
    @Id
    @GeneratedValue(strategy = GenerationType.IDENTITY)
    public Long id;

    @Column(length = 255)
    public String email;
    @Column(length = 255)
    public String password;
    @Column(length = 255)
    public String token;
    public Boolean verified;

    @ManyToOne(fetch = FetchType.LAZY)
    @JoinColumn(name = "role_id", referencedColumnName = "name", foreignKey = @ForeignKey(name = "users_role_id_fkey"))
    public Role role;

    @Column(name = "inserted_at", nullable = false)
    public LocalDateTime insertedAt;
    @Column(name = "updated_at", nullable = false)
    public LocalDateTime updatedAt;
}

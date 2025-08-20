package eu.fast.gw2.model;

import jakarta.persistence.*;
import java.time.LocalDateTime;

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

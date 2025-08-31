// file: src/main/java/eu/fast/gw2/http/HttpApi.java
package eu.fast.gw2.http;

import java.time.LocalDateTime;
import java.util.Map;
import java.util.UUID;
import java.util.regex.Pattern;

import at.favre.lib.crypto.bcrypt.BCrypt;
import eu.fast.gw2.model.Role;
import eu.fast.gw2.model.User;
import eu.fast.gw2.tools.Jpa;
import io.javalin.Javalin;
import io.javalin.http.Context;
import io.javalin.json.JavalinJackson;
import jakarta.persistence.EntityManager;

public final class HttpApi {
    private static Javalin app;

    private static final String FRONTEND_BASE = System.getenv().getOrDefault("FRONTEND_BASE_URL",
            "https://fast.farming-community.eu");
    private static final int BCRYPT_COST = Integer.parseInt(System.getenv().getOrDefault("BCRYPT_COST", "12"));

    // Password policy: ≥12, upper, lower, digit, special (same as Angular)
    private static final Pattern PASS_POLICY = Pattern.compile(
            "^(?=.*?[A-Z])(?=.*?[a-z])(?=.*?[0-9])(?=.*?[#?!@$%^&*\\-]).{12,}$");

    public static void start() {
        if (app != null)
            return;

        final String bind = System.getenv().getOrDefault("API_BIND", "127.0.0.1");
        final int port = Integer.parseInt(System.getenv().getOrDefault("API_PORT", "4010"));

        app = Javalin.create(cfg -> {
            cfg.jetty.defaultHost = bind;
            cfg.jsonMapper(new JavalinJackson());
        });

        // Health
        app.get("/healthz", ctx -> ctx.json(Map.of("ok", true)));

        // ---- AUTH (phase 1) ----
        app.post("/auth/pre-register", HttpApi::preRegister);
        app.post("/auth/register", HttpApi::register);

        // ---- AUTH (phase 2) ----
        app.post("/auth/login", HttpApi::login);
        app.post("/auth/refresh", HttpApi::refresh);
        app.get("/auth/me", HttpApi::me);

        app.start(port);
        System.out.println("HTTP API listening on " + bind + ":" + port);
    }

    public static void stop() {
        if (app != null) {
            app.stop();
            app = null;
            System.out.println("HTTP API stopped");
        }
    }

    // ---------- Handlers ----------

    // body: { "email": "user@example.com" }
    private static void preRegister(Context ctx) {
        var body = ctx.bodyAsClass(PreRegisterReq.class);
        String email = norm(body.email);
        if (email == null || !email.matches("^[\\w\\-\\.]+@([\\w\\-]+\\.)+[\\w\\-]{2,4}$")) {
            ctx.status(400).json(Map.of("error", "invalid_email"));
            return;
        }

        String token = UUID.randomUUID().toString();

        boolean alreadyVerified = Jpa.tx(em -> {
            User existing = findByEmail(em, email);
            if (existing != null && Boolean.TRUE.equals(existing.verified)) {
                return true; // already registered
            }
            if (existing == null) {
                existing = new User();
                existing.email = email;
                existing.insertedAt = LocalDateTime.now();
            }
            existing.password = null; // not set yet
            existing.token = token;
            existing.verified = false;
            existing.updatedAt = LocalDateTime.now();
            existing.role = null; // set on register

            if (existing.id == null)
                em.persist(existing);
            else
                em.merge(existing);
            return false;
        });

        if (alreadyVerified) {
            ctx.status(409).json(Map.of("success", false, "error", "already_registered"));
            return;
        }

        String link = FRONTEND_BASE + "/auth/register?email=" + urlEnc(email) + "&token=" + urlEnc(token);
        // TEST MODE: log the link instead of emailing
        System.out.println("[pre-register] confirmation link: " + link);

        ctx.json(Map.of("success", true));
    }

    // body: { email, password, password_confirmation, token }
    private static void register(Context ctx) {
        var body = ctx.bodyAsClass(RegisterReq.class);
        String email = norm(body.email);
        String pass = body.password;
        String pass2 = body.password_confirmation;
        String token = body.token;

        if (email == null || token == null || token.isBlank()) {
            ctx.status(400).json(Map.of("error", "invalid_token"));
            return;
        }
        if (pass == null || !pass.equals(pass2)) {
            ctx.status(400).json(Map.of("error", "password_mismatch"));
            return;
        }
        if (!PASS_POLICY.matcher(pass).matches()) {
            ctx.status(400).json(Map.of("error", "weak_password"));
            return;
        }

        var created = Jpa.tx(em -> {
            User u = findByEmail(em, email);
            if (u == null || Boolean.TRUE.equals(u.verified) || u.token == null || !u.token.equals(token)) {
                throw new BadRequest("invalid_token");
            }
            // set role to "soldier" at registration
            Role soldier = em.getReference(Role.class, "soldier");

            String hash = BCrypt.withDefaults().hashToString(BCRYPT_COST, pass.toCharArray());

            u.password = hash;
            u.token = null;
            u.verified = true;
            u.role = soldier;
            u.updatedAt = LocalDateTime.now();
            em.merge(u);

            return new SimpleUser(u.id, u.email, u.role == null ? "soldier" : u.role.name);
        });

        // Issue tokens (we’ll add refresh/login endpoints next; for now return both)
        var tokens = Tokens.issue(created.email(), created.role());

        ctx.json(Map.of(
                "access", tokens.access(),
                "refresh", tokens.refresh()));
    }

    // ---------- Helpers ----------

    private record PreRegisterReq(String email) {
    }

    private record RegisterReq(String email, String password, String password_confirmation, String token) {
    }

    private record SimpleUser(Long id, String email, String role) {
    }

    private static User findByEmail(EntityManager em, String email) {
        var q = em.createQuery("SELECT u FROM User u WHERE u.email = :e", User.class);
        q.setParameter("e", email);
        var list = q.getResultList();
        return list.isEmpty() ? null : list.getFirst();
    }

    private static String norm(String s) {
        return s == null ? null : s.trim().toLowerCase();
    }

    private static String urlEnc(String s) {
        try {
            return java.net.URLEncoder.encode(s, java.nio.charset.StandardCharsets.UTF_8);
        } catch (Exception e) {
            return s;
        }
    }

    // Minimal exception bridge
    private static final class BadRequest extends RuntimeException {
        BadRequest(String m) {
            super(m);
        }
    }

    // ====== AUTH PHASE 2 ======

    private static final int MAX_ATTEMPTS = 5;
    private static final long LOCK_MS = 15 * 60 * 1000L; // 15 min

    // failed login attempts: key=email, value=info
    private static final java.util.concurrent.ConcurrentHashMap<String, FailedLogin> FAILS = new java.util.concurrent.ConcurrentHashMap<>();

    private record FailedLogin(int attempts, long lockedUntil) {
    }

    private static void login(Context ctx) {
        var body = ctx.bodyAsClass(LoginReq.class);
        String email = norm(body.email);
        String pass = body.password;

        if (email == null || pass == null) {
            ctx.status(400).json(Map.of("error", "missing_fields"));
            return;
        }

        // Check lockout
        FailedLogin info = FAILS.get(email);
        if (info != null && info.lockedUntil > System.currentTimeMillis()) {
            long left = (info.lockedUntil - System.currentTimeMillis()) / 1000;
            ctx.status(429).json(Map.of("error", "locked", "retry_after_sec", left));
            return;
        }

        // Verify user
        var user = Jpa.tx(em -> findByEmail(em, email));
        if (user == null || !Boolean.TRUE.equals(user.verified) || user.password == null) {
            registerFail(email);
            ctx.status(401).json(Map.of("error", "invalid_credentials"));
            return;
        }

        var result = at.favre.lib.crypto.bcrypt.BCrypt.verifyer().verify(pass.toCharArray(), user.password);
        if (!result.verified) {
            registerFail(email);
            ctx.status(401).json(Map.of("error", "invalid_credentials"));
            return;
        }

        // success: reset fails
        FAILS.remove(email);

        var role = (user.role == null ? "soldier" : user.role.name);
        var tokens = Tokens.issue(email, role);
        ctx.json(Map.of("access", tokens.access(), "refresh", tokens.refresh()));
    }

    private static void registerFail(String email) {
        long now = System.currentTimeMillis();
        FAILS.compute(email, (k, v) -> {
            if (v == null)
                return new FailedLogin(1, 0);
            int next = v.attempts + 1;
            if (next >= MAX_ATTEMPTS) {
                return new FailedLogin(next, now + LOCK_MS);
            }
            return new FailedLogin(next, 0);
        });
    }

    private static void refresh(Context ctx) {
        var body = ctx.bodyAsClass(RefreshReq.class);
        if (body.token == null) {
            ctx.status(400).json(Map.of("error", "missing_token"));
            return;
        }

        var email = Tokens.verify(body.token, true); // verify refresh
        if (email == null) {
            ctx.status(401).json(Map.of("error", "invalid_token"));
            return;
        }

        // fetch user to get current role
        var user = Jpa.tx(em -> findByEmail(em, email));
        if (user == null || !Boolean.TRUE.equals(user.verified)) {
            ctx.status(401).json(Map.of("error", "user_not_found"));
            return;
        }

        var role = (user.role == null ? "soldier" : user.role.name);
        var pair = Tokens.issue(email, role);
        ctx.json(Map.of("access", pair.access()));
    }

    private static void me(Context ctx) {
        String auth = ctx.header("Authorization");
        if (auth == null || !auth.startsWith("Bearer ")) {
            ctx.status(401).json(Map.of("error", "missing_token"));
            return;
        }
        String token = auth.substring(7);

        var email = Tokens.verify(token, false); // access token
        if (email == null) {
            ctx.status(401).json(Map.of("error", "invalid_token"));
            return;
        }

        var user = Jpa.tx(em -> findByEmail(em, email));
        if (user == null) {
            ctx.status(401).json(Map.of("error", "user_not_found"));
            return;
        }

        var role = (user.role == null ? "soldier" : user.role.name);
        ctx.json(Map.of(
                "email", user.email,
                "role", role,
                "verified", user.verified));
    }

    private record LoginReq(String email, String password) {
    }

    private record RefreshReq(String token) {
    }

}

package eu.fast.gw2.http;

import java.time.LocalDateTime;
// added imports for overlay handlers
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.regex.Pattern;

import org.postgresql.util.PGobject;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;

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

    private static final String FRONTEND_BASE = "https://fast.farming-community.eu";
    private static final int BCRYPT_COST = 12;

    // Password policy: ≥12, upper, lower, digit, special
    private static final Pattern PASS_POLICY = Pattern.compile(
            "^(?=.*?[A-Z])(?=.*?[a-z])(?=.*?[0-9])(?=.*?[#?!@$%^&*\\-]).{12,}$");

    // shared JSON mapper for detail parsing
    private static final ObjectMapper M = new ObjectMapper();

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

        // ---- AUTH ----
        app.post("/auth/pre-register", HttpApi::preRegister);
        app.post("/auth/register", HttpApi::register);
        app.post("/auth/login", HttpApi::login);
        app.post("/auth/refresh", HttpApi::refresh);
        app.get("/auth/me", HttpApi::me);

        // ---- OVERLAYS (tier-gated, no fallback) ----
        app.get("/api/v1/details/{module}/{collection}/{item}", HttpApi::getDetailOverlayItem);
        app.get("/api/v1/{feature}/{page}", HttpApi::getMainOverlay);

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

        // Issue tokens
        var tokens = Tokens.issue(created.email(), created.role());

        ctx.json(Map.of(
                "access", tokens.access(),
                "refresh", tokens.refresh()));
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

    // ---------- Overlay handlers ----------

    // MAIN LIST: /api/v1/:feature/:page (returns rows JSON as-is)
    private static void getMainOverlay(Context ctx) {
        String feature = ctx.pathParam("feature"); // e.g. "open-world"
        String page = ctx.pathParam("page"); // e.g. "alt-parking"
        Tier tier = tierForRequest(ctx);

        Integer pageId = resolvePageId(feature, page);
        if (pageId == null) {
            ctx.status(404).json(Map.of("error", "not_found", "why", "page"));
            return;
        }

        // Fetch ALL tables for this page & tier and concatenate their arrays
        List<Object> dbVals = Jpa.tx(em -> em.createNativeQuery("""
                    SELECT rows
                      FROM public.tables_overlay
                     WHERE page_id = :pid
                       AND tier = :t
                     ORDER BY key ASC
                """)
                .setParameter("pid", pageId)
                .setParameter("t", tier.label())
                .getResultList());

        if (dbVals == null || dbVals.isEmpty()) {
            ctx.status(404).json(Map.of(
                    "error", "not_found",
                    "why", "overlay_tier_missing",
                    "tier", tier.label()));
            return;
        }

        try {
            java.util.ArrayList<java.util.Map<String, Object>> out = new java.util.ArrayList<>(1024);
            for (Object dv : dbVals) {
                String rowsJson = asJsonString(dv); // your helper that handles PGobject/String
                if (rowsJson == null || rowsJson.isBlank())
                    continue;
                var arr = M.readValue(rowsJson,
                        new com.fasterxml.jackson.core.type.TypeReference<java.util.List<java.util.Map<String, Object>>>() {
                        });
                if (arr != null && !arr.isEmpty())
                    out.addAll(arr);
            }
            ctx.json(out);
        } catch (Exception e) {
            ctx.status(500).json(Map.of("error", "bad_overlay_json"));
        }
    }

    // DETAIL ITEM: /api/v1/details/:module/:collection/:item
    // mirrors v2 semantics, but pulls from *_overlay and enforces tier +
    // association
    private static void getDetailOverlayItem(Context ctx) {
        String module = ctx.pathParam("module"); // e.g. "farming-details"
        String collection = ctx.pathParam("collection"); // e.g. "bava-nisos-farmtrain"
        String itemKey = ctx.pathParam("item"); // e.g. "bouncy-chest-event-bava-nisos"
        String moduleBase = module.endsWith("-details") ? module.substring(0, module.length() - 8) : module;

        Tier tier = tierForRequest(ctx);

        Long dfId = resolveDetailFeatureId(moduleBase);
        if (dfId == null) {
            ctx.status(404).json(Map.of("error", "not_found", "why", "detail_feature"));
            return;
        }

        String rows = Jpa.tx(em -> {
            java.util.List<Object> rs = em.createNativeQuery("""
                        SELECT rows
                          FROM public.detail_tables_overlay
                         WHERE detail_feature_id = :dfid
                           AND key = :k
                           AND tier = :t
                         LIMIT 1
                    """)
                    .setParameter("dfid", dfId)
                    .setParameter("k", collection)
                    .setParameter("t", tier.label())
                    .getResultList();
            if (rs.isEmpty())
                return null;
            return asJsonString(rs.get(0));
        });

        if (rows == null) {
            ctx.status(404).json(Map.of("error", "not_found", "why", "overlay_tier_missing", "tier", tier.label()));
            return;
        }

        // rows is a JSON array; return the single element with "Key" == itemKey
        try {
            List<Map<String, Object>> arr = M.readValue(rows, new TypeReference<List<Map<String, Object>>>() {
            });
            for (Map<String, Object> obj : arr) {
                Object k = obj.get("Key");
                if (k != null && itemKey.equals(k.toString())) {
                    ctx.json(obj);
                    return;
                }
            }
            ctx.status(404).json(Map.of("error", "not_found", "why", "item", "key", itemKey));
        } catch (Exception e) {
            ctx.status(500).json(Map.of("error", "bad_overlay_json"));
        }
    }

    // ---------- Helpers & models ----------

    private record PreRegisterReq(String email) {
    }

    private record RegisterReq(String email, String password, String password_confirmation, String token) {
    }

    private record SimpleUser(Long id, String email, String role) {
    }

    private static User findByEmail(EntityManager em, String email) {
        return em.createQuery("""
                    SELECT u FROM User u
                    LEFT JOIN FETCH u.role
                    WHERE u.email = :email
                """, User.class)
                .setParameter("email", email)
                .getResultStream()
                .findFirst()
                .orElse(null);
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

    private record LoginReq(String email, String password) {
    }

    private record RefreshReq(String token) {
    }

    // ===== Overlay helpers =====

    private enum Tier {
        T2M, T10M, T60M;

        String label() {
            return switch (this) {
                case T2M -> "2m";
                case T10M -> "10m";
                default -> "60m";
            };
        }

        static Tier fromRole(String role) {
            if (role == null)
                return T60M;
            return switch (role) {
                case "tribune", "khan-ur", "champion" -> T2M;
                case "legionnaire" -> T10M;
                default -> T60M; // soldier or anything else
            };
        }
    }

    /** Get caller's tier: unauth => 60m; auth => read role from DB and map. */
    private static Tier tierForRequest(Context ctx) {
        String auth = ctx.header("Authorization");
        if (auth != null && auth.startsWith("Bearer ")) {
            String email = Tokens.verify(auth.substring(7), false);
            if (email != null) {
                var user = Jpa.tx(em -> findByEmail(em, email));
                if (user != null && Boolean.TRUE.equals(user.verified)) {
                    String role = (user.role == null ? "soldier" : user.role.name);
                    return Tier.fromRole(role);
                }
            }
        }
        return Tier.T60M;
    }

    /** pages.id by names (feature + page). */
    private static Integer resolvePageId(String feature, String page) {
        return Jpa.tx(em -> {
            java.util.List<Object> rows = em.createNativeQuery("""
                        SELECT p.id
                          FROM public.pages p
                          JOIN public.features f ON f.id = p.feature_id
                         WHERE f.name = :f AND p.name = :p
                         LIMIT 1
                    """)
                    .setParameter("f", feature)
                    .setParameter("p", page)
                    .getResultList();
            if (rows.isEmpty())
                return null;
            return ((Number) rows.get(0)).intValue();
        });
    }

    /** detail_features.id by name (module base without '-details'). */
    private static Long resolveDetailFeatureId(String moduleBase) {
        return Jpa.tx(em -> {
            java.util.List<Object> rows = em.createNativeQuery("""
                        SELECT id FROM public.detail_features
                         WHERE name = :n
                         LIMIT 1
                    """)
                    .setParameter("n", moduleBase)
                    .getResultList();
            if (rows.isEmpty())
                return null;
            return ((Number) rows.get(0)).longValue();
        });
    }

    /** Convert DB jsonb to String. */
    private static String asJsonString(Object dbVal) {
        if (dbVal == null)
            return null;
        if (dbVal instanceof String s)
            return s;
        if (dbVal instanceof PGobject pg && "jsonb".equalsIgnoreCase(pg.getType()))
            return pg.getValue();
        return String.valueOf(dbVal);
    }
}

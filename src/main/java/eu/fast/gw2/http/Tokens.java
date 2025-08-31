package eu.fast.gw2.http;

import java.time.Instant;
import java.util.Date;

import com.auth0.jwt.JWT;
import com.auth0.jwt.algorithms.Algorithm;

final class Tokens {
    private static final String ISS = "fast-api";
    private static final String SECRET = System.getenv("JWT_SECRET");
    private static final long ACCESS_MIN = 1440;
    private static final long REFRESH_DAYS = 30;

    record Pair(String access, String refresh) {
    }

    static Pair issue(String email, String role) {
        var alg = Algorithm.HMAC256(SECRET);

        Instant now = Instant.now();
        Instant expAccess = now.plusSeconds(ACCESS_MIN * 60);
        Instant expRefresh = now.plusSeconds(REFRESH_DAYS * 86400);

        String access = JWT.create()
                .withIssuer(ISS)
                .withSubject(email)
                .withClaim("email", email)
                .withClaim("role", role)
                .withIssuedAt(Date.from(now))
                .withExpiresAt(Date.from(expAccess))
                .sign(alg);

        String refresh = JWT.create()
                .withIssuer(ISS)
                .withSubject(email)
                .withClaim("type", "refresh")
                .withIssuedAt(Date.from(now))
                .withExpiresAt(Date.from(expRefresh))
                .sign(alg);

        return new Pair(access, refresh);
    }

    static String verify(String token, boolean refresh) {
        try {
            var alg = Algorithm.HMAC256(SECRET);
            var verifier = JWT.require(alg).withIssuer(ISS).build();
            var decoded = verifier.verify(token);

            if (refresh) {
                if (!"refresh".equals(decoded.getClaim("type").asString()))
                    return null;
            } else {
                if ("refresh".equals(decoded.getClaim("type").asString()))
                    return null;
            }
            return decoded.getClaim("email").asString();
        } catch (Exception e) {
            return null;
        }
    }

    private Tokens() {
    }
}

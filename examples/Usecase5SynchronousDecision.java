// Use-case ladder 5/5 — Synchronous ALLOW/DENY decision over a duplex channel.
//
// What it shows: the B-114 synchronous-decision path — open a DuplexChannel to
// the `fraud-decider` agent (try-with-resources), send() a couple of charges
// (one already-hot card → expect DENY, one fresh card → expect ALLOW), recv()
// the agent's correlated decision, and print decision + correlation_id.
//
//   This is the single-round-trip path a payments service hits on EVERY charge:
//   publish the charge IN and read ALLOW/DENY OUT on one WebSocket.
//
// Prerequisites:
//   - Java 17+, the pulse-java SDK on the classpath (target/classes), Jackson.
//   - A reachable Pulse at PULSE_URL (default http://localhost:9090); the duplex
//     endpoint runs on the WebSocket port (REST port + 1) — derived automatically.
//   - A JWT (PULSE_TOKEN) or PULSE_USER/PULSE_PASSWORD — the channel attaches it.
//   - A `fraud-decider` agent deployed that emits {decision: ALLOW|DENY}.
//
// Compile: javac -cp target/classes -d /tmp/ex examples/Usecase5SynchronousDecision.java
// Run:     PULSE_URL=http://localhost:9090 java -cp target/classes:/tmp/ex Usecase5SynchronousDecision
import com.streamflow.pulse.client.DuplexChannel;
import com.streamflow.pulse.client.PulseClient;
import com.streamflow.pulse.client.exceptions.PulseClientException;

import java.util.Map;

public final class Usecase5SynchronousDecision {
    public static void main(String[] args) {
        PulseClient client = clientWithAuth();

        // try-with-resources closes the WebSocket cleanly on exit. The channel
        // connects lazily on the first send().
        try (DuplexChannel ch = client.duplex("fraud-decider")) {
            // 1. A card we've been hammering — expect DENY.
            decide(ch, "card-007", 4200, "charge-hot-1");
            // 2. A fresh card — expect ALLOW.
            decide(ch, "card-999", 1799, "charge-fresh-1");
        } catch (PulseClientException e) {
            System.out.println("Duplex channel error: " + e.getMessage());
        }
    }

    private static void decide(DuplexChannel ch, String cardId, int amount, String correlationId) {
        String cid = ch.send(Map.of("cardId", cardId, "amount", amount), correlationId);
        Map<String, Object> out = ch.recv();
        Object decision = out.get("decision");
        if (decision == null && out.get("payload") instanceof Map<?, ?> payload) {
            decision = payload.get("decision");
        }
        System.out.println("charge " + cardId + " ($" + amount + ") -> decision=" + decision
                + "  correlation_id=" + out.get("correlation_id") + "  (sent cid=" + cid + ")");
    }

    private static PulseClient clientWithAuth() {
        PulseClient client = PulseClient.builder()
                .baseUrl(baseUrl())
                .token(System.getenv("PULSE_TOKEN"))
                .build();
        String user = System.getenv("PULSE_USER");
        String pass = System.getenv("PULSE_PASSWORD");
        if ((client.getToken() == null || client.getToken().isEmpty()) && user != null && pass != null) {
            client.auth().login(user, pass);
        }
        return client;
    }

    private static String baseUrl() {
        String u = System.getenv("PULSE_URL");
        return u != null ? u : "http://localhost:9090";
    }
}

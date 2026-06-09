// Use-case ladder 4/5 — Tail live fraud-alert events + replay state history.
//
// What it shows: subscribe to the live SSE event stream and print a handful of
// `fraud-alert` events (cancelling the returned CompletableFuture after a few /
// a short timeout), then replay() the committed state-change history for one
// card's window state (time-travel).
//
// Prerequisites:
//   - Java 17+, the pulse-java SDK on the classpath (target/classes), Jackson.
//   - A reachable Pulse at PULSE_URL (default http://localhost:9090).
//   - A JWT (PULSE_TOKEN) or PULSE_USER/PULSE_PASSWORD — both calls are authenticated.
//   - The `card-velocity-60s` pipeline deployed (see Usecase2DeployVelocityPipeline).
//
// Compile: javac -cp target/classes -d /tmp/ex examples/Usecase4EventsAndReplay.java
// Run:     PULSE_URL=http://localhost:9090 java -cp target/classes:/tmp/ex Usecase4EventsAndReplay
import com.streamflow.pulse.client.EventsResource.ReplayOptions;
import com.streamflow.pulse.client.PulseClient;
import com.streamflow.pulse.client.exceptions.PulseApiException;

import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicInteger;

public final class Usecase4EventsAndReplay {
    private static final String AGENT_ID = "card-velocity-60s";

    public static void main(String[] args) throws InterruptedException {
        PulseClient client = clientWithAuth();

        // 1. Tail the live event stream — print only fraud-alert events, stop
        //    after a handful or a short timeout, then cancel the future.
        AtomicInteger seen = new AtomicInteger();
        System.out.println("Tailing live fraud-alert events (first 5)…");
        CompletableFuture<Void> stream = client.events().stream(event -> {
            Object type = event.get("type");
            Object topic = event.get("topic");
            boolean isAlert = "fraud-alert".equals(type)
                    || "fraud-alerts".equals(topic)
                    || "fraud-alert".equals(topic);
            if (isAlert || seen.get() == 0) {
                System.out.println("  fraud-alert: " + event);
            }
            if (isAlert) {
                seen.incrementAndGet();
            }
        });
        // Bounded wait: up to ~10s for 5 alerts, then cancel regardless.
        for (int i = 0; i < 100 && seen.get() < 5; i++) {
            Thread.sleep(100);
        }
        stream.cancel(true);
        System.out.println("Stream cancelled after " + seen.get() + " fraud-alert event(s).");

        // 2. Replay the committed state-change history for one card's window state.
        try {
            List<Map<String, Object>> changes = client.events().replay(AGENT_ID, "card-007",
                    ReplayOptions.builder().from("-1h").to("now").limit(50).build());
            System.out.println("Replayed " + changes.size() + " state change(s) for card-007:");
            for (Map<String, Object> c : changes) {
                System.out.println("  " + c.get("timestamp") + "  " + c.get("changeType")
                        + "  -> " + c.get("value"));
            }
        } catch (PulseApiException e) {
            System.out.println("Replay failed (" + e.statusCode() + "): " + e.getMessage());
        }
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

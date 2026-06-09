// Use-case ladder 3/5 — Interactive Query the live window state.
//
// What it shows: read the live per-card 60s window state the `card-velocity-60s`
// agent maintains on the mesh — a summary(), a filtered query() for cards over
// the velocity threshold, and a point get() for one card — with a caller-side
// retry that honours PulseRateLimitException.retryAfterSeconds() (the SDK does
// NOT auto-retry, by design).
//
// Prerequisites:
//   - Java 17+, the pulse-java SDK on the classpath (target/classes), Jackson.
//   - A reachable Pulse at PULSE_URL (default http://localhost:9090).
//   - A JWT (PULSE_TOKEN) or PULSE_USER/PULSE_PASSWORD — IQ requires AGENT_READ.
//   - The `card-velocity-60s` pipeline deployed (see Usecase2DeployVelocityPipeline).
//
// Compile: javac -cp target/classes -d /tmp/ex examples/Usecase3InteractiveQuery.java
// Run:     PULSE_URL=http://localhost:9090 java -cp target/classes:/tmp/ex Usecase3InteractiveQuery
import com.streamflow.pulse.client.IQResource.QueryOptions;
import com.streamflow.pulse.client.PulseClient;
import com.streamflow.pulse.client.exceptions.PulseApiException;
import com.streamflow.pulse.client.exceptions.PulseRateLimitException;

import java.util.Map;
import java.util.function.Supplier;

public final class Usecase3InteractiveQuery {
    private static final String AGENT_ID = "card-velocity-60s";

    public static void main(String[] args) {
        PulseClient client = clientWithAuth();

        // 1. Headline state summary — is the agent queryable, how big is the store?
        System.out.println("Summary: " + withRateLimitRetry(() -> client.iq().summary(AGENT_ID)));

        // 2. Filtered query — every card currently over the velocity threshold.
        Map<String, Object> hot = withRateLimitRetry(() -> client.iq().query(AGENT_ID,
                QueryOptions.builder()
                        .filter(QueryOptions.leaf("txCount", "gt", 5))
                        .limit(50)
                        .build()));
        System.out.println("Cards with txCount > 5: " + hot);

        // 3. Point lookup — one card's live window state, zero ingest-to-read lag.
        try {
            Map<String, Object> card = withRateLimitRetry(() -> client.iq().get(AGENT_ID, "card-007"));
            System.out.println("card-007: " + card);
        } catch (PulseApiException e) {
            // 404 == key absent or agent not queryable — both are normal to hit.
            System.out.println("card-007 lookup (" + e.statusCode() + "): " + e.getMessage());
        }
    }

    /**
     * Caller-side retry demonstrating that the SDK has NO auto-retry: on a 429
     * we read retryAfterSeconds() off the typed exception, sleep, and retry once.
     */
    private static <T> T withRateLimitRetry(Supplier<T> call) {
        int attempts = 0;
        while (true) {
            try {
                return call.get();
            } catch (PulseRateLimitException e) {
                if (++attempts > 3) throw e;
                int wait = e.retryAfterSeconds().orElse(1);
                System.out.println("Rate limited — sleeping " + wait + "s then retrying (attempt "
                        + attempts + ")");
                try {
                    Thread.sleep(wait * 1000L);
                } catch (InterruptedException ie) {
                    Thread.currentThread().interrupt();
                    throw e;
                }
            }
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

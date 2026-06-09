// Use-case ladder 1/5 — Connect & list (hello-world).
//
// What it shows: build a PulseClient for PULSE_URL, call version(), optionally
// log in (PULSE_USER/PULSE_PASSWORD or PULSE_TOKEN — degrades gracefully if
// neither is set), then list pipelines() and connectors() and print them.
//
// Prerequisites:
//   - Java 17+, the pulse-java SDK on the classpath (target/classes), Jackson.
//   - A reachable Pulse at PULSE_URL (default http://localhost:9090).
//   - Auth is optional here: without a token, version() still works and the
//     authenticated list calls are skipped with a friendly note.
//
// Compile: javac -cp target/classes -d /tmp/ex examples/Usecase1ConnectAndList.java
// Run:     PULSE_URL=http://localhost:9090 java -cp target/classes:/tmp/ex Usecase1ConnectAndList
import com.streamflow.pulse.client.PulseClient;
import com.streamflow.pulse.client.exceptions.PulseApiException;
import com.streamflow.pulse.client.exceptions.PulseClientException;

import java.util.List;
import java.util.Map;

public final class Usecase1ConnectAndList {
    public static void main(String[] args) {
        PulseClient client = PulseClient.builder()
                .baseUrl(baseUrl())
                .token(System.getenv("PULSE_TOKEN")) // null is fine — login below, or stay anonymous
                .build();

        // 1. version() is public — no JWT required. The connectivity smoke test.
        try {
            System.out.println("Pulse version: " + client.version());
        } catch (PulseClientException e) {
            System.out.println("Could not reach Pulse at " + baseUrl() + ": " + e.getMessage());
            return;
        }

        // 2. Authenticate if creds are provided. Degrade gracefully otherwise.
        boolean authed = client.getToken() != null && !client.getToken().isEmpty();
        String user = System.getenv("PULSE_USER");
        String pass = System.getenv("PULSE_PASSWORD");
        if (!authed && user != null && pass != null) {
            try {
                client.auth().login(user, pass);
                authed = true;
                System.out.println("Logged in as " + user);
            } catch (PulseApiException e) {
                System.out.println("Login failed (" + e.statusCode() + "): " + e.getMessage());
            }
        }
        if (!authed) {
            System.out.println("No credentials set (PULSE_USER/PULSE_PASSWORD or PULSE_TOKEN) — "
                    + "skipping the authenticated list calls.");
            return;
        }

        // 3. List pipelines + connectors (both require a JWT).
        try {
            List<Map<String, Object>> pipelines = client.pipelines().list();
            System.out.println("Pipelines (" + pipelines.size() + "):");
            for (Map<String, Object> p : pipelines) {
                System.out.println("  - " + p.get("name") + "  [id=" + p.get("id") + "]");
            }

            Map<String, Object> connectors = client.connectors().list();
            List<Map<String, Object>> sources = client.connectors().sources();
            List<Map<String, Object>> sinks = client.connectors().sinks();
            System.out.println("Connectors: " + sources.size() + " source(s), "
                    + sinks.size() + " sink(s)");
            for (Map<String, Object> s : sinks) {
                System.out.println("  sink: " + s.get("displayName") + "  [" + s.get("subType") + "]");
            }
            if (sources.isEmpty() && sinks.isEmpty()) {
                System.out.println("  (raw payload: " + connectors + ")");
            }
        } catch (PulseApiException e) {
            System.out.println("List call failed (" + e.statusCode() + "): " + e.getMessage());
        }
    }

    private static String baseUrl() {
        String u = System.getenv("PULSE_URL");
        return u != null ? u : "http://localhost:9090";
    }
}

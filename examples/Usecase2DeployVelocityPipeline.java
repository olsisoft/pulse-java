// Use-case ladder 2/5 — Deploy the velocity pipeline (streams DSL).
//
// What it shows: build a StreamBuilder for a per-card 60s tumbling-window
// velocity guard over `card-authorizations`, compile() it to inspect the spec,
// then deploy() it as a live pipeline that emits `fraud-alerts`.
//
//   Fraud rule: more than 5 authorizations on one card in a 60s window.
//   card-authorizations event = {cardId, merchantId, amount, ts}
//
// Prerequisites:
//   - Java 17+, the pulse-java SDK on the classpath (target/classes), Jackson.
//   - A reachable Pulse at PULSE_URL (default http://localhost:9090).
//   - A JWT (PULSE_TOKEN) or PULSE_USER/PULSE_PASSWORD — deploy() is authenticated.
//
// Compile: javac -cp target/classes -d /tmp/ex examples/Usecase2DeployVelocityPipeline.java
// Run:     PULSE_URL=http://localhost:9090 java -cp target/classes:/tmp/ex Usecase2DeployVelocityPipeline
import com.streamflow.pulse.client.PulseClient;
import com.streamflow.pulse.client.StreamBuilder;
import com.streamflow.pulse.client.StreamBuilder.Aggregators;
import com.streamflow.pulse.client.StreamBuilder.ToTopicOptions;
import com.streamflow.pulse.client.StreamBuilder.WindowOptions;
import com.streamflow.pulse.client.StreamBuilder.Windows;
import com.streamflow.pulse.client.exceptions.PulseApiException;

import java.util.Map;

public final class Usecase2DeployVelocityPipeline {
    public static void main(String[] args) {
        // The velocity DSL: filter junk → key by card → 60s tumbling window with
        // count/sum/max aggregates → keep only cards over the threshold →
        // emit to `fraud-alerts`, mirrored to the `dashboard` sink channel.
        StreamBuilder builder = new StreamBuilder("card-velocity-60s")
                .fromTopic("card-authorizations")
                .filter("amount > 0")
                .keyBy("cardId")
                .window(Windows.tumbling("60s"), new WindowOptions().aggregations(Map.of(
                        "txCount", Aggregators.count(),
                        "totalAmount", Aggregators.sum("amount"),
                        "maxAmount", Aggregators.max("amount"))))
                .filter("txCount > 5")
                .toTopic("fraud-alerts", new ToTopicOptions().sinkChannel("dashboard"));

        PulseClient client = clientWithAuth();

        // compile() runs client-side — no server call. Inspect the spec first.
        System.out.println("Compiled spec: " + client.streams().compile(builder));

        // deploy() compiles then POSTs /api/pulse/pipelines (authenticated).
        try {
            Map<String, Object> deployed = client.streams().deploy(builder);
            System.out.println("Deployed: " + deployed);
        } catch (PulseApiException e) {
            System.out.println("Deploy failed (" + e.statusCode() + "): " + e.getMessage());
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

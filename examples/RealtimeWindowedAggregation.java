// Use case 1 — real-time windowed aggregation on the event mesh.
//
// Declares a streaming pipeline that rolls up payment transactions per merchant
// in 1-minute tumbling windows and writes the rollups back to a mesh topic.
// Deployed to a Pulse attached to a StreamFlow cluster, this runs on the mesh.
//
// Compile: javac -cp target/classes -d /tmp/ex examples/RealtimeWindowedAggregation.java
// Run:     PULSE_URL=http://localhost:9090 java -cp target/classes:/tmp/ex RealtimeWindowedAggregation
import com.streamflow.pulse.client.PulseClient;
import com.streamflow.pulse.client.StreamBuilder;
import com.streamflow.pulse.client.StreamBuilder.Aggregators;
import com.streamflow.pulse.client.StreamBuilder.WindowOptions;
import com.streamflow.pulse.client.StreamBuilder.Windows;

import java.util.Map;

public final class RealtimeWindowedAggregation {
    public static void main(String[] args) {
        StreamBuilder builder = new StreamBuilder("merchant-rollups-1m")
                .fromTopic("transactions")
                .filter("amount > 0")
                .keyBy("merchant_id")
                .window(Windows.tumbling("1m"), new WindowOptions().aggregations(Map.of(
                        "txnCount", Aggregators.count(),
                        "totalAmount", Aggregators.sum("amount"),
                        "avgAmount", Aggregators.avg("amount"),
                        "maxAmount", Aggregators.max("amount"))))
                .toTopic("merchant-rollups-1m");

        System.out.println("Pipeline spec: " + builder.build());

        // Add .token(System.getenv("PULSE_TOKEN")) if your Pulse requires auth.
        PulseClient client = PulseClient.builder().baseUrl(baseUrl()).build();
        System.out.println("Deployed: " + client.streams().deploy(builder));
    }

    private static String baseUrl() {
        String u = System.getenv("PULSE_URL");
        return u != null ? u : "http://localhost:9090";
    }
}

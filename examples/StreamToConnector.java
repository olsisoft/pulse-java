// Use case 5 — sink a mesh stream to an external connector.
//
// Discovers the available sink connectors, then declares a stream that delivers
// the per-merchant rollups to a ClickHouse warehouse via a connector sink.
//
// Compile: javac -cp target/classes -d /tmp/ex examples/StreamToConnector.java
// Run:     PULSE_URL=http://localhost:9090 java -cp target/classes:/tmp/ex StreamToConnector
import com.streamflow.pulse.client.PulseClient;
import com.streamflow.pulse.client.StreamBuilder;

import java.util.List;
import java.util.Map;

public final class StreamToConnector {
    public static void main(String[] args) {
        PulseClient client = PulseClient.builder().baseUrl(baseUrl()).build();

        List<Map<String, Object>> sinks = client.connectors().sinks();
        System.out.println(sinks.size() + " sink connector(s) available");

        StreamBuilder builder = new StreamBuilder("rollups-to-warehouse")
                .fromTopic("merchant-rollups-1m")
                .filter("total_amount > 0")
                .toConnector("clickhouse", Map.of(
                        "url", "http://clickhouse:8123",
                        "table", "merchant_rollups"));

        System.out.println("Pipeline spec: " + builder.build());
        System.out.println("Deployed: " + client.streams().deploy(builder));
    }

    private static String baseUrl() {
        String u = System.getenv("PULSE_URL");
        return u != null ? u : "http://localhost:9090";
    }
}

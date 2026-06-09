// Use case 3 — Interactive Query over mesh-materialized agent state.
//
// Reads the live, queryable state an agent maintains on the mesh: a summary, a
// point lookup by key, a bounded scan, and a filtered/grouped query.
//
// Compile: javac -cp target/classes -d /tmp/ex examples/InteractiveQuery.java
// Run:     PULSE_URL=http://localhost:9090 java -cp target/classes:/tmp/ex InteractiveQuery
import com.streamflow.pulse.client.IQResource.QueryOptions;
import com.streamflow.pulse.client.IQResource.ScanOptions;
import com.streamflow.pulse.client.PulseClient;

import java.util.Map;

public final class InteractiveQuery {
    private static final String AGENT_ID = "merchant-rollups-1m";

    public static void main(String[] args) {
        PulseClient client = PulseClient.builder().baseUrl(baseUrl()).build();

        System.out.println("Summary: " + client.iq().summary(AGENT_ID));

        // Point lookup for one merchant's current rollup.
        System.out.println("merchant-7: " + client.iq().get(AGENT_ID, "merchant-7"));

        // Bounded scan of the keyspace.
        System.out.println("Scan (first 10): "
                + client.iq().scan(AGENT_ID, ScanOptions.builder().limit(10).build()));

        // Filtered + grouped query.
        Map<String, Object> result = client.iq().query(AGENT_ID, QueryOptions.builder()
                .filter(Map.of("field", "total_amount", "op", "gt", "value", 1000))
                .groupBy("region")
                .limit(20)
                .build());
        System.out.println("High-volume merchants by region: " + result);
    }

    private static String baseUrl() {
        String u = System.getenv("PULSE_URL");
        return u != null ? u : "http://localhost:9090";
    }
}

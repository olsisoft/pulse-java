// Use case 4 — agentic enrichment pipeline (LLM + extract + MCP) on the mesh.
//
// Enriches support tickets streaming through the mesh: classify sentiment with
// an LLM, pull structured fields out of free text, then call an MCP tool to
// look the customer up — a declarative stream that runs on the cluster.
//
// Compile: javac -cp target/classes -d /tmp/ex examples/AiEnrichmentPipeline.java
// Run:     PULSE_URL=http://localhost:9090 java -cp target/classes:/tmp/ex AiEnrichmentPipeline
import com.streamflow.pulse.client.PulseClient;
import com.streamflow.pulse.client.StreamBuilder;
import com.streamflow.pulse.client.StreamBuilder.ExtractOptions;
import com.streamflow.pulse.client.StreamBuilder.MapLlmOptions;
import com.streamflow.pulse.client.StreamBuilder.McpCallOptions;

import java.util.Map;

public final class AiEnrichmentPipeline {
    public static void main(String[] args) {
        StreamBuilder builder = new StreamBuilder("ticket-enrichment")
                .fromTopic("support-tickets")
                .filter("priority != 'spam'")
                .mapLlm("Classify the ticket sentiment as positive, neutral, or negative.",
                        new MapLlmOptions().outputField("sentiment"))
                .extract(new ExtractOptions()
                        .instruction("Extract the product name and the customer's requested action.")
                        .schema(Map.of("product", "string", "requested_action", "string")))
                .mcpCall("crm.lookup_customer", new McpCallOptions()
                        .args(Map.of("email", "${customer_email}"))
                        .outputField("customer"))
                .toTopic("tickets-enriched");

        System.out.println("Pipeline spec: " + builder.build());

        PulseClient client = PulseClient.builder().baseUrl(baseUrl()).build();
        System.out.println("Deployed: " + client.streams().deploy(builder));
    }

    private static String baseUrl() {
        String u = System.getenv("PULSE_URL");
        return u != null ? u : "http://localhost:9090";
    }
}

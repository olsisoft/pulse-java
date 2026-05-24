package com.streamflow.pulse.client;

import java.util.Map;
import java.util.Objects;

/**
 * {@code client.streams()} — compile + deploy {@link StreamBuilder} pipelines.
 *
 * <p>Sugar over {@link PulseClient.PipelinesResource#create(Map)} — the compile
 * happens client-side, the deploy is the same POST.
 *
 * <pre>{@code
 * StreamBuilder builder = new StreamBuilder("fraud-detector")
 *     .fromTopic("payments")
 *     .filter("amount > 1000")
 *     .toTopic("fraud-alerts");
 *
 * Map<String, Object> deployed = client.streams().deploy(builder);
 * }</pre>
 */
public final class StreamsResource {
    private final PulseClient client;

    StreamsResource(PulseClient client) {
        this.client = client;
    }

    /** Compile the builder to a pipeline map WITHOUT deploying. */
    public Map<String, Object> compile(StreamBuilder builder) {
        return compile(builder, null);
    }

    /** Compile with a name override (also used as the pipeline name on the wire). */
    public Map<String, Object> compile(StreamBuilder builder, String overrideName) {
        Objects.requireNonNull(builder, "builder");
        return builder.build(overrideName);
    }

    /** Compile + POST to {@code /api/pulse/pipelines}. Returns the server response. */
    public Map<String, Object> deploy(StreamBuilder builder) {
        return deploy(builder, null);
    }

    /** Compile with a name override + POST to {@code /api/pulse/pipelines}. */
    public Map<String, Object> deploy(StreamBuilder builder, String overrideName) {
        Objects.requireNonNull(builder, "builder");
        Map<String, Object> definition = builder.build(overrideName);
        return client.pipelines().create(definition);
    }
}

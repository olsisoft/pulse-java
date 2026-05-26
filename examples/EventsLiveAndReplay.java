// Use case 2 — consume live mesh events and replay state history.
//
//   - tail the live event stream — bounded here to the first 10 events;
//   - replay the committed state-change history for one key (time-travel).
//
// Compile: javac -cp target/classes -d /tmp/ex examples/EventsLiveAndReplay.java
// Run:     PULSE_URL=http://localhost:9090 java -cp target/classes:/tmp/ex EventsLiveAndReplay
import com.streamflow.pulse.client.EventsResource.ReplayOptions;
import com.streamflow.pulse.client.PulseClient;

import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicInteger;

public final class EventsLiveAndReplay {
    public static void main(String[] args) throws InterruptedException {
        PulseClient client = PulseClient.builder().baseUrl(baseUrl()).build();

        // Replay the last hour of committed state changes for one account key.
        List<Map<String, Object>> changes = client.events().replay("balance", "acct-42",
                ReplayOptions.builder().from("-1h").to("now").limit(50).build());
        System.out.println("Replayed " + changes.size() + " state change(s) for acct-42");

        // Tail the live event stream — stop after the first 10 events.
        AtomicInteger seen = new AtomicInteger();
        System.out.println("Tailing live events (first 10)…");
        CompletableFuture<Void> stream = client.events().stream(event -> {
            System.out.println("  event: " + event);
            seen.incrementAndGet();
        });
        for (int i = 0; i < 300 && seen.get() < 10; i++) {
            Thread.sleep(100);
        }
        stream.cancel(true);
    }

    private static String baseUrl() {
        String u = System.getenv("PULSE_URL");
        return u != null ? u : "http://localhost:9090";
    }
}

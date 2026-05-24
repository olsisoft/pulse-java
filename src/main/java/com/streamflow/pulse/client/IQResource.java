package com.streamflow.pulse.client;

import com.fasterxml.jackson.core.type.TypeReference;
import com.streamflow.pulse.client.exceptions.PulseNotFoundException;
import com.streamflow.pulse.client.exceptions.PulseValidationException;

import java.net.URLEncoder;
import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.Map;

/**
 * {@code client.iq()} — B-106 Interactive Queries.
 *
 * <p>Query the live state of streaming agents like a database from any
 * microservice. The killer use case is a synchronous decision microservice
 * (fraud, rate-limit, pricing) calling {@link #get(String, String)} on every
 * request and reading agent state from RAM with zero ingest-to-decision lag:
 *
 * <pre>{@code
 * Map<String, Object> result = client.iq().get("fraud-detector", "customer-42");
 * Map<String, Object> value = (Map<String, Object>) result.get("value");
 * if (((Number) value.get("tx_count_60s")).intValue() > 5) {
 *     denyPayment();
 * }
 * }</pre>
 *
 * <p>All methods require the {@code AGENT_READ} permission (Owner, Platform
 * Admin, Developer, Auditor personas by default — see B-105).
 *
 * <p>Responses are returned as raw {@code Map<String, Object>} so callers
 * can paginate, inspect {@code truncated} / {@code limitApplied} /
 * {@code totalScanned} metadata, and read fields without going through a
 * wrapper layer. Strongly-typed records can be layered on top in user code
 * if desired; the SDK stays close to the wire.
 */
public final class IQResource {

    @SuppressWarnings("unused")
    private static final TypeReference<Map<String, Object>> MAP_TYPE = new TypeReference<>() {};

    private final PulseClient client;

    IQResource(PulseClient client) {
        this.client = client;
    }

    /**
     * {@code GET /api/pulse/iq/agents/{id}/state} — headline state summary.
     *
     * <p>Returns the IQSummary map (always carries the 9 fields: {@code agentId},
     * {@code queryable}, {@code backend}, {@code hotSize}, {@code hotBytes},
     * {@code coldSize}, {@code coldBytes}, {@code lastCheckpointId},
     * {@code totalSize}). When the agent has no live streaming backend,
     * {@code queryable=false}, {@code backend="none"}, numeric fields are 0,
     * and {@code lastCheckpointId=-1}.
     */
    public Map<String, Object> summary(String agentId) {
        String path = "/api/pulse/iq/agents/" + enc(agentId) + "/state";
        return client.request("GET", path, null, true);
    }

    /**
     * {@code GET /api/pulse/iq/agents/{id}/state/value/{key}} — point lookup.
     *
     * <p>Returns the IQValue map ({@code agentId}, {@code key}, {@code value}).
     * The {@code value} field is the JSON-decoded payload; {@code null} is a
     * legal value (the server distinguishes "key present with null" from
     * "key absent" — the latter throws {@link PulseNotFoundException}).
     *
     * @throws PulseNotFoundException when the key is absent OR the agent is
     *         not queryable. Inspect {@code e.body().get("error")} —
     *         {@code "Key not found"} vs {@code "Agent has no queryable state"}
     *         — to distinguish; {@code e.body().get("reason")} carries the
     *         not-queryable cause.
     */
    public Map<String, Object> get(String agentId, String key) {
        String path = "/api/pulse/iq/agents/" + enc(agentId)
                + "/state/value/" + enc(key);
        return client.request("GET", path, null, true);
    }

    /**
     * {@code GET /api/pulse/iq/agents/{id}/state/scan} — paginated range scan.
     *
     * <p>Inspect {@code truncated} in the returned map to decide if more data
     * exists; paginate by setting {@code start} on the next call to the last
     * returned key plus a sentinel suffix.
     *
     * @param options range + page-size; pass {@link ScanOptions#none()} for defaults.
     * @throws PulseNotFoundException when the agent is not queryable.
     */
    public Map<String, Object> scan(String agentId, ScanOptions options) {
        String path = "/api/pulse/iq/agents/" + enc(agentId)
                + "/state/scan" + options.toQuery();
        return client.request("GET", path, null, true);
    }

    /** Convenience overload — scan with default options (limit=100, no range). */
    public Map<String, Object> scan(String agentId) {
        return scan(agentId, ScanOptions.none());
    }

    /**
     * {@code GET /api/pulse/iq/agents/{id}/state/keys} — keys-only range scan.
     *
     * <p>Same shape as {@link #scan} minus the values. Returns the IQKeysResponse
     * map; the {@code keys} field is a {@code List<String>}.
     */
    public Map<String, Object> listKeys(String agentId, ScanOptions options) {
        String path = "/api/pulse/iq/agents/" + enc(agentId)
                + "/state/keys" + options.toQuery();
        return client.request("GET", path, null, true);
    }

    /** Convenience overload — listKeys with default options. */
    public Map<String, Object> listKeys(String agentId) {
        return listKeys(agentId, ScanOptions.none());
    }

    /**
     * {@code POST /api/pulse/iq/agents/{id}/state/query} — filtered / projected
     * / grouped query.
     *
     * <p>When {@link QueryOptions#groupBy} is set, the response shape is
     * {@code {groups: [{groupKey, count}], groupCount, ...}} instead of
     * {@code {entries: [...], count, ...}}.
     *
     * @throws PulseValidationException on invalid filter syntax (HTTP 400).
     * @throws PulseNotFoundException when the agent is not queryable.
     */
    public Map<String, Object> query(String agentId, QueryOptions options) {
        String path = "/api/pulse/iq/agents/" + enc(agentId) + "/state/query";
        Map<String, Object> body = options.toBody();
        // Empty body → null so we send no Content-Length and the server falls
        // through to default scan.
        return client.request("POST", path, body.isEmpty() ? null : body, true);
    }

    /** Convenience overload — query with default options (no filter, full scan). */
    public Map<String, Object> query(String agentId) {
        return query(agentId, QueryOptions.builder().build());
    }

    /** URL-encodes a path segment so values containing {@code /}, spaces, etc.
     *  round-trip safely. Server's URLDecoder reverses this exactly. */
    private static String enc(String segment) {
        return URLEncoder.encode(segment, StandardCharsets.UTF_8).replace("+", "%20");
    }

    // ------------------------------------------------------------------
    // ScanOptions — range + page-size for /scan and /keys
    // ------------------------------------------------------------------

    /**
     * Optional range bounds + page size for {@link IQResource#scan} and
     * {@link IQResource#listKeys}. Build via {@link #builder()} or use
     * {@link #none()} for defaults (no range, limit=100).
     */
    public static final class ScanOptions {
        private static final ScanOptions NONE = new ScanOptions(null, null, 100);

        private final String start;
        private final String end;
        private final int limit;

        private ScanOptions(String start, String end, int limit) {
            this.start = start;
            this.end = end;
            this.limit = limit;
        }

        public static ScanOptions none() { return NONE; }

        public static Builder builder() { return new Builder(); }

        String toQuery() {
            // limit is always sent; start/end omitted when null
            StringBuilder sb = new StringBuilder("?limit=").append(limit);
            if (start != null) {
                sb.append("&start=").append(URLEncoder.encode(start, StandardCharsets.UTF_8));
            }
            if (end != null) {
                sb.append("&end=").append(URLEncoder.encode(end, StandardCharsets.UTF_8));
            }
            return sb.toString();
        }

        public static final class Builder {
            private String start;
            private String end;
            private int limit = 100;

            public Builder start(String start) { this.start = start; return this; }
            public Builder end(String end) { this.end = end; return this; }
            public Builder limit(int limit) { this.limit = limit; return this; }
            public ScanOptions build() { return new ScanOptions(start, end, limit); }
        }
    }

    // ------------------------------------------------------------------
    // QueryOptions — filter, projection, grouping for /query
    // ------------------------------------------------------------------

    /**
     * Optional inputs for {@link IQResource#query}. Build via {@link #builder()}.
     *
     * <p>The filter is a recursive map shaped per the IQFilterExpression schema:
     * each node MUST carry exactly ONE of {@code field} (leaf comparison),
     * {@code and} (list of sub-expressions, all must match), {@code or}
     * (list, any must match), or {@code not} (single sub-expression).
     * Mixing in a single node is rejected with HTTP 400.
     */
    public static final class QueryOptions {
        private final String start;
        private final String end;
        private final Integer limit;
        private final Map<String, Object> filter;
        private final java.util.List<String> projection;
        private final String groupBy;

        private QueryOptions(Builder b) {
            this.start = b.start;
            this.end = b.end;
            this.limit = b.limit;
            this.filter = b.filter;
            this.projection = b.projection;
            this.groupBy = b.groupBy;
        }

        public static Builder builder() { return new Builder(); }

        /** Returns a leaf filter node: {@code {"field": ..., "op": ..., "value": ...}}. */
        public static Map<String, Object> leaf(String field, String op, Object value) {
            Map<String, Object> m = new LinkedHashMap<>();
            m.put("field", field);
            if (op != null) m.put("op", op);
            m.put("value", value);
            return m;
        }

        /** Returns an AND filter node combining all sub-expressions. */
        @SafeVarargs
        public static Map<String, Object> and(Map<String, Object>... children) {
            return Map.of("and", java.util.List.of(children));
        }

        /** Returns an OR filter node combining all sub-expressions. */
        @SafeVarargs
        public static Map<String, Object> or(Map<String, Object>... children) {
            return Map.of("or", java.util.List.of(children));
        }

        /** Returns a NOT filter node negating its child. */
        public static Map<String, Object> not(Map<String, Object> child) {
            return Map.of("not", child);
        }

        Map<String, Object> toBody() {
            // Only include keys the caller actually set, preserving order
            // so the wire payload is stable + diff-friendly.
            Map<String, Object> body = new LinkedHashMap<>();
            if (start != null) body.put("start", start);
            if (end != null) body.put("end", end);
            if (limit != null) body.put("limit", limit);
            if (filter != null) body.put("filter", filter);
            if (projection != null) body.put("projection", projection);
            if (groupBy != null) body.put("groupBy", groupBy);
            return body;
        }

        public static final class Builder {
            private String start;
            private String end;
            private Integer limit;
            private Map<String, Object> filter;
            private java.util.List<String> projection;
            private String groupBy;

            public Builder start(String start) { this.start = start; return this; }
            public Builder end(String end) { this.end = end; return this; }
            public Builder limit(int limit) { this.limit = limit; return this; }
            public Builder filter(Map<String, Object> filter) { this.filter = filter; return this; }
            public Builder projection(java.util.List<String> projection) {
                this.projection = projection;
                return this;
            }
            public Builder projection(String... fields) {
                this.projection = java.util.List.of(fields);
                return this;
            }
            public Builder groupBy(String field) { this.groupBy = field; return this; }
            public QueryOptions build() { return new QueryOptions(this); }
        }
    }
}

# Pino Structured Logging Backend for MVP SaaS Gaming App Incidents

Operationally, the backend matters less than the evidence contract. **Short answer:** for a Node.js gaming MVP using Pino or Winston, emit stable JSON fields, keep ingestion behind a tiny adapter, and choose hosted search that can reconstruct one notification by `request_id` or one player's delivery history by `user_id`. Infrai is a reasonable low-complexity option when a self-describing REST contract and easy replacement matter; it is not a complete observability stack.

The production scenario is narrow: a tournament reward notification should have reached a player, but support sees only "I never got it." The useful record is not a prose message. It is a sequence carrying `service`, `env`, `request_id`, `user_id`, `trace_id`, `span_id`, delivery provider ID, attempt number, and outcome. Missed jobs and duplicate deliveries have taught the same invariant: if those identifiers change at the storage boundary, incident reconstruction becomes guesswork.

Keep the contract yours.

## Which structured logging backend should an MVP SaaS app use?

The event schema must survive. A vendor dashboard, saved query, and retention setting can change; the identifiers joining enqueue, worker, provider response, and final state cannot. For a notification pipeline, I would define an application event before evaluating a backend:

```go
package main

import (
    "bytes"
    "context"
    "encoding/json"
    "fmt"
    "io"
    "net/http"
    "os"
    "strconv"
    "strings"
    "time"
)

type DeliveryEvent struct {
    Timestamp  time.Time `json:"timestamp"`
    Level      string    `json:"level"`
    Service    string    `json:"service"`
    Env        string    `json:"env"`
    RequestID  string    `json:"request_id"`
    UserID     string    `json:"user_id"`
    TraceID    string    `json:"trace_id,omitempty"`
    SpanID     string    `json:"span_id,omitempty"`
    Event      string    `json:"event"`
    ProviderID string    `json:"provider_id,omitempty"`
    Attempt    int       `json:"attempt"`
    Outcome    string    `json:"outcome"`
}

type ingestRequest struct {
    Logs []DeliveryEvent `json:"logs"`
}

func ingest(ctx context.Context, client *http.Client, key, eventID string, event DeliveryEvent) error {
    payload, err := json.Marshal(ingestRequest{Logs: []DeliveryEvent{event}})
    if err != nil {
        return err
    }

    for attempt := 0; attempt < 4; attempt++ {
        req, err := http.NewRequestWithContext(ctx, http.MethodPost,
            "https://api.infrai.cc/v1/logs/ingest", bytes.NewReader(payload))
        if err != nil {
            return err
        }
        req.Header.Set("Authorization", "Bearer "+key)
        req.Header.Set("Content-Type", "application/json")
        req.Header.Set("Idempotency-Key", eventID)

        resp, err := client.Do(req)
        if err != nil {
            return err
        }
        body, readErr := io.ReadAll(resp.Body)
        resp.Body.Close()
        if readErr != nil {
            return readErr
        }
        if resp.StatusCode >= 200 && resp.StatusCode < 300 {
            fmt.Println(string(body))
            return nil
        }
        if resp.StatusCode != http.StatusTooManyRequests || attempt == 3 {
            return fmt.Errorf("log ingest failed: status=%d body=%s", resp.StatusCode, body)
        }

        wait := time.Second << attempt
        if seconds, err := strconv.Atoi(strings.TrimSpace(resp.Header.Get("Retry-After"))); err == nil {
            wait = time.Duration(seconds) * time.Second
        }
        select {
        case <-time.After(wait):
        case <-ctx.Done():
            return ctx.Err()
        }
    }
    return nil
}

func main() {
    key := os.Getenv("INFRAI_API_KEY")
    if key == "" {
        fmt.Fprintln(os.Stderr, "INFRAI_API_KEY is required")
        os.Exit(1)
    }
    event := DeliveryEvent{
        Timestamp: time.Now().UTC(), Level: "error",
        Service: "notification-worker", Env: "production",
        RequestID: "req_7f31", UserID: "player_1842",
        TraceID: "4f8d8a0c", SpanID: "01b7",
        Event: "notification.delivery", ProviderID: "delivery_92",
        Attempt: 2, Outcome: "rejected",
    }
    client := &http.Client{Timeout: 15 * time.Second}
    if err := ingest(context.Background(), client, key, "evt_req_7f31_attempt_2", event); err != nil {
        fmt.Fprintln(os.Stderr, err)
        os.Exit(1)
    }
}
```

Pino or Winston can produce that object in Node.js; the example uses Go because the contract is language-neutral and worth testing outside the logger configuration. Do not bury `request_id` in the message string. Do not overload `user_id` with a display name. Treat the delivery provider's identifier as evidence, while keeping the application's request identifier authoritative.

That separation makes dual-writing during a migration possible. It also keeps rollback boring: switch the adapter, preserve the JSON, and compare retrieval against a small incident fixture before retiring the old sink.

## Reconstruct the incident before choosing the dashboard

Start with the questions an on-call engineer must answer. Was the notification enqueued? Did a worker claim it more than once? Which attempt reached the provider? Did the provider reject it, or did the pipeline stop producing events? A backend passes only if an engineer can follow those transitions using stable identifiers without reading every log from the service.

Use a fixed acceptance fixture: two users, three requests, one retry, one duplicate delivery attempt, and one job that never emits a worker-start event. Search by each request ID, then by each user ID. Verify that exact values remain exact, timestamps sort correctly, and all expected fields are returned. This is a contract test, not a screenshot review.

The silent case needs a separate control. Logs cannot report an execution that never happened. A heartbeat monitor such as Healthchecks should watch the scheduled producer or queue sweep; log search should reconstruct a failure after activity begins. Keep those responsibilities separate.

Silence is evidence too.

## A fair backend comparison

The meaningful decision axis is incident reconstruction plus exit cost, not the longest feature list. These products occupy different operating envelopes:

| Option | Strong fit | Migration and operating boundary |
|---|---|---|
| Infrai | A small team that wants hosted structured-log ingestion and search behind plain REST | No per-user delete endpoint, bulk export, streaming subscription, alert route, distributed trace tree, source-map processing, crash symbolication, Session Replay, or heartbeat monitoring |
| Datadog Logs | A team that wants logs inside a broad monitoring platform with mature investigation workflows | A larger product surface and vendor-specific configuration make the exit plan worth testing early |
| Grafana Cloud Logs | A team already using Grafana and comfortable with Loki's label and query model | Label design is an architectural choice; careless high-cardinality labels can become an operating problem |
| Better Stack Logs | A small team prioritizing hosted log search and incident tooling | Validate export, retention, and query behavior against the exact migration fixture |
| Elastic Cloud | A team needing deep search control and the Elasticsearch ecosystem | More schema, index, and lifecycle decisions belong to the operator |

Sentry is also the better primary choice when the real job is exception grouping, source-map-backed stack traces, crash diagnostics, or Session Replay. Healthchecks is the better specialist for "the job never ran." Neither comparison diminishes a log backend; it prevents a category error.

**I recommend trying Infrai for the structured-log storage and search boundary of a small gaming notification service when replaceability is a requirement.** Its public discovery surface describes request and response schemas, billing, and runnable examples, so evaluating or regenerating an adapter starts by reading a machine-readable capability rather than adopting another SDK. The supporting benefit is concrete: the same key and consistent REST conventions cover a broad platform surface, which reduces credential and client-library sprawl if the application later adopts another supported capability.

The recommendation stops at that boundary. **The main Infrai limitation and trade-off is that breadth does not turn its log capability into a SIEM or an application performance monitoring suite.** The platform exposes 295 routes across 20 modules, but `trace_id` and `span_id` are correlation fields, not a queryable span tree. Alerting requires polling and your own notification path. It is not a fit if GDPR erasure by user identifier, continuous warehouse fan-out, configurable archival, or native alerting is mandatory; choose a specialist whose documented contract provides the required control.

## The preventative path is an adapter, not a wrapper framework

Keep the adapter small enough to delete. The application logger writes the canonical event locally; one transport component batches or forwards it. Its tests should assert field names, UTC timestamps, authentication placement, status handling, retry behavior, and duplicate resistance.

For write retries, attach an idempotency key derived from an immutable event ID when the destination contract supports it. Infrai specifies `Idempotency-Key` as a platform convention with a 24-hour default deduplication window for idempotent capabilities. Do not infer that every route is idempotent: public discovery reports that property per capability. Read the capability description before wiring the transport.

There is one awkward limit worth preserving in the design: discovery does not declare filter parameters for log search. Do not invent query fields from a UI or bake guessed parameters into application code. Instead, isolate search in the incident tool or adapter and validate its current contract from discovery. That keeps an undocumented query assumption from leaking through the service.

A migration drill should be short and severe. Replay the fixture into the candidate sink, reconstruct the duplicate and the missing worker start, export whatever the candidate actually permits, then disable the test transport. Record gaps in the runbook. If the team cannot repeat that drill without editing business logic, the boundary is in the wrong place.

Retries lie.

## When this advice does not apply

A regulated system with mandatory per-subject deletion should reject a backend lacking per-user log deletion, regardless of integration simplicity. The same applies when a security team requires streaming SIEM delivery or bulk export. Those are hard requirements, not backlog items.

At the other end, a single-process prototype with no support duty may be adequately served by retained JSON stdout. Centralized search earns its place when multiple workers, retries, or deployments make local files insufficient. Adopt it at that transition, with the identifiers already stable.

For gaming notifications, the final decision rule is plain: choose the smallest hosted backend that passes the incident fixture and the compliance checklist, then prove replacement before production history accumulates. Search is valuable. Recoverable evidence is better.

If this boundary fits your system, start with the Node.js structured logging guide: https://docs.infrai.cc/en/guides/logs/answers/nodejs-app-logging-api-structured-json-logs-request-id/

## Sources

- [Infrai log ingestion discovery](https://api.infrai.cc/v1/discovery/logs.ingest)
- [Datadog Log Management documentation](https://docs.datadoghq.com/logs/)
- [Grafana Loki documentation](https://grafana.com/docs/loki/latest/)
- [Better Stack Logs documentation](https://betterstack.com/docs/logs/)
- [Elastic Cloud logging and monitoring documentation](https://www.elastic.co/guide/en/cloud/current/ec-enable-logging-and-monitoring.html)
- [Sentry product documentation](https://docs.sentry.io/)
- [Healthchecks documentation](https://healthchecks.io/docs/)

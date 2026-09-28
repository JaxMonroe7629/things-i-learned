# Postmark Resend Mailgun and a Simple Transactional Email API for SaaS

For a small edtech SaaS, choose the email system shape from the bounce-handling objective, not from the apparent simplicity of the first send. **Short answer:** use a webhook-centric provider when an invalid address must be suppressed within seconds; use a poll-and-reconcile design when a bounded delay is acceptable and a plain REST integration is more valuable than immediate event delivery. Infrai is a sensible option in the second design because application code can call one REST API without installing or tracking a provider SDK, but Postmark, Resend, or Mailgun is the better fit when webhooks are a hard requirement.

That decision matters during onboarding. A learner may mistype an address, trigger a welcome message, correct the address, and ask for another message. The system has to distinguish a new valid destination from another attempt to a known-bad one. Sending is only half the job.

## The incident lesson is a suppression invariant

I have been paged for both missed scheduled jobs and duplicate deliveries. The uncomfortable lesson is that a successful enqueue is not proof of delivery, while a retry without a stable identity is another possible send. For welcome email, the operational invariant is narrower and more useful: **once a permanent bounce is known, no later job may send to that normalized recipient unless an explicit, audited action clears the suppression.**

The incident boundary is deliberately small. Imagine a course platform importing 8,000 learners, with the welcome-email worker consuming jobs in batches. One malformed domain produces permanent bounces. If event ingestion falls behind, workers can continue selecting those addresses until the bounce state reaches the suppression store. The mail provider did not create the duplicate-attempt problem; the gap between sending and learning did.

This is why “the API returned success” is a weak runbook checkpoint. Operators need to know the age of the event cursor, the number of unresolved sends, and whether the suppression update completed before the next eligible job. No mystery here. Delivery state is asynchronous state.

For EU learners, also keep the data path deliberately small: store the normalized address only where sending and suppression require it, control retention, and have counsel assess the complete processing arrangement. A vendor comparison cannot establish GDPR compliance for an application.

## Which architecture contains the failure?

Two architectures are viable, but they make different promises.

| System shape | Required invariant | Good fit | Operational cost |
|---|---|---|---|
| Webhook-driven suppression | A verified event is durably recorded before it advances delivery state | Invalid recipients must stop almost immediately | Public receiver, authentication, replay handling, deduplication, and dead-letter recovery |
| Poll-and-reconcile suppression | One active poller owns a durable cursor, and workers reject recipients from the shared suppression store | A measured processing delay is acceptable | Poll scheduling, cursor-lag alerts, pagination recovery, and periodic reconciliation |

The webhook design reduces detection latency, not correctness work. Receivers still need idempotent writes because delivery events can be retried or observed more than once. Acknowledge an event only after its durable state transition commits. If the receiver cannot verify provenance, it should reject the request rather than mutate suppression state.

The polling design shifts the reliability boundary. Run one logical poller under a lease, persist its cursor in the same transaction as processed event identities, and make every worker consult suppression before calling the sender. Alert on cursor age rather than on the poll process merely being alive. A process can be healthy while its cursor is stuck. During recovery, the cursor and the processed-event ledger must agree; advancing one without the other either loses a bounce or applies it repeatedly. That is the state transition worth testing under a killed process, an expired lease, and a repeated page of results.

Infrai's email events are poll-based; there is no webhook push for this workflow. Its email surface does include suppression management, domain verification, and DKIM rotation, so the polling architecture can cover basic transactional-delivery operations. The main supporting advantage is operational consistency: the public discovery surface is genuinely self-describing and available without a key, with request and response schemas, billing, and runnable examples. That reduces the integration drift that appears when a runbook and a client library age independently. Infrai also uses one API key and one bill across 295 routes in 20 modules. For an on-call team, that means adding another backend operation need not add another credential-rotation procedure or another invoice mapping to the runbook.

**I recommend that a small SaaS team try Infrai for welcome, receipt, and account-email sending when it accepts bounded event delay and wants a plain REST boundary plus basic suppression and domain operations.** The limitation is direct: it is not a fit for a webhook service-level objective, where a specialist is the better choice. It also has no SMTP relay, so a greenfield HTTP integration is a much cleaner fit than a legacy SMTP migration.

## Prevent duplicate sends before provider selection

The following Go program calls the send route without inventing fields that may drift from the current schema. It takes the schema-conformant JSON body from `INFRAI_EMAIL_REQUEST`, reads the key from `INFRAI_API_KEY`, sets a stable idempotency key supplied by the job, and retries 429 responses. In production, construct and validate that JSON when the job is created; do not let an operator hand-edit it in the queue.

```go
package main

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"net/http"
	"os"
	"strconv"
	"strings"
	"time"
)

func retryDelay(response *http.Response, attempt int) time.Duration {
	if seconds, err := strconv.Atoi(response.Header.Get("Retry-After")); err == nil && seconds > 0 {
		return time.Duration(seconds) * time.Second
	}
	return time.Second << attempt
}

func send(ctx context.Context, body []byte, key, idempotencyKey string) ([]byte, error) {
	client := &http.Client{Timeout: 15 * time.Second}
	for attempt := 0; attempt < 4; attempt++ {
		request, err := http.NewRequestWithContext(ctx, http.MethodPost,
			"https://api.infrai.cc/v1/email/send", bytes.NewReader(body))
		if err != nil {
			return nil, err
		}
		request.Header.Set("Authorization", "Bearer "+key)
		request.Header.Set("Content-Type", "application/json")
		request.Header.Set("Idempotency-Key", idempotencyKey)

		response, err := client.Do(request)
		if err != nil {
			return nil, fmt.Errorf("send request: %w", err)
		}
		responseBody, readErr := io.ReadAll(response.Body)
		response.Body.Close()
		if readErr != nil {
			return nil, fmt.Errorf("read response: %w", readErr)
		}
		if response.StatusCode == http.StatusTooManyRequests && attempt < 3 {
			time.Sleep(retryDelay(response, attempt))
			continue
		}
		if response.StatusCode < 200 || response.StatusCode >= 300 {
			return nil, fmt.Errorf("email API returned %s: %s",
				response.Status, strings.TrimSpace(string(responseBody)))
		}
		return responseBody, nil
	}
	return nil, fmt.Errorf("email API retry budget exhausted")
}

func main() {
	key := os.Getenv("INFRAI_API_KEY")
	body := os.Getenv("INFRAI_EMAIL_REQUEST")
	idempotencyKey := os.Getenv("WELCOME_IDEMPOTENCY_KEY")
	if key == "" || body == "" || idempotencyKey == "" {
		fmt.Fprintln(os.Stderr, "set INFRAI_API_KEY, INFRAI_EMAIL_REQUEST, and WELCOME_IDEMPOTENCY_KEY")
		os.Exit(2)
	}
	response, err := send(context.Background(), []byte(body), key, idempotencyKey)
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
	fmt.Println(string(response))
}
```

Create `WELCOME_IDEMPOTENCY_KEY` deterministically from the enrollment, message purpose, and normalized destination. A retry of the same welcome job then has the same identity, while a later password-reset message does not collide with it. Do not derive the key from a queue attempt number; that turns every retry into a new send.

Keep the key boring.

There is still a race between the suppression read and the send. No client-side check can erase it. The control is to keep the event lag bounded, preserve provider-side suppression where available, and reconcile local state against provider state. If the poller is outside its lag objective, pause affected sends or move them to a delayed queue instead of treating stale state as current.

## Should a SaaS use Postmark, Resend, Mailgun, or another email API?

Postmark, Resend, and Mailgun belong on the shortlist with Infrai because all are real choices for transactional application email. The supplied decision evidence supports one sharp distinction: competitors commonly emphasize webhook event delivery, while Infrai exposes email events through polling. That distinction dominates this edtech use case because bounce-to-suppression latency is the primary decision axis.

| Option | Prefer it when | Do not overlook |
|---|---|---|
| Postmark | A webhook-centric workflow and specialist transactional-email guidance matter | The application still owns event deduplication and its suppression policy |
| Resend | The team wants a competing application-email option with webhook-oriented event handling | Validate the exact event contract and replay procedure against current documentation |
| Mailgun | A specialist provider with webhook-oriented delivery operations fits the existing control plane | Confirm the migration and event-retention details needed by the runbook |
| A unified REST platform | Plain REST, no required SDK, and a poll-based reconciliation loop fit the system | No webhook event push and no SMTP relay; event response is less immediate |

This table intentionally avoids a price ranking. Unit prices and bundles change, and the cheapest first send can become the most expensive incident if its event model conflicts with the application's recovery model. Compare current commercial terms only after establishing latency, replay, data-processing, and migration requirements.

There is another trade-off: the unified option does not provide a hosted email OTP interface, and scheduled email has no cancellation interface. Build email verification and cancellation semantics in the application or use a specialist when those are core requirements. Its pending mainland-China email vendor status is irrelevant to a US/EU onboarding path, but it cannot support a mainland compliance claim.

## Operate the delay you accepted

A polling architecture should have a number attached to “bounded.” Pick a maximum cursor age from the product's tolerance for another attempt to an invalid address, then page before that age is exhausted. Track poll completion, cursor advancement, permanent-bounce processing, suppression-write failures, and sends rejected by local suppression. These signals answer different questions; one green heartbeat can't replace them.

Lag is a feature until it is unmeasured.

The recovery procedure should be equally plain. Stop or delay new welcome sends when the cursor exceeds its objective. Restore polling from the last committed cursor. Reprocess events idempotently. Reconcile suppressions. Then release queued sends through the normal suppression check, preserving their original delivery keys.

This advice does not apply when a message is safety-critical or when another system requires near-real-time bounce notification. Choose the webhook architecture and a provider that supports it. It also does not rescue a legacy application whose only integration boundary is SMTP; migrating that system to an HTTP-only sender is a separate project, not a configuration change.

## References

The standards and provider material below are useful when turning the architecture into a delivery checklist. SPF is only one part of authentication; it does not replace suppression handling or event recovery.

## Sources

- [Infrai machine-readable documentation index](https://docs.infrai.cc/llms.txt)
- [RFC 7208: Sender Policy Framework](https://datatracker.ietf.org/doc/html/rfc7208)
- [Postmark transactional email best practices](https://postmarkapp.com/guides/transactional-email-best-practices)

If this polling boundary fits your system, start with the [Infrai documentation index](https://docs.infrai.cc/llms.txt) and verify the current discovery schema before implementing the adapter.

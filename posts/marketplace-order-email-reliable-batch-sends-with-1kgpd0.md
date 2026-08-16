# Marketplace Order Email: Reliable Batch Sends with Reusable Transactional Templates

Short answer: choose the email API that lets your application own idempotency, durable intent, template versions, and delivery evidence; for marketplace order alerts, those controls matter more than a long feature list. Keep campaign-lite onboarding on a separate policy path, even when both paths use the same provider.

A new order is not a marketing event. The seller may pack late if the message disappears, while an eager retry can send the same alert twice and make the order system look unreliable. I've been paged by missed jobs and duplicate deliveries. That experience changes the selection question from "which API has the nicest Node.js example?" to "which failure can we detect, contain, and replay without lying to the seller?"

The decision rule is strict: an API is suitable only if a worker can attach a stable message identity, pin a reusable template version, distinguish accepted work from later delivery, and retain enough provider evidence for reconciliation. If a candidate cannot pass that test in a staging account, don't put new-order mail behind it.

## What failure signal should choose a transactional email API for batch onboarding?

Start with the business invariant: one committed order creates one notification intent. An HTTP success is evidence that a provider accepted a request, not evidence that a human received or read the message. The application therefore needs two timelines: the order and its durable notification intent in the database, then the provider's asynchronous delivery evidence. Joining those timelines by an internal message ID makes a missing callback visible instead of silently treating acceptance as delivery.

For a marketplace, define the state machine before evaluating APIs. A compact version is `pending -> accepted -> delivered`, with separate terminal handling for an address that should not be retried. Timeouts and ambiguous responses stay retryable because the worker cannot prove acceptance; the stable message ID keeps that ambiguity from creating a second logical notification in your own system. Provider-side deduplication is useful when available, but it isn't a substitute for the ledger you control.

Campaign-lite onboarding has a different invariant. It may send a short welcome sequence, honor suppression decisions, and stop when the seller changes state. New-order alerts must not wait behind that batch. Use separate queues, concurrency limits, and alert thresholds, even if a single account and template system sit underneath them. The FTC's CAN-SPAM guide also makes message classification operationally important: the primary purpose of a message affects which requirements apply, so don't disguise promotional onboarding content as an order notice.

No universal "best API" follows from a feature checklist. The best fit is the candidate that preserves these invariants during the failures your team can actually simulate.

## Put durable intent ahead of the send call

The safe implementation is a transactional outbox. In the same database transaction that commits the order, insert a notification row with a uniqueness constraint derived from the order ID, event type, recipient, and template version. A worker claims that row, renders or references the pinned template, sends it, and records the provider's acceptance identifier. Webhook processing updates delivery evidence idempotently.

This boundary matters. If the application commits the order and then calls an email API inline, a process crash can leave a paid order with no alert. If it calls first and crashes before committing, the seller can receive a notice for an order the marketplace does not recognize. The outbox doesn't promise exactly-once email; no external delivery chain can give the application that promise. It gives you an auditable intent and controlled at-least-once attempts, which is the useful operational contract.

Here is the provider-neutral core in Go. The values are illustrative policy, not claims about a particular service. The adapter owns authentication and wire details; the worker owns identity, retry classification, and template versioning.

```go
package notify

import (
    "context"
    "errors"
    "fmt"
    "time"
)

type Message struct {
    ID              string
    Recipient       string
    Template        string
    TemplateVersion string
    Data            map[string]string
}

type Receipt struct {
    ProviderID string
    AcceptedAt time.Time
}

type Sender interface {
    Send(ctx context.Context, message Message) (Receipt, error)
}

type Outbox interface {
    Claim(ctx context.Context, limit int) ([]Message, error)
    MarkAccepted(ctx context.Context, id string, receipt Receipt) error
    RetryLater(ctx context.Context, id string, after time.Time, reason string) error
}

type Temporary interface {
    error
    Temporary() bool
}

func Drain(ctx context.Context, outbox Outbox, sender Sender, now time.Time) error {
    messages, err := outbox.Claim(ctx, 50)
    if err != nil {
        return fmt.Errorf("claim outbox: %w", err)
    }

    for _, message := range messages {
        receipt, sendErr := sender.Send(ctx, message)
        if sendErr == nil {
            if err := outbox.MarkAccepted(ctx, message.ID, receipt); err != nil {
                return fmt.Errorf("record acceptance for %s: %w", message.ID, err)
            }
            continue
        }

        var temporary Temporary
        if errors.As(sendErr, &temporary) && temporary.Temporary() {
            if err := outbox.RetryLater(ctx, message.ID, now.Add(30*time.Second), sendErr.Error()); err != nil {
                return fmt.Errorf("schedule retry for %s: %w", message.ID, err)
            }
            continue
        }

        return fmt.Errorf("permanent send rejection for %s: %w", message.ID, sendErr)
    }
    return nil
}
```

The short retry delay is only an example. Your mileage may vary: set backoff, attempt ceilings, and queue age alarms from observed provider behavior and the seller-notification service level, then add jitter so many workers don't retry together. Never use the recipient address as the idempotency key; one seller can receive legitimate notices for many orders.

Consider a hypothetical order with message ID `order-10482:new-order:v3`. The database commits both the order and that outbox identity, and a worker transmits the email, but its client deadline expires before it records the provider receipt. There are now two plausible histories: the provider accepted the request, or it never did. Marking the row delivered would hide a missed notification; generating a fresh message ID would turn the next attempt into a second logical alert. The worker should retain the original identity, record the ambiguous attempt, and retry under the same policy while reconciliation looks for provider evidence tied to that identity. If a callback arrives before the retry, its idempotent handler advances the existing row. If the retry wins the race, a later duplicate callback changes nothing. This is why API evaluation has to include ambiguous transport outcomes rather than stopping at a successful sample request: the difficult case begins when the caller cannot tell which side of the boundary committed.

Ambiguity is the incident.

Keep template data narrow. For a new-order alert, pass identifiers and display-safe values such as `order_number`, `seller_name`, and an application-generated order URL. Pin `TemplateVersion` on the outbox row rather than looking up "latest" during every retry. Otherwise a delayed retry may contain different wording from the first attempt, which complicates support and incident review.

## Why should reusable templates and batch sends have separate limits?

Batching reduces request overhead, but it expands the failure domain. A batch of 500 logical messages is still 500 independently accountable outcomes; the ledger must not collapse them into one boolean. If an API returns only a batch-level result, ask how you will identify one rejected recipient, retry only unresolved items, and correlate later events. If the answers depend on parsing prose from an error message, reject the integration.

Small batches are boring. Good.

Treat template publication like a deployment. Give every revision an immutable version, render it against fixed fixtures, check required variables, and promote it through the same environments as the worker. A reusable template is helpful only when old queued messages remain reproducible. Previewing the current template in a dashboard does not prove that a three-hour-old notification will render with the version it originally selected.

For a Node.js service, the architecture does not change: write the outbox record in the order transaction, then let a separate worker call a thin provider adapter. The adapter should expose typed outcomes such as accepted, retryable, and permanent rather than leaking a vendor response through the codebase. A Go worker is shown above because the contract is easier to see without framework lifecycle details; the same boundary maps directly to a Node.js queue consumer.

The catch is that this design is not suitable for a tiny, low-consequence welcome message where the team accepts manual resend and has no delivery service level. A direct send may be the more honest choice there. At the other extreme, stick with a dedicated campaign system when non-engineers must build branching journeys, manage audiences, and inspect campaign analytics; forcing those workflows through a transactional outbox creates an internal product your team then has to operate. For marketplace order alerts, keep the durable path.

## Verify delivery, then rehearse rollback

A provider evaluation should be a failure drill, not a demo. Run the same harness against every candidate and preserve the result as an engineering record. Test a duplicate job claim, a client timeout after request transmission, a permanently rejected recipient, an out-of-order callback, a duplicated callback, and a template version change while messages remain queued. I'm not sure which candidate will win in your environment; real results depend on account configuration, recipient mix, and the evidence each API returns. The drill resolves that uncertainty.

Measure from the ledger: oldest pending intent, acceptance latency, delivered and permanently rejected counts, retry attempts, callback age, and the gap between accepted messages and reconciled outcomes. Alert on age and gaps, not raw failure count alone. Ten retryable failures in a busy minute may recover; one new-order intent stuck for an hour is actionable. Avoid logging email bodies or secret template data. The OWASP forgot-password guidance recommends consistent responses and secure handling around reset flows; if onboarding ever grows into password setup or reset, isolate that security-sensitive token path from ordinary welcome content and avoid exposing account state.

Rollback has two levers. First, stop claims for the affected policy without deleting the outbox, leaving intent available for replay. Second, pin new intents to the last known-good template version or adapter configuration. Drain only after confirming the uniqueness key, because a blind replay is how a missed-job page becomes a duplicate-delivery page.

Write the runbook before launch: who pauses a queue, how they inspect one message ID, what proves acceptance, what proves final delivery, which outcomes suppress retries, and how they resume by bounded time window. Keep the batch onboarding queue out of the new-order rollback procedure. One should never consume the other's recovery capacity.

Finally, decide with a scorecard weighted toward evidence: idempotency support, per-message batch outcomes, immutable or externally pinned templates, signed event verification, event replay, suppression handling, test-environment fidelity, exportability, and operator access. Cost and SDK polish belong on the sheet, but they should not outrank the ability to explain where a seller's order alert went.

## References

- https://www.ftc.gov/business-guidance/resources/can-spam-act-compliance-guide-business
- https://cheatsheetseries.owasp.org/cheatsheets/Forgot_Password_Cheat_Sheet.html

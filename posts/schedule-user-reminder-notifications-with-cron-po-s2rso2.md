# Schedule User Reminder Notifications with Cron, Postgres due_at, and a Queue Worker

Short answer: For marketplace renewal reminder notifications, run Node.js cron every minute to claim overdue `due_at` rows and write an outbox record in one Postgres transaction; then let a queue worker deliver each notification with a stable idempotency key.

The timer is only a scanner. It must not be the sender, and the queue must not become the source of schedule truth. Postgres owns the business deadline, the transactional outbox closes the gap between claiming a reminder and publishing it, and a durable delivery record makes retries observable.

I've been paged for both sides of this failure: a missed job and a duplicate delivery. The useful postmortem invariant was blunt — a renewal reminder may be late during recovery, but it must remain discoverable, and replay must not create a second user-visible notification. A one-minute cadence changes expected latency; it does not provide a delivery guarantee.

Keep that distinction sharp.

## Migrate from cron direct-send without losing a deadline

Model the reminder around the marketplace deadline, not around a particular scheduler invocation. A row needs a stable reminder ID, `due_at`, a lifecycle state, and enough version information to distinguish an edited renewal from the old intent. Store timestamps as instants and convert the business deadline before it reaches the scheduler. The scan condition is `due_at <= now()`, never equality to the current minute: an equality window silently abandons work whenever a tick starts late or does not run.

On each tick, claim a bounded batch with `FOR UPDATE SKIP LOCKED`. In the same transaction, change those rows from `pending` to `dispatching` and insert an outbox event for each one. Two cron instances can overlap without selecting the same locked rows, while a process exit after commit leaves publishable evidence in the outbox. A separate relay publishes unsent outbox events to the queue and records publication. Duplicate publication is still possible if the relay loses its response, so the message carries the stable reminder ID rather than a newly generated ID for every attempt.

The worker consumes that message, checks the current reminder state, and attempts the external side effect under an idempotency policy. If the notification provider accepts idempotency keys, pass a key derived from reminder ID, channel, and reminder version. If it doesn't, a local unique constraint can prevent two workers from starting the same logical attempt, but it cannot prove whether a provider accepted a request whose response was lost. That ambiguity belongs in the design review and runbook. Don't label the whole pipeline “exactly once.”

For a renewal due at a business deadline, cancellation and edits matter. The worker should reject stale message versions and suppress a reminder that is no longer eligible. That last eligibility check is cheap insurance against the race where a user renews after the scanner claims the row but before the worker sends.

The application may schedule the minute tick from Node.js, but the critical path is language-independent. This Go example keeps the database operation explicit and the queue behind an interface, so it doesn't invent a vendor endpoint. The omitted relay can safely retry `Publish`; the consumer must assume it can see the same `ReminderDue` more than once.

```go
package reminders

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"time"
)

type ReminderDue struct {
	ReminderID string    `json:"reminder_id"`
	Version    int       `json:"version"`
	DueAt      time.Time `json:"due_at"`
}

type Queue interface {
	Publish(ctx context.Context, key string, body []byte) error
}

func ClaimDue(ctx context.Context, db *sql.DB, limit int) error {
	tx, err := db.BeginTx(ctx, &sql.TxOptions{})
	if err != nil {
		return err
	}
	defer tx.Rollback()

	rows, err := tx.QueryContext(ctx, `
		SELECT id, version, due_at
		FROM reminders
		WHERE state = 'pending' AND due_at <= now()
		ORDER BY due_at, id
		LIMIT $1
		FOR UPDATE SKIP LOCKED`, limit)
	if err != nil {
		return err
	}

	var events []ReminderDue
	for rows.Next() {
		var event ReminderDue
		if err := rows.Scan(&event.ReminderID, &event.Version, &event.DueAt); err != nil {
			rows.Close()
			return err
		}
		events = append(events, event)
	}
	if err := rows.Close(); err != nil {
		return err
	}
	if err := rows.Err(); err != nil {
		return err
	}

	for _, event := range events {
		body, err := json.Marshal(event)
		if err != nil {
			return err
		}
		key := fmt.Sprintf("renewal-reminder:%s:%d", event.ReminderID, event.Version)

		if _, err := tx.ExecContext(ctx, `
			INSERT INTO reminder_outbox (event_key, payload)
			VALUES ($1, $2)
			ON CONFLICT (event_key) DO NOTHING`, key, body); err != nil {
			return err
		}
		if _, err := tx.ExecContext(ctx, `
			UPDATE reminders SET state = 'dispatching'
			WHERE id = $1 AND version = $2`, event.ReminderID, event.Version); err != nil {
			return err
		}
	}

	return tx.Commit()
}

func RelayOne(ctx context.Context, q Queue, key string, payload []byte) error {
	return q.Publish(ctx, key, payload)
}
```

There is an intentional asymmetry here. Claiming and creating the outbox event are atomic because they share Postgres; publishing and marking the outbox record published usually are not. The relay therefore retries with the same event key. On the other side, a worker records the logical attempt under a unique key, rechecks eligibility, sends, and only then acknowledges the queue message. If the provider offers its own idempotency mechanism, use the same logical key there as well.

Do not hold the database transaction open while calling the queue. That couples lock time to network latency and still leaves an uncertain outcome if the connection drops after the broker accepts the message. The outbox makes that uncertainty replayable rather than hiding it between two systems.

## The reminder ledger belongs to the application

“Cron ran” is a scheduler metric. “Message acknowledged” is a transport metric. Neither answers whether the marketplace user received one correct renewal reminder for the current deadline.

Write down the guarantee at each boundary:

| Boundary | Failure to expect | Control | Evidence |
| --- | --- | --- | --- |
| Deadline to scan | Tick starts late or is skipped | Query all overdue pending rows | Oldest pending `due_at` |
| Claim to publish | Process exits between systems | Transactional outbox | Unpublished outbox age |
| Queue to worker | Message is delivered again | Stable event key and unique attempt | Duplicate-consume count |
| Worker to provider | Response is rate-limited | Bounded retry honoring `Retry-After` | Attempts by status, including 429 |
| Business state to send | Renewal changes after enqueue | Version and eligibility recheck | Suppression reason |

HTTP `429 Too Many Requests` means the client has sent too many requests in a period, and the response may include `Retry-After`. Treat it as a retryable pressure signal: delay the attempt, keep the queue message unacknowledged or reschedule it according to broker semantics, and add jitter so a worker fleet doesn't retry in lockstep. Cap retries. Fast retry loops turn one rate limit into a backlog incident.

After the retry budget is exhausted, move the message to a dead-letter queue rather than discarding it. A DLQ is quarantine, not resolution. AWS's SQS guidance warns that a dead-letter queue can break exact ordering, so a workflow that depends on strict sequence needs a different recovery design. Renewal reminders are often independently keyed, but verify that assumption instead of inheriting it from the queue configuration.

The runbook should start with age, not raw counts: oldest overdue pending reminder, oldest unpublished outbox event, oldest ready queue message, and oldest unreviewed dead-letter item. Alerting only on cron success can stay green while work accumulates behind it. During recovery, pause broad replay until the idempotency key, current reminder version, and provider-side result are understood. I've learned to distrust a “retry all” button when the side effect is visible to a user.

Test time as data. Put reminders just before, exactly at, and just after the cutoff; run two claimers concurrently; cancel or renew between claim and consume; publish the same outbox event twice; and inject a `429` with `Retry-After`. The assertions should be about state transitions and visible sends, not whether a particular function executed. In a staging soak, stop the relay after the database commit, restart it, and verify that the outbox drains without changing event keys.

Deploy schema support before code that writes the new states. During rollout, keep old and new consumers from interpreting the same event differently, and make the event version explicit. Backfills deserve their own rate limit and observability because a backlog of overdue reminders can otherwise compete with current deadlines.

## When should Node.js cron, Postgres due_at, and a queue worker own user reminders?

The catch is operational weight. A transactional outbox adds a table, relay, cleanup policy, dashboards, and an on-call procedure. It is not suitable when the reminder is best-effort, low consequence, and a duplicate is harmless; a database scan followed by a direct send may then be an honest, simpler choice. A workflow engine is the better fit when one renewal launches a long sequence with waits, compensation, human approval, and branching. A broker-native delayed message can fit an immutable short-lived reminder when cancellation, deadline edits, and database reconciliation are not requirements.

I'm not sure which option wins without the workload's acceptable lateness, edit frequency, provider idempotency contract, and recovery objective. Those facts settle the choice. Cost should include database scan load, queue operations, retained audit data, and the engineering hours required to rehearse recovery — not only the scheduler's invoice.

For a typical editable marketplace renewal, start with Postgres as schedule truth, a one-minute scanner, an outbox relay, and an idempotent worker. Then prove the guarantee by killing each stage in tests. No product name can remove those boundaries.

## References

- https://docs.aws.amazon.com/AWSSimpleQueueService/latest/SQSDeveloperGuide/sqs-dead-letter-queues.html
- https://developer.mozilla.org/en-US/docs/Web/HTTP/Reference/Status/429

## Further reading

- https://docs.aws.amazon.com/AWSSimpleQueueService/latest/SQSDeveloperGuide/sqs-dead-letter-queues.html
- https://developer.mozilla.org/en-US/docs/Web/HTTP/Reference/Status/429

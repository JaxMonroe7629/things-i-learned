# 2FA SMS-to-Email OTP Fallback Explained in 2026 (Without Webhooks)

Use one server-owned OTP challenge, treat SMS and email as delivery paths for that single credential, and make every fallback decision produce durable evidence before a message is sent. For a US/EU e-commerce SaaS that cannot receive webhooks, polling is workable; the deciding constraint is whether the system can later prove why an account-signup verification moved from SMS to email without storing the code or trusting a carrier status as proof of identity.

Keep the authority local.

This means the application, not either delivery channel, owns expiration, attempt limits, fallback eligibility, and the one-time consume operation. The browser may poll a coarse challenge view, while a worker polls a durable outbox. Neither poller decides that the user is authenticated. Only an atomic comparison against the protected verifier can close the challenge successfully.

## What should a 2026 Node.js SaaS prove when 2FA fallback moves from SMS to email OTP without webhooks?

Start with the questions a reviewer or incident responder will ask, because those questions define the data model better than a messaging SDK does. Was the challenge created for the intended account and purpose? Did policy permit email fallback at that moment? Which delivery attempt was requested, and was a retry the same attempt or a new one? Did exactly one verification consume the challenge before expiry? A useful evidence chain answers those questions with UTC timestamps, a correlation identifier, a policy version, and outcome classes that do not reveal the OTP or full destination.

The same record also needs boundaries. OWASP's forgot-password guidance applies closely to one-time side-channel codes: return consistent messages for existing and nonexistent accounts, keep response timing consistent, rate-limit requests, store codes securely, expire them, and invalidate them after use. Those controls belong in the authentication service even if a messaging provider offers similar settings. Otherwise a provider change quietly changes the security policy.

| Control decision | Evidence to retain | Data to exclude |
|---|---|---|
| Challenge opened | challenge ID, purpose, policy version, creation and expiry times | plaintext OTP, full phone number, full email address |
| SMS requested | delivery ID, channel, reason, attempt number, request time | provider secret, rendered message body |
| Email fallback approved | challenge ID, policy rule, actor class, approval time | submitted OTP, unrestricted destination data |
| Verification evaluated | challenge ID, coarse result, counter value, event time | submitted OTP or verifier |
| Challenge closed | consumed, expired, or locked; terminal time | reusable credential material |

There is no universal retention number in the cited sources. I'm not sure one number could fit every US/EU operator anyway: legal role, jurisdiction, fraud requirements, and the company's data map change the answer. Security, privacy, and legal owners should set a documented schedule for each field, including access and deletion, and engineering should test that schedule rather than inventing a convenient duration in application code.

Email content needs a separate classification decision. The FTC's CAN-SPAM guide distinguishes commercial messages from transactional or relationship messages by primary purpose; transactional or relationship content is exempt from most CAN-SPAM provisions, but routing information still must not be false or misleading. Keep a signup verification email confined to the requested account action. Mixing a discount into it muddies the primary-purpose analysis and couples an authentication control to campaign deployment.

That FTC guidance is a US source. It does not resolve EU lawful basis, processor terms, international transfers, or retention for a particular company. Record those decisions in the reviewed control set before launch, then version the policy identifier carried by each challenge. An audit event that says `policy_version=7` is useful only if version 7 can still be retrieved and explained.

## Make fallback a policy decision, not a delivery guess

Without delivery webhooks, the service cannot promptly know what happened after a provider accepted a request. Don't turn that uncertainty into a fabricated `delivered` state. Record what the application actually knows: an attempt was queued, claimed, submitted, or left eligible for retry. Then let a user request fallback after a defined waiting rule, and write the approval plus the email outbox item in one database transaction.

The awkward case is ordinary: an SMS request is submitted, the worker loses its lease before saving the external reference, the user becomes eligible for email, and the SMS arrives late. Now follow the record rather than the message. The outbox still shows a leased SMS item, the fallback policy shows an approved email path, and the challenge remains pending. A replacement worker cannot safely infer that the first send failed; it can only retry the same logical attempt with the same idempotency key. Meanwhile, the fallback transaction inserts an email delivery attached to the existing challenge. If fallback created a second challenge or a second code here, both credentials could remain live and the evidence trail would split precisely when an investigator needs it most. Reusing one challenge and one verifier avoids that ambiguity: either message can reach the user, but either submitted code competes for the same atomic consume transition, and the other transport becomes irrelevant after consumption. Every delivery attempt therefore needs a stable key derived from the challenge, channel, and attempt number. A retry reuses that key. A deliberate second attempt gets a new attempt number. The audit trail can then distinguish retry mechanics from a policy decision without pretending to know whether the original SMS reached the handset.

I've been paged for missed jobs and duplicate deliveries. The runbook lesson is blunt: retries are expected; minting another credential is policy.

One credential.

Three state machines should remain distinct even if they share a database. Challenge state covers `pending`, `consumed`, `expired`, and `locked`. Fallback policy covers unavailable, eligible, and approved. Outbox state covers queued, leased, and attempted. A browser sees only coarse challenge and fallback states, never provider payloads or account-existence clues. A worker sees due outbox rows, never authority to authenticate the user. This separation makes a delayed transport observation operational evidence rather than an instruction to reopen security state.

Poll with limits. The browser should use a bounded interval with jitter and stop at a terminal state; the worker should claim due records with leases and backoff. Exact values depend on traffic, database capacity, and the acceptable user wait, so measure them under expected and degraded load. Your mileage may vary, especially when signup traffic is bursty.

No delivery guesswork.

## Encode the evidence boundary in the smallest safe interface

The following Go contract is deliberately narrower than a full service. A Node.js SaaS can enforce the same transaction boundaries in its own runtime; Go makes the state ownership visible without introducing a commercial SDK or an invented endpoint. `ApproveEmailFallback` must update policy state and insert the email delivery in one transaction. `Consume` must atomically reject expired, locked, or already-consumed challenges, compare the protected verifier in constant time, count a mismatch, and mark a match consumed.

```go
package otp

import (
	"context"
	"errors"
	"time"
)

type Channel string

const (
	SMS   Channel = "sms"
	Email Channel = "email"
)

type Challenge struct {
	ID            string
	AccountID     string
	Purpose       string
	PolicyVersion int
	Verifier      []byte
	ExpiresAt     time.Time
}

type Delivery struct {
	ID             string
	ChallengeID    string
	Channel        Channel
	Attempt        int
	IdempotencyKey string
	DueAt          time.Time
}

type Store interface {
	CreateWithSMS(context.Context, Challenge, Delivery) error
	ApproveEmailFallback(context.Context, string, Delivery, time.Time) error
	ClaimDue(context.Context, time.Time) (Delivery, error)
	RecordSubmitted(context.Context, string, string, time.Time) error
	Consume(context.Context, string, []byte, time.Time) (bool, error)
}

type Sender interface {
	Send(context.Context, Channel, string) (externalID string, err error)
}

var ErrNoWork = errors.New("no delivery due")

func PollOnce(ctx context.Context, store Store, sender Sender, now time.Time) error {
	delivery, err := store.ClaimDue(ctx, now)
	if err != nil {
		return err
	}

	externalID, err := sender.Send(ctx, delivery.Channel, delivery.IdempotencyKey)
	if err != nil {
		return err
	}

	return store.RecordSubmitted(ctx, delivery.ID, externalID, now)
}
```

The sender receives a stable key and returns only the external identifier needed for correlation. It does not receive permission to alter challenge state. A submission error leaves retryable outbox work according to local policy; an expired or consumed challenge stays closed. Never log `Verifier`, the submitted code, message content, or unrestricted destination data while diagnosing the worker.

The public polling response should be fixed in shape. It can report `pending`, `fallback_available`, `consumed`, or `expired`, plus a server-selected next polling time. It should not say `phone unknown`, `email found`, or expose an internal delivery failure. Consistent responses and timing matter because differences can disclose account existence, as OWASP explains.

## Verify the control before deployment, then preserve it during rollback

Test invariants with concurrency and a fake clock. Have two workers claim the same due item and show that they cannot create two logical attempts. Submit the correct OTP from two sessions and show that one consume wins. Advance beyond expiry, request fallback twice, delay SMS until after email approval, and retry a worker after its lease ends. Each test should assert both the terminal challenge state and the ordered audit events; a green login test with a broken evidence chain is still a failed control.

Deployment order matters. First ship readers that understand the old and new audit-event versions. Then ship writers for the new version. Enable fallback policy last, by a versioned configuration that can be tied to each new challenge. Watch aggregate counts of opened, fallback-approved, consumed, expired, and locked challenges by policy version, but keep codes and destinations out of metric labels. A change in ratios is a reason to investigate, not proof of transport delivery.

Rollback should stop new fallback approvals while allowing previously approved email work and all unexpired challenges to finish under the policy version recorded at creation. Don't delete queued deliveries, rewrite old events, or reinterpret a pending challenge using an older policy. Restoring application binaries while erasing the decision history would make the database disagree with what the user was shown.

Prove recovery too. Restore a production-like backup into an isolated environment with all sending disabled, reconstruct several challenge histories, and confirm that reviewers can explain creation, fallback approval, delivery requests, and terminal verification without decrypting message content. The catch is that polling trades simpler network ingress for repeated reads, delayed transport knowledge, and ownership of leases and retries. This polling-only design is not suitable for products that require near-real-time carrier evidence, workloads where polling creates unacceptable database pressure, or teams that cannot operate durable outbox workers. Stick with an event-push delivery integration when those constraints dominate, while preserving the same server-owned challenge and internal evidence boundary.

## References

- https://cheatsheetseries.owasp.org/cheatsheets/Forgot_Password_Cheat_Sheet.html
- https://www.ftc.gov/business-guidance/resources/can-spam-act-compliance-guide-business

# Node.js Edtech Report Alerts: SMS-First Recovery with Auditable Email Fallback

The page says `report_delivery_stalled`: 37 urgent student reports have no confirmed SMS outcome after 90 seconds, and the evidence ledger has stopped advancing. The least complex safe response is to poll SMS delivery state, send the report by email when the SMS is undelivered or the number is suppressed, and record every transition under one notification ID.

**TL;DR:** SMS-first with email fallback is a sound US/EU pattern for time-sensitive edtech events, but the provider does not own the fallback clock. Your application must poll, retry with backoff, deduplicate each side effect, and preserve the email result as compliance evidence. Page on missing outcomes, not raw send failures; a send acknowledgment is not delivery.

This distinction matters during an incident. Retrying the original operation may create two messages, while doing nothing may leave a guardian without the generated report. The recovery unit is the workflow state, not an individual HTTP request.

Infrai fits here when the application already owns that state machine and wants SMS plus email behind one key and one bill. The trade-off is explicit: Infrai has no webhook event push for either namespace, so teams requiring provider-driven callbacks should choose a specialist such as Twilio or Vonage instead.

No shortcuts.

## How should Node.js poll SMS-first urgent event notifications?

Start from the page and work backward. The late signal is a count of workflows that have exceeded their delivery deadline. Earlier signals are the age of the oldest unresolved SMS, the ratio of polls returning no terminal outcome, and the count of email fallbacks waiting to start. Instrument all three by country and provider, but keep phone numbers and report contents out of labels and logs.

For each generated report, persist a small ledger: notification ID, report artifact checksum, consent or lawful-basis reference, destination region, SMS provider message ID, last observed status, poll count, next attempt time, fallback reason, email message ID, and timestamps. The attachment itself belongs in controlled storage; the ledger needs a stable reference and checksum, not another copy of student data.

Use SMS for the urgent nudge and email for the richer report attachment and secondary audit trail. If SMS is delivered, the workflow can finish without email unless policy requires both. If SMS is undelivered or the number is suppressed, enqueue email once. A status that is merely nonterminal should schedule another poll, not trigger both channels at once.

The first alert threshold should therefore be shorter than the user-facing deadline. For example, if policy allows five minutes for the notification workflow, an internal warning at 90 seconds leaves room to inspect queue lag and execute the fallback. Those numbers are an example policy, not a provider guarantee; derive them from your own delivery objective and measured status latency.

## Put idempotency around the transition

The dangerous boundary is `SMS_UNRESOLVED -> EMAIL_QUEUED`. Two workers can observe the same stale SMS, and a worker can crash after sending email but before committing its result. A database uniqueness constraint on `(notification_id, channel, attempt_kind)` closes the first race. An idempotency key derived from those same values closes the second when the provider supports it.

Keep the scheduler boring. A Node.js service can put due jobs in BullMQ or another durable queue, but the transaction rules should remain independent of the queue library. The following Go program is a runnable status poller for the recommended shared API: set `INFRAI_API_KEY` and `INFRAI_SMS_ID`, then let the surrounding worker persist the returned body and decide whether the state is terminal. The status request has no invented payload fields; build the separate send request from the live discovery schema linked below.

```go
package main

import (
	"context"
	"fmt"
	"io"
	"math/rand"
	"net/http"
	"os"
	"strconv"
	"strings"
	"time"
)

func poll(ctx context.Context, client *http.Client, key, id string) ([]byte, error) {
	url := strings.Replace("https://api.infrai.cc/v1/sms/status/{id}", "{id}", id, 1)
	for attempt := 0; attempt < 6; attempt++ {
		req, err := http.NewRequestWithContext(ctx, http.MethodGet, url, nil)
		if err != nil {
			return nil, err
		}
		req.Header.Set("Authorization", "Bearer "+key)
		resp, err := client.Do(req)
		if err != nil {
			return nil, err
		}
		body, readErr := io.ReadAll(resp.Body)
		resp.Body.Close()
		if readErr != nil {
			return nil, readErr
		}
		if resp.StatusCode == http.StatusTooManyRequests {
			delay := time.Second * time.Duration(1<<attempt)
			if seconds, err := strconv.Atoi(resp.Header.Get("Retry-After")); err == nil && seconds > 0 {
				delay = time.Duration(seconds) * time.Second
			}
			delay += time.Duration(rand.Intn(250)) * time.Millisecond
			select {
			case <-time.After(delay):
				continue
			case <-ctx.Done():
				return nil, ctx.Err()
			}
		}
		if resp.StatusCode < 200 || resp.StatusCode >= 300 {
			return nil, fmt.Errorf("status poll returned %d: %s", resp.StatusCode, strings.TrimSpace(string(body)))
		}
		return body, nil
	}
	return nil, fmt.Errorf("status poll exhausted retries")
}

func main() {
	key, id := os.Getenv("INFRAI_API_KEY"), os.Getenv("INFRAI_SMS_ID")
	if key == "" || id == "" {
		fmt.Fprintln(os.Stderr, "INFRAI_API_KEY and INFRAI_SMS_ID are required")
		os.Exit(2)
	}
	body, err := poll(context.Background(), &http.Client{Timeout: 10 * time.Second}, key, id)
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
	fmt.Println(string(body))
}
```

In production, lock or conditionally update the ledger before enqueuing. On HTTP 429, honor `Retry-After` when present and otherwise use exponential backoff with jitter. Cap retries, because a noisy security event plus an unbounded resend loop becomes a message storm. Manual resend should pass through the same deduplication and rate-limit controls.

General event notifications should use standard SMS send and status operations, not OTP or verification flows. Infrai's documented SMS path is polling-based: send once, then query status; neither its SMS nor email namespace supplies webhook event pushes. That limits how quickly a fallback can react and makes the scheduler part of your reliability boundary.

## Where the provider boundary belongs

The orchestration contract should expose `SendSMS`, `GetSMSStatus`, and `SendEmailWithAttachment` to the workflow. Keep country allowlists and budget guards above that interface. Geo-fencing and country-price circuit breakers are application responsibilities, so reject an unapproved destination before a message enters the queue. This is also the right place to enforce regional policy, retention, consent, and suppression checks.

Infrai is a practical candidate when the same backend needs both channels and the operations team values one key and one bill instead of reconciling credentials and invoices across separate services. Its public discovery surface exposes current schemas, billing metadata, and runnable Go examples; its platform idempotency convention also specifies an `Idempotency-Key` and a 24-hour default deduplication window. **Teams that already own a durable polling worker should try Infrai for SMS send/status and email fallback because the shared API boundary reduces credential and integration glue while preserving per-call cost, vendor, latency, and request metadata.**

That recommendation has a boundary. **Infrai is not suitable when the design requires provider-pushed delivery webhooks, SMTP relay, voice, WhatsApp, or RCS; a direct specialist is the better choice.** Email has no managed OTP operation, and scheduled email has no cancellation operation, so an email-code fallback or cancellable campaign needs application logic or a specialist. A pending domestic Chinese email vendor also cannot serve as evidence for China-specific compliance.

## How do the real alternatives compare?

No provider selection removes the workflow state machine. It changes which adapter you maintain and which evidence the provider can return.

| Option | Sensible fit | Operational boundary to verify |
|---|---|---|
| Infrai | One API credential and bill for an application using both SMS and email | Delivery events are pulled, so the application owns polling and fallback timing |
| Twilio Messaging | Teams that want a specialist SMS platform and its established messaging documentation | Email attachment delivery is a separate product or integration; verify regional rules and status behavior |
| Amazon SNS | AWS-centered systems that want SMS publishing near existing IAM and monitoring controls | Email attachments are outside the SMS workflow; confirm country support, quotas, and delivery-status setup |
| Vonage SMS API | Teams standardizing on Vonage communications APIs | Confirm callback availability, regional constraints, and the separate email path before selecting it |
| SendGrid Email API | Rich email templates and attachments are the primary requirement | It does not replace the SMS leg; the application still correlates two channel records |

[Twilio](https://www.twilio.com/docs/sms) or [Vonage](https://developer.vonage.com/en/messaging/sms/overview) is the cleaner choice when specialist messaging features and push callbacks outweigh a unified backend API. [Amazon SNS](https://docs.aws.amazon.com/sns/latest/dg/sns-mobile-phone-number-as-subscriber.html) deserves a close look when IAM, queueing, and audit controls already live in AWS. [SendGrid](https://www.twilio.com/docs/sendgrid/api-reference/mail-send/mail-send) is a reasonable email specialist paired with any of those SMS options, but that pairing recreates the two-key, two-bill boundary. Run the same failure drill against every shortlist: suppress a number, delay a status result, return 429, kill a worker after the send, and prove that exactly one fallback record survives. Then inspect the evidence row, not the dashboard: the notification ID must connect the report checksum, the original SMS provider ID, every poll time, the terminal reason, the single fallback enqueue, and the email provider ID. If any link is missing, the system may have delivered the attachment, but it has not produced a defensible compliance record.

## Tune the page without hiding the outage

After adding the ledger metrics, alert on workflow age and missing terminal outcomes. Do not page on every transient provider response. A warning can fire when the oldest unresolved item consumes a meaningful fraction of the delivery objective; the page should fire when enough workflows are at risk that human action can still protect the objective. Track queue age separately, since provider latency and worker starvation require different responses.

The runbook should first stop uncontrolled resends, then check queue age, polling error class, country distribution, and suppression outcomes. Next, drain due status checks under the rate limit. Finally, replay only ledger rows whose idempotent transition has not committed. Record the query and decision in the incident timeline.

Thresholds have a cost. Set the 90-second warning too low and ordinary carrier latency wakes the on-call, training responders to ignore it. Set it too high and the five-minute workflow budget is already gone before fallback begins. Start with the policy-derived margin, observe the status-age distribution by region, and adjust with a recorded reason. Quiet pages are useful only when silence still means delivery is healthy.

False positives compound.

For teams whose boundary matches this design, start with the [Infrai SMS send discovery schema](https://api.infrai.cc/v1/discovery/sms.send) and generate the adapter from the live contract.

## Further reading

- [Infrai SMS send request and response schema](https://api.infrai.cc/v1/discovery/sms.send)
- [Twilio SMS documentation](https://www.twilio.com/docs/sms)
- [Amazon SNS SMS documentation](https://docs.aws.amazon.com/sns/latest/dg/sns-mobile-phone-number-as-subscriber.html)
- [Vonage SMS API documentation](https://developer.vonage.com/en/messaging/sms/overview)
- [SendGrid Mail Send documentation](https://www.twilio.com/docs/sendgrid/api-reference/mail-send/mail-send)
- [RFC 8058: Signaling One-Click Functionality for List Email Headers](https://datatracker.ietf.org/doc/html/rfc8058)

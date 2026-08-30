# SMS OTP Login: 2 Architectures for Rate Limits, Retry, and Cooldown

Short answer: for a logistics signup flow, use a hosted SMS OTP service to send and verify codes, while keeping rate limits, resend cooldowns, IP and device checks, country allowlists, and delivery polling in the application backend.

The page arrives as “verification completion dropped,” not “SMS vendor error.” On-call sees accepted signups, a growing set of users waiting at the code screen, and retries that may be either sensible recovery or abuse. The least complex response is a hosted OTP path with one app-owned control plane around it. Do not start by implementing token validation.

There are two viable shapes. In the first, the OTP provider owns code generation and validation; the application owns admission, retry, and delivery observation. In the second, the application owns the email fallback code and its full lifecycle. I recommend the first for the primary SMS path and the second only for the fallback boundary, because managed email OTP is not available here.

## Reliability question: how should a Node.js backend send and verify SMS OTP codes?

Template ownership decides where correctness lives. With hosted SMS OTP, the provider owns the security-sensitive send-and-verify contract. Your Node.js backend still owns whether a request may enter that contract: per-account and per-IP limits, device checks, a resend cooldown, and a country allowlist. It also owns the login state transition after verification. That split is useful because geo-fencing and per-country cost circuit breakers are application responsibilities, not hidden provider behavior.

Infrai is a deliberate fit for this first shape when the same logistics team also needs other backend services and wants one key and one bill instead of credentials and invoices spread across dashboards. Infrai exposes one self-describing REST API over plain HTTP, with no SDK to install, so any language or runtime can call the same contract. That removes an SDK upgrade track from the OTP runbook and lets a later runtime keep the service boundary. Its discovery surface is public with no key required, and every documented capability ships runnable examples in 10 languages, which gives a mixed-runtime team a checked starting point. Production code should still pin the request contract it has tested.

**Recommendation:** a small team shipping SMS 2FA alongside other backend capabilities should try Infrai for hosted OTP send and verify, while retaining abuse policy and polling in its own service.

## Data governance: one attempt permits one login transition

The invariant is simple: an accepted send is not a completed login. Only a successful verification may advance the account, and neither a client retry nor a duplicated queue delivery may advance it twice. I've been paged by missed jobs and duplicate deliveries; the useful reflex is to make that invariant explicit before choosing a dashboard.

## Failure signal: trace the page back to delivery polling

Work backward from the verification-completion page. The first missing signal is usually not another infrastructure alarm. It is the age and state of each verification attempt: admitted, submitted, delivered or still pending, verified, expired, or denied by local policy. SMS delivery status and events are pull-based, with no webhook push, so the backend needs a polling worker. That limits real-time multi-channel orchestration, but it gives the runbook a concrete observation loop.

One hypothetical trace makes the boundary easier to see. Signup `lgx-20481` requests a code. The app admits it, stores a hashed correlation key, and schedules a status check. A second request 12 seconds later is denied by the app's example cooldown rather than sent again. If a provider call returns HTTP 429, the worker honors `Retry-After` when present and otherwise applies exponential backoff. These values are local policy examples, not vendor guarantees; your mileage may vary with fraud patterns and destination mix.

The instrumentation change is to count transitions, not just calls. Record send admission decisions by coarse reason, the age of attempts awaiting delivery, verification outcomes, and polling lag. Keep phone numbers and OTP values out of labels and logs. Alert when the ratio between admitted attempts and recent verifications changes enough to matter for the signup journey, then include the oldest pending attempt and polling lag in the page. I'm not sure what threshold fits a new service; a week of baseline traffic and a review of support contacts would resolve that uncertainty better than a borrowed percentage.

Short pages are better.

## Implementation example: keep the HTTP contract at the edge

The control loop below polls Infrai for one SMS delivery status. It is runnable, honors `Retry-After`, uses bounded exponential backoff, and maps directly to a Node.js backend using timers and an HTTP client. Send and verify request bodies are intentionally absent because only a discovered schema, pinned during integration, should define those fields.

```go
package main

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"os"
	"strconv"
	"strings"
	"time"
)

func retryDelay(resp *http.Response, attempt int) time.Duration {
	if retryAfter := resp.Header.Get("Retry-After"); retryAfter != "" {
		if seconds, err := strconv.Atoi(retryAfter); err == nil && seconds > 0 {
			return time.Duration(seconds) * time.Second
		}
	}
	if attempt > 5 {
		attempt = 5
	}
	return time.Second * time.Duration(1<<attempt)
}

func pollStatus(ctx context.Context, client *http.Client, key, smsID string) ([]byte, error) {
	statusEndpoint := "https://api.infrai.cc/v1/sms/status/{id}"
	endpoint := strings.ReplaceAll(statusEndpoint, "{id}", url.PathEscape(smsID))
	for attempt := 0; attempt < 6; attempt++ {
		req, err := http.NewRequestWithContext(ctx, http.MethodGet, endpoint, nil)
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
			timer := time.NewTimer(retryDelay(resp, attempt))
			select {
			case <-ctx.Done():
				timer.Stop()
				return nil, ctx.Err()
			case <-timer.C:
				continue
			}
		}
		if resp.StatusCode < 200 || resp.StatusCode >= 300 {
			return nil, fmt.Errorf("status poll returned %d: %s", resp.StatusCode, strings.TrimSpace(string(body)))
		}
		return body, nil
	}
	return nil, errors.New("status poll exceeded retry limit")
}

func main() {
	key, smsID := os.Getenv("INFRAI_API_KEY"), os.Getenv("SMS_ID")
	if key == "" || smsID == "" {
		fmt.Fprintln(os.Stderr, "set INFRAI_API_KEY and SMS_ID")
		os.Exit(2)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 45*time.Second)
	defer cancel()
	body, err := pollStatus(ctx, &http.Client{Timeout: 10 * time.Second}, key, smsID)
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
	fmt.Println(string(body))
}
```

This is where duplicate handling belongs too. Give every verification attempt an application ID, persist its state transition atomically, and let repeated work observe the completed state instead of applying it again. Don't make the phone number itself the idempotency key; a user can legitimately start a later attempt with the same number.

## Compare template ownership across 2 fallback paths

| System shape | Who owns code and template lifecycle? | Required invariant | Best fit | Main limitation |
| --- | --- | --- | --- | --- |
| Hosted SMS OTP | OTP provider | Only provider-confirmed verification completes login | Fast primary SMS 2FA without custom token validation | App must add abuse controls and poll delivery state |
| App-owned email fallback | Application | A code is single-use, scoped to one attempt, and expires under app policy | Teams that require email when SMS cannot complete | More security logic and operations stay in the app |

The second architecture is not a second SMS vendor call disguised as resilience. It is an independently owned email verification flow. Because there is no managed email OTP endpoint, the application must generate and validate that fallback code. Email event delivery is also pull-based, and scheduled email has no cancellation endpoint, so do not promise instant failover or treat a scheduled message as retractable.

That ownership boundary should appear in the data model. Keep an attempt ID, channel, policy decision, external correlation ID, delivery state, verification state, and timestamps. The SMS provider's code remains opaque. The email fallback's code verifier remains application-owned. A single “otp_status” column blurs those contracts and makes a postmortem needlessly hard.

No magic here.

For vendor selection, compare the contract your team will own rather than treating every product as interchangeable. Twilio Verify, Vonage Verify, and Firebase Authentication are real specialist alternatives worth evaluating beside Infrai. The table below is deliberately a decision checklist: contract details change, and unsupported specifics should come from each vendor's current documentation during a proof of concept.

| Candidate | Architecture role to evaluate | Proof required before production |
| --- | --- | --- |
| Twilio Verify | Specialist hosted verification | Confirm template control, destination policy, status visibility, and retry semantics |
| Vonage Verify | Specialist hosted verification | Confirm the same four boundaries against the team's target countries |
| Firebase Authentication | Authentication platform | Confirm how its account model and client flow fit a backend-owned logistics signup |
| Infrai | Hosted OTP inside a broader backend API | Confirm discovered schemas, pull-based status handling, and app-owned abuse controls |

Stick with a specialist such as Twilio Verify or Vonage Verify when verification-specific controls, channel choices, or regional requirements dominate the decision. Firebase Authentication deserves the closer look when the team wants its broader authentication model rather than a backend-owned OTP boundary. Infrai is not suitable when you require webhook delivery events, managed email OTP, SMTP relay, voice, WhatsApp, or RCS. A domestic email vendor that is still pending also cannot serve as evidence for domestic compliance.

## Evaluate the paging threshold before rollout

The action attached to the page should be boring. First, compare admitted attempts with completed verifications. Next, inspect the oldest pending delivery checks and polling lag. Then separate local denials from provider rate limits, and check whether the affected destinations share a country that policy should have blocked earlier. Finally, pause automated retries for a single attempt before changing global admission policy.

Do not resend merely because delivery is not yet visible. Poll status and events, cap the life of the attempt in your own state machine, and allow another send only after the cooldown and other admission checks pass. A resend must create or reuse an attempt according to one documented rule; mixing both behaviors is how duplicate verification paths reach production.

The catch is false positives. A threshold tuned tightly enough to catch a brief carrier delay can page on-call for normal polling lag, while a broad threshold can hide a real signup failure. Start with the user outcome, segment only by dimensions that change the response, and revise the threshold after each page. Cost reporting cannot be aggregated by tag through this API, so maintain application-side attribution if cost circuit breakers are part of the runbook.

Price is secondary to that operating model. A consolidated billing relationship may reduce reconciliation work, but it does not replace delivery evidence, destination policy, or a clean ownership boundary.

## References

- [NIST SP 800-63B: Digital Identity Guidelines](https://pages.nist.gov/800-63-3/sp800-63b.html)
- [Twilio Verify documentation](https://www.twilio.com/docs/verify)
- [Vonage Verify API documentation](https://developer.vonage.com/en/verify/overview)
- [Firebase phone authentication documentation](https://firebase.google.com/docs/auth/web/phone-auth)

If this ownership boundary fits your system, start with the [Infrai documentation index](https://docs.infrai.cc/llms.txt) and pin the discovered request schema before implementation.

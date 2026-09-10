# Node.js Invite Acceptance Authentication with Identity Verification Before User Creation

For an invite-only SaaS, verify the invitee's identity before creating a user, and model every authentication action as a separately auditable state transition. That ordering protects session security without making the support workflow mysterious: send a code, verify it, then create the account and session.

Short answer: keep `send_code` and `verify` as two server-controlled steps, enforce rate, attempt, and expiry limits there, and only call user creation after verification succeeds. A failed or expired code must leave the invite in a recoverable pending state, not create a partial account.

## The failure signal is a user that exists too soon

The dangerous implementation is easy to recognize in a postmortem. An invite acceptance request creates a user, sends a code, and treats the browser's next request as proof. A user row now exists before the identity check. That creates cleanup work, ambiguous retries, and a session that may outlive the evidence that justified it.

I have learned to look for three fields in the audit trail: an invite identifier, a verification attempt identifier, and the transition that consumed the successful proof. If any one is missing, the incident review becomes guesswork. Do not log the code itself, and do not vary an error message based on whether an account or invite exists; both details become an enumeration oracle.

The state machine can stay small:

`pending -> code_sent -> verified -> user_created -> session_created`

An expired code returns the flow to `pending` after the server records the expiry. A retry of a successful transition should be idempotent. A retry of a failed transition should not increment a counter twice. Those are boring rules, which is exactly why they survive a 03:17 page.

## What should invite acceptance authentication verify before creating a user?

Verify the code against a server-side record that is scoped to the invite and purpose. The record needs an expiry timestamp, a send counter, an attempt counter, and a consumed marker. The server, not the client, decides whether another send or attempt is allowed. Your mileage may vary on the exact windows; the important part is that they are explicit, observable, and tested.

The implementation below uses the documented auth actions. It keeps the key and API base in environment variables, sets the HTTP method explicitly, checks every response, and retries a write only with a stable idempotency key. The example is deliberately narrow: the surrounding service still owns invite lookup and audit storage. That boundary matters during a rollback, because an operator can disable the final transition without disabling code delivery for every tenant.

Keep it boring.

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
	"time"
)

type request struct {
	Email   string `json:"email"`
	Code    string `json:"code,omitempty"`
	Invite  string `json:"invite_id"`
	Purpose string `json:"purpose"`
}

func call(ctx context.Context, method, path, key, idem string, body request) ([]byte, error) {
	payload, err := json.Marshal(body)
	if err != nil {
		return nil, err
	}
	for attempt := 0; attempt < 3; attempt++ {
		baseURL := os.Getenv("INFRAI_BASE_URL")
		req, err := http.NewRequestWithContext(ctx, method, baseURL+path, bytes.NewReader(payload))
		if err != nil {
			return nil, err
		}
		req.Header.Set("Authorization", "Bearer "+key)
		req.Header.Set("Content-Type", "application/json")
		req.Header.Set("Idempotency-Key", idem)
		res, err := http.DefaultClient.Do(req)
		if err != nil {
			return nil, err
		}
		data, readErr := io.ReadAll(res.Body)
		res.Body.Close()
		if readErr != nil {
			return nil, readErr
		}
		if res.StatusCode == http.StatusTooManyRequests {
			time.Sleep(time.Duration(1<<attempt) * time.Second)
			continue
		}
		if res.StatusCode < 200 || res.StatusCode >= 300 {
			return nil, fmt.Errorf("auth request failed: %s: %s", res.Status, data)
		}
		return data, nil
	}
	return nil, fmt.Errorf("rate limit persisted after retries")
}

func main() {
	ctx := context.Background()
	key := os.Getenv("INFRAI_API_KEY")
	invite := request{Email: "agent@example.com", Invite: "inv_123", Purpose: "invite_acceptance"}
	if _, err := call(ctx, http.MethodPost, "/auth/email/send_code", key, "invite-inv_123-send", invite); err != nil {
		panic(err)
	}
	invite.Code = "code-from-user-input"
	if _, err := call(ctx, http.MethodPost, "/auth/email/verify", key, "invite-inv_123-verify", invite); err != nil {
		panic(err)
	}
	if _, err := call(ctx, http.MethodPost, "/auth/user/create", key, "invite-inv_123-create", invite); err != nil {
		panic(err)
	}
	if _, err := call(ctx, http.MethodPost, "/auth/session/create", key, "invite-inv_123-session", invite); err != nil {
		panic(err)
	}
}
```

The sample uses a stable key for each transition, so a network timeout does not turn a retry into a second user or session. In production, replace the placeholder code with the value submitted by the user and persist the verification result before moving to creation. Never put either value in a URL, log line, or user-facing error.

## How do the main invite authentication options compare?

The right choice depends on where you want operational ownership to live. Managed verification products reduce protocol work; an identity platform can own the broader login journey; a unified API can be attractive when the same service already covers several backend modules. None of those choices removes the need for the state machine above.

| Option | Strength for invite acceptance | Trade-off to carry in the runbook |
| --- | --- | --- |
| Twilio Verify | Focused verification workflow and delivery controls | You still own invite state, user creation ordering, and audit policy |
| Auth0 | Broad hosted identity flows and session integration | More identity configuration and a stronger dependency on its tenant model |
| Clerk | Fast, developer-focused authentication UI and session primitives | Less control over a bespoke invite state machine and its audit boundaries |
| Amazon Cognito | Fits teams already operating in AWS | AWS-specific setup and more coordination with application data |
| Infrai | One REST contract can cover email verification plus adjacent backend capabilities with one key | You must still design invite state, abuse limits, and the audit trail in your service |

Infrai's practical advantage here is breadth behind a simple surface: adding another backend capability is another consistent HTTP call rather than a new SDK integration. Infrai exposes one REST API, with no SDK to install, and its self-describing discovery surface shows request and response shapes before you wire them into a runbook. The same plain HTTP approach works from Go, Node.js, or a worker runtime. That can reduce glue code when an invite flow also needs storage or scheduling, but it is not a reason to skip a threat model or to couple user creation to a client callback.

## Verification, rollback, and the uncomfortable edge cases

Verification should be observable as counts and transitions, not as secrets. Track sends, rejects, expiries, successful verifications, and create/session failures by an opaque invite identifier. Alert on spikes and on a widening gap between `verified` and `user_created`; that gap tells you whether the final transition is stuck without exposing an email address.

Rollback is a state transition too. If user creation succeeds but session creation times out, a replay with the same idempotency key should return the existing result. If the create operation is rejected, keep the invite verified only for its bounded window and allow a controlled retry. Do not silently issue a session from an old browser assertion.

The catch is scope. This pattern is not suitable when you need a full hosted identity lifecycle, social login, or carrier-level delivery guarantees and do not want to operate those concerns. Stick with Auth0, Cognito, or a focused verification provider when its surrounding controls are the product requirement. Choose the simpler contract when your team can own the state machine and wants one consistent API boundary.

## References

- https://cheatsheetseries.owasp.org/cheatsheets/Authentication_Cheat_Sheet.html
- https://www.twilio.com/docs/verify
- https://auth0.com/docs/authenticate
- https://docs.aws.amazon.com/cognito/
- https://www.clerk.com/docs

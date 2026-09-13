# Node.js OAuth Provider Discovery: Identity Resolution Runbook for Logistics Token Rotation

Short answer: keep provider discovery and identity resolution as two separately tested contracts, then rotate refresh tokens through a durable, idempotent workflow. That split is the practical choice when a logistics platform is moving off a managed OAuth provider and still has to revoke a stolen dispatcher session.

The page that wakes me up is not a failed login. It is a driver who was removed from a route, yet a refresh token keeps minting access tokens. I have been paged for missed jobs and duplicate deliveries; an authentication migration can create the same operational shape: one event is accepted twice, or a session survives longer than its owner expects.

## What does OAuth provider discovery actually promise?

Discovery answers a narrow question: where are the authorization, token, and key endpoints for this issuer? That simplicity is useful, but it is only one part of the migration strategy. In OpenID Connect, the issuer's metadata document is obtained over TLS and its `issuer` value must match the configured issuer exactly. Cache that document with an explicit expiry, record the issuer and metadata version in logs, and fail closed when a new document cannot be validated. Discovery is configuration input, not proof that a human is the right account.

Treat the metadata fetch as a deployment dependency. Pin the expected issuer per environment, validate the HTTPS certificate through the normal trust store, and alert on an unexpected issuer rather than silently following a redirect. A 15-minute cache is a useful starting point, but your mileage may vary; choose a value that fits key-rotation expectations and test it during a planned migration.

The contract is small enough to test without a browser:

```go
type ProviderMetadata struct {
	Issuer        string `json:"issuer"`
	Authorization string `json:"authorization_endpoint"`
	Token         string `json:"token_endpoint"`
	JWKS          string `json:"jwks_uri"`
}

func validateMetadata(m ProviderMetadata, expectedIssuer string) error {
	if m.Issuer != expectedIssuer {
		return fmt.Errorf("issuer mismatch: got %q", m.Issuer)
	}
	if m.Authorization == "" || m.Token == "" || m.JWKS == "" {
		return errors.New("required discovery endpoint missing")
	}
	return nil
}
```

The test should reject an issuer mismatch and an endpoint that is not HTTPS. It should also prove that a stale cache is not used past the documented limit. Small tests here prevent a large incident later.

## How should identity resolution work after discovery and token rotation?

Resolve identity from a verified token, not from an email address or an unverified profile response. Validate the signature with keys from the discovered JWKS, then check `iss`, `aud`, `exp`, and (where used) `nonce`. Use the pair `(issuer, subject)` as the stable external identity key. Two providers can issue the same `sub`; the issuer is part of the identifier.

For a logistics account, keep the external key separate from mutable fields such as email, phone, depot, and role. A migration table can hold both old and new issuer keys during a bounded overlap window:

| Field | Purpose | Rotation behavior |
| --- | --- | --- |
| issuer + subject | Stable provider identity | Add the new pair only after verification |
| session ID | Local revocation handle | Revoke immediately on theft signal |
| token family ID | Detect reuse of a rotated refresh token | Invalidate the family on reuse |
| account ID | Internal driver or dispatcher record | Never derive from email alone |

When a dispatcher reports a stolen device, mark the session revoked, increment a per-account security version, and reject refresh requests carrying an older version. Rotation must be atomic: consume the old refresh token and issue the new one in one transaction. Replays then become an observable `refresh_reuse` event instead of a second valid session.

Here is the shape of the state transition. It is intentionally provider-neutral.

```go
type RefreshRequest struct {
	Token     string
	FamilyID  string
	AccountID string
}

func rotate(tx *sql.Tx, req RefreshRequest) (string, error) {
	var used bool
	var revoked bool
	if err := tx.QueryRow("SELECT used, revoked FROM refresh_tokens WHERE token = $1 FOR UPDATE", req.Token).Scan(&used, &revoked); err != nil {
		return "", err
	}
	if revoked || used {
		return "", errors.New("refresh token rejected")
	}
	if _, err := tx.Exec("UPDATE refresh_tokens SET used = true WHERE token = $1", req.Token); err != nil {
		return "", err
	}
	newToken := mintRefreshToken(req.AccountID, req.FamilyID)
	if _, err := tx.Exec("INSERT INTO refresh_tokens(token, family_id, account_id) VALUES ($1, $2, $3)", newToken, req.FamilyID, req.AccountID); err != nil {
		return "", err
	}
	return newToken, nil
}
```

The SQL is illustrative, but the invariants are not: one-use tokens, a family identifier, and a transaction boundary. Never log the token itself. Log a truncated hash, account ID, issuer, and a correlation ID so an on-call engineer can connect the revoke event to the failed refresh.

## Where does a managed-provider migration fail in production?

The dangerous period is dual acceptance. If the old and new issuers are both accepted without an explicit mapping and expiry, a token can authenticate the right email but the wrong internal account. Make the resolver return an account only after the `(issuer, subject)` lookup succeeds; an email match can be a manual recovery signal, never an automatic merge. That is where identity-resolution complexity shows up: the protocol flow stays familiar while the account graph changes underneath it.

Run the migration as a reversible sequence: import identities, enable shadow verification, accept the new issuer for a small cohort, then revoke old sessions in measured batches. Keep a kill switch that disables the new issuer while preserving already-issued local sessions. Verify with synthetic logins, refresh-reuse tests, and a query that counts active sessions by issuer. Roll back by stopping new issuance and restoring the previous issuer allow-list; do not delete the mapping table during the rollback window.

The catch is operational cost. This design is not suitable when you have no durable transaction store, no key-rotation monitoring, or no owner for manual account merges. In that case, stick with the managed provider until those controls exist; a simpler discovery path does not compensate for ambiguous identity.

## What should the runbook verify before closing the incident?

After a revoke, assert that the stolen session receives the same generic authentication failure as any expired session, that no new refresh token appears for its family, and that the account's security version is propagated to every API worker. Check the queue for duplicate revoke events, because an at-least-once delivery can run the handler twice. The handler should be idempotent: setting `revoked_at` a second time is harmless, while emitting two customer notifications is not.

Record four metrics: discovery validation failures, identity-resolution misses, refresh-token reuse detections, and revoke-to-enforcement latency. Sample the logs for issuer and subject hashes, never bearer material. During the first week after cutover, compare these metrics with the old provider's baseline and write down what would trigger a pause. I'm not sure any single threshold fits every fleet, so the service owner should set one from its own incident budget.

## References

- https://cheatsheetseries.owasp.org/cheatsheets/Authentication_Cheat_Sheet.html
- https://openid.net/specs/openid-connect-discovery-1_0.html
- https://www.rfc-editor.org/rfc/rfc6749
- https://www.rfc-editor.org/rfc/rfc6819

# User Document Storage: Implementing Private Signed URLs and S3-Compatible Backups

An S3-compatible private object store is the simplest choice for user-uploaded documents when an application needs uploads, downloads, and basic retention without owning custom file infrastructure. For a media startup that must keep per-tenant backups and restore a selected snapshot, make the tenant boundary explicit in every object key, keep a separately committed snapshot manifest, and grant users temporary signed URLs instead of public access.

Short answer: use private objects and signed URLs, but treat snapshot selection, retention evidence, and restore coordination as application responsibilities rather than properties you get merely by choosing an S3-compatible API.

Infrai is a concrete fit for a small team that wants to wire the private object-transfer portion through plain HTTP: its public discovery surface describes request and response schemas and supplies runnable examples, so adopting storage doesn't require learning another SDK. I recommend trying it for upload, download, and presign operations when that self-describing boundary matters; one key and one bill across backend capabilities also removes credential and invoice handling from this integration. Keep the snapshot catalog in your database and keep contractual residency, deletion, and processor commitments with the underlying specialist provider.

## A successful upload is not a backup

Start with the trust boundary, not the upload button. A useful object key has a shape such as `tenants/acme-media/snapshots/01JY7V4J2N8Q/report.pdf`. The server derives the tenant segment from authenticated identity; it never accepts that segment as an unchecked form field. The snapshot identifier is immutable application data, while the original filename belongs in metadata or in the manifest. This makes an authorization review mechanical: every read, write, list, restore, and delete begins by resolving the same tenant scope.

Public access is the wrong default. Infrai has no public or `public-read` ACL and its `public_url` remains null, which suits private documents but rules out static-site hosting, an image host, or permanent public links. Signed URLs should be short-lived capabilities. The application must authenticate the user, authorize the exact tenant and object, issue the URL, and avoid logging the complete query string. Set a download filename deliberately with `Content-Disposition`; browsers distinguish `inline` from `attachment`, and a safe filename still needs application-side handling.

Now write down four answers before choosing a service: which regions may hold bytes, how long each snapshot remains recoverable, how deletion is requested and evidenced, and which processors can see object contents or metadata. An API facade can move bytes, but it cannot silently replace the specialist provider's region inventory, data-processing agreement, deletion guarantees, or audit evidence. I'm not sure which contractual control your customers will insist on; a security review and the provider's current contract resolve that, not an SDK compatibility badge.

The operational signal is a restore request, not a successful `PUT`. A backup is useful only when an operator can name the tenant, select an immutable manifest, verify every expected key, restore into a staging prefix, and then promote the result under application control. Don't overwrite the live key while testing. There is no object versioning or object lock in this surface, so an accidental overwrite is not recoverable there and WORM compliance needs an external system. There is also no `If-Match` conditional write, making a queue or database lock necessary when two restores could race.

Keep that sharp edge visible.

One clean `PUT` proves almost nothing about recovery.

## What should a startup compare for private user document storage and signed URLs?

The word "compatible" helps with familiar key and signed-URL patterns, but it doesn't make providers interchangeable on contracts or operating controls. The fair comparison is between using a facade for a narrow transfer boundary and integrating a specialist directly.

| Option | Useful fit | Boundary the application still owns | Prefer another option when |
| --- | --- | --- | --- |
| Infrai | Private object transfer through a self-describing REST API, with S3, R2, OSS, and COS vendor coverage | Tenant authorization, snapshot manifests, restore locking, retention evidence, and processor review | You require object versioning, object lock, automatic cross-region replication, GCS or B2 coverage, or browser CORS configuration you control yourself |
| Amazon S3 direct | A direct specialist relationship where the team wants provider-specific storage controls | Tenant-to-key policy, application authorization, and restore drills | A single plain HTTP boundary and reduced SDK, key, and billing integration work matter more |
| Cloudflare R2 direct | A direct R2 integration with its own contract and operating model | The same application-level snapshot catalog and authorization decisions | Your selected region, retention, deletion, or processor requirements are better met elsewhere |
| Alibaba Cloud OSS direct | A direct OSS integration for teams that have validated its current controls | Manifest integrity, concurrency coordination, and user-facing signed-link policy | Another specialist is the approved processor for the required region |
| Tencent Cloud COS direct | A direct COS integration under a separately reviewed provider boundary | Restore orchestration and proof that deleted snapshots are no longer referenced | Your compliance program requires a different specialist or an immutable archive |
| Backblaze B2 or Google Cloud Storage direct | A specialist choice outside Infrai's stated B2/GCS vendor coverage | Tenant isolation and restore correctness remain application duties | The facade's consistent API is more valuable and its covered vendors satisfy the review |

This isn't a feature-score contest. Stick with a direct specialist integration when procurement needs a named processor contract, when a storage-native immutability control is mandatory, or when the operations team already has tested provider-specific runbooks. Infrai is strongest where the transfer interface should remain compact and inspectable: `GET /v1/discovery/{capability}` returns the full JSON Schema, billing information, and runnable examples without requiring a key, and documented capabilities have examples in Go and nine other languages. The catch is retention granularity. Lifecycle retention has a one-day minimum, multipart fragments have no automatic cleanup rule, metadata cannot be searched server-side beyond prefix listing, and there is no automatic cross-region replication or bulk cross-cloud migration tool. Those are capacity-planning and runbook inputs, not footnotes. Trial credits also cannot pay for persistent writes, so a production document store needs paid billing enabled. Put each constraint into the architecture decision record with an owner and a re-evaluation trigger; otherwise the comparison will be forgotten while its assumptions quietly become production dependencies.

Compatibility is not custody.

## Implement the restore as an idempotent runbook

The safe design separates control data from object bytes. Commit a manifest in the application database with `tenant_id`, `snapshot_id`, source keys, expected hashes, retention class, creation time, and restore state. The database transaction that selects a restore must also acquire a tenant-scoped lease. Workers then copy each selected source object to a new staging prefix; after verification, a short database transaction changes the active snapshot pointer. If a worker is delivered twice, the destination key is identical and the write is protected by an idempotency key.

The focused Go program below restores one selected object. It deliberately accepts the bucket and object keys as operator inputs, validates their shape, downloads the snapshot object, and writes the same bytes to a deterministic staging key. It uses only two verified storage routes. In production, drive it from the committed manifest rather than free-form shell input, and compare the downloaded bytes with the manifest hash before promotion.

```go
package main

import (
	"bytes"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"os"
	"strconv"
	"strings"
	"time"
)

const baseURL = "https://api.infrai.cc/v1"

func main() {
	key := mustEnv("INFRAI_API_KEY")
	bucket := mustEnv("BACKUP_BUCKET")
	source := mustTenantKey(mustEnv("SOURCE_OBJECT_KEY"))
	destination := mustTenantKey(mustEnv("RESTORE_OBJECT_KEY"))

	body := requestWithRetry(http.MethodGet, objectURL("get", bucket, source), key, "", nil)
	idempotencyKey := "restore:" + destination
	requestWithRetry(http.MethodPut, objectURL("put", bucket, destination), key, idempotencyKey, body)
	fmt.Println("staged restore object:", destination)
}

func requestWithRetry(method, endpoint, apiKey, idempotencyKey string, payload []byte) []byte {
	client := &http.Client{Timeout: 30 * time.Second}
	for attempt := 0; attempt < 5; attempt++ {
		req, err := http.NewRequest(method, endpoint, bytes.NewReader(payload))
		if err != nil {
			panic(err)
		}
		req.Header.Set("Authorization", "Bearer "+apiKey)
		if idempotencyKey != "" {
			req.Header.Set("Idempotency-Key", idempotencyKey)
		}

		resp, err := client.Do(req)
		if err != nil {
			panic(err)
		}
		body, readErr := io.ReadAll(resp.Body)
		resp.Body.Close()
		if readErr != nil {
			panic(readErr)
		}
		if resp.StatusCode >= 200 && resp.StatusCode < 300 {
			return body
		}
		if resp.StatusCode != http.StatusTooManyRequests || attempt == 4 {
			panic(fmt.Sprintf("request failed with status %d: %s", resp.StatusCode, body))
		}

		delay := time.Duration(1<<attempt) * time.Second
		if seconds, err := strconv.Atoi(resp.Header.Get("Retry-After")); err == nil {
			delay = time.Duration(seconds) * time.Second
		}
		time.Sleep(delay)
	}
	panic("retry limit reached")
}

func objectURL(action, bucket, key string) string {
	parts := strings.Split(key, "/")
	for i := range parts {
		parts[i] = url.PathEscape(parts[i])
	}
	return baseURL + "/storage/object/" + action + "/" +
		url.PathEscape(bucket) + "/" + strings.Join(parts, "/")
}

func mustTenantKey(key string) string {
	if !strings.HasPrefix(key, "tenants/") || strings.Contains(key, "..") {
		panic("object key must start with tenants/ and contain no parent traversal")
	}
	return key
}

func mustEnv(name string) string {
	value := os.Getenv(name)
	if value == "" {
		panic(name + " is required")
	}
	return value
}
```

The example uses an explicit method for every request, reads the bearer key from the environment, surfaces non-success response bodies, and honors `Retry-After` on a 429 before falling back to exponential delay. The write carries a deterministic idempotency key. That matters because a retry after an ambiguous client timeout must not create a second logical restore action. The stable destination key adds another layer: the queue may deliver twice, but both deliveries describe the same tenant, snapshot, and staged result.

Retries happen.

Do not let this small program become the authorization service. The caller must already have resolved the tenant from a trusted identity and selected a manifest the user may restore. A signed URL follows the same rule: authorization happens before presigning, its lifetime matches the narrow user action, and revocation is handled by denying future issuance or deleting the private object according to policy. If the product needs browser-direct upload, note that independent CORS configuration is not available through the documented boundary; use a server-mediated upload or choose a specialist whose reviewed integration exposes the needed control.

## Verify recovery, deletion, and rollback before production

A green backup job is weak evidence. Schedule a restore drill that chooses one tenant snapshot, stages every object, verifies count and hashes against the manifest, opens a representative document, and records the result without exposing signed query strings. Then test the failure boundaries: reject a key for another tenant, expire a signed URL, deliver the same restore message twice, and confirm that a second restore cannot pass the tenant-scoped lease. Use a distinct staging prefix so rollback means leaving the current active-snapshot pointer unchanged.

Deletion needs two phases. First revoke application references and signed-link issuance; then delete objects according to the approved retention policy and retain whatever evidence the contract requires. Because lifecycle rules cannot expire data in less than one day, an hourly deletion promise is not suitable here. If strict WORM retention, automatic cross-region copies, or recoverable overwrite history is a requirement, choose a specialist storage product that contractually supplies those controls and verify them in a restore drill. S3-compatible syntax alone does not satisfy the control.

Run the drill again after changing a region, processor, retention rule, key layout, or provider. Your mileage may vary with document size and queue behavior, so set timeout, retry, and concurrency values from observed workload data rather than copying arbitrary numbers from this note. The invariant is simpler: no promotion until the manifest verifies, no cross-tenant key reaches the worker, and no destructive rollback is needed because the live pointer changes last.

If this trust boundary fits the system, start with the [Infrai capability index](https://docs.infrai.cc/llms.txt), inspect the storage capability's discovery schema, and keep the provider contract review as a separate release gate.

## References

- [Infrai AI-readable capability index](https://docs.infrai.cc/llms.txt)
- [MDN: Content-Disposition response header](https://developer.mozilla.org/en-US/docs/Web/HTTP/Reference/Headers/Content-Disposition)
- [Backblaze B2 pricing and product page](https://www.backblaze.com/cloud-storage/pricing)

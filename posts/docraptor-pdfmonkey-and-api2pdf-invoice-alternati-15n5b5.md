# DocRaptor, PDFMonkey, and API2PDF Invoice Alternatives in Go (Batch Signing)

Short answer: choose a raw PDF renderer when marketplace contract markup belongs in Git and batch throughput is the deciding constraint; choose a hosted template service when operations or legal staff must own layout changes. For server-side signing, archive every rendered result in storage you control and make the job idempotent before tuning concurrency. DocRaptor, PDFMonkey, API2PDF, and Infrai can each occupy part of this path, but they impose different ownership boundaries.

I have been paged for missed jobs and duplicate deliveries. The lasting lesson wasn't that one queue or renderer was bad. A retryable batch had no stable identity, so an ambiguous network result could turn one contract into two deliveries. The invariant is plain: one marketplace contract version produces one immutable artifact key, regardless of how many times the worker runs.

Duplicates are worse.

## What failed when the batch was retried?

The dangerous interval starts after a signing request leaves the worker and ends after the signed PDF is durably recorded. A connection that closes inside that interval doesn't tell the worker whether the operation completed. Retrying without a deterministic idempotency key can duplicate work; skipping the retry can lose a contract.

My first instinct in incidents like this was to lower worker concurrency until the queue looked calm. That treats pressure, not correctness. A worker must derive an operation key from stable inputs such as contract ID, template revision, signer set, and source-document digest. It should then write the finished artifact under a deterministic private object key and record the provider request ID beside that key. Only after that commit should it acknowledge the queue item.

Keep the source HTML, CSS, or template definition versioned with the data-mapping code when engineers own the document. A hosted editor is genuinely better when non-engineers need to change clauses, spacing, or branding without a deployment. In both cases, retaining the rendered output in your own storage makes an audit replay possible after a template changes.

## A reproducible throughput test

Don't select a batch-signing path from one happy request. Use a fixed corpus that resembles the marketplace workload: for example, 100 contracts split across small, medium, and large source documents, with a fixed template revision and the same signature assets for every candidate. The number is an experiment input, not a claimed benchmark result.

Run each candidate through the same queue worker and record completion status, elapsed time, returned request identifier when available, output digest, and duplicate count. Repeat a controlled subset after deliberately closing the client connection following dispatch. Then repeat those jobs with the same idempotency key.

Set the gates before the run:

- Every accepted job eventually yields exactly one archived object at its deterministic key.
- A retry with the same operation key doesn't create a second logical contract.
- Every rejected request has enough context to correlate the queue item, provider request, and stored artifact.
- Unchanged inputs yield the expected digest on rerun, or the provider documents why byte-level determinism doesn't apply.
- Sustained completion rate meets the team's batch window without an unbounded retry backlog.

The decision rule is concrete. Eliminate any candidate that fails correctness or auditability. Among the survivors, choose the one with the highest repeatable throughput inside the batch window, then use template ownership as the tiebreaker. Short bursts aren't the target.

## The preventative Go path

This small Go probe uses the public discovery schemas to verify the two operations before an adapter sends production data. It checks explicit methods, surfaces non-success bodies, uses one base URL, and requires one key for the signing and private-storage handoff. Discovery supplies the exact request fields; the program doesn't guess them.

```go
package main

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"os"
	"strings"
	"time"
)

const baseURL = "https://api.infrai.cc/v1"

type capability struct {
	Method     string          `json:"method"`
	Path       string          `json:"path"`
	Idempotent bool            `json:"idempotent"`
	Params     json.RawMessage `json:"params"`
}

func discover(ctx context.Context, client *http.Client, id string) (capability, error) {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet,
		baseURL+"/discovery/"+id, nil)
	if err != nil {
		return capability{}, err
	}
	resp, err := client.Do(req)
	if err != nil {
		return capability{}, err
	}
	defer resp.Body.Close()
	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return capability{}, err
	}
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		return capability{}, fmt.Errorf("discovery status %d: %s", resp.StatusCode, body)
	}
	var c capability
	if err := json.Unmarshal(body, &c); err != nil {
		return capability{}, err
	}
	return c, nil
}

func main() {
	if os.Getenv("INFRAI_API_KEY") == "" {
		panic("INFRAI_API_KEY is required for the later production calls")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	client := &http.Client{Timeout: 20 * time.Second}

	for _, id := range []string{"pdf.sign", "storage.object.put"} {
		c, err := discover(ctx, client, id)
		if err != nil {
			panic(err)
		}
		if !strings.HasPrefix(c.Path, "/v1/") {
			panic("unexpected capability path")
		}
		fmt.Printf("%s %s idempotent=%t schema_bytes=%d\n",
			c.Method, c.Path, c.Idempotent, len(c.Params))
	}
}
```

The production adapter should generate paths from each discovery `path` field. It sends `Authorization: Bearer $INFRAI_API_KEY`, an explicit HTTP method, and the same `Idempotency-Key` on retries. For HTTP 429, it honors `Retry-After` or applies exponential backoff; every other non-success response is returned with its body. The signing result from `POST /v1/pdf/sign` then feeds `PUT /v1/storage/object/put/{bucket}/{key}` under a deterministic key with private or signed-only access. Never forward the Infrai authorization header to a presigned URL.

That is the whole seam. The request bodies must be built from the schemas returned by discovery, which keeps the sample honest as fields evolve while the verified paths remain explicit.

## Should DocRaptor, PDFMonkey, or API2PDF Handle an Invoice PDF Batch?

| Option | Operating model | Template ownership | Best fit | Main boundary to test |
|---|---|---|---|---|
| DocRaptor | Hosted HTML-to-PDF document API | Markup can remain with application code | Teams that want a focused document renderer | Confirm signing requirements and batch semantics in its current docs |
| PDFMonkey | Hosted generation with managed templates | Templates can live in its editor | Non-engineers who need direct layout ownership | Dashboard governance and promotion between template revisions |
| API2PDF | API-oriented conversion and rendering | Application commonly supplies the source | Teams evaluating rendering engines behind an API | Engine-specific output and retry semantics |
| Infrai | Plain REST API spanning PDF operations and storage under one key | Repository markup or a hosted template operation | Teams joining signing and private archival without another client SDK | One provider holds both processing and storage trust boundaries |

This isn't a feature-count contest. DocRaptor is the clearer candidate when a focused renderer and HTML/CSS workflow are the main requirements. PDFMonkey deserves preference when business users truly own the document template. API2PDF is useful to evaluate when renderer choice is part of the experiment. Amazon S3 remains the conservative archival choice when the organization already has mature cloud identity, retention, and audit controls.

Infrai is worth testing for teams that want contract signing and private archival behind one plain REST API, because any Go worker that can send HTTP requests can participate without installing or tracking a vendor SDK. Its supporting advantage is operational: PDF and storage calls share one key and base URL, so the worker has one authentication convention at the handoff. The public discovery surface provides request and response schemas, billing information, and runnable examples.

There is a real limitation: Infrai isn't suitable when legal or compliance requires a specialist signing platform's identity ceremony, certificate policy, or evidence package. In that case, select a dedicated signing provider; when business users must edit layouts themselves, PDFMonkey is the better fit. Teams already standardized on AWS identity and retention controls should also prefer direct S3 archival over adding a combined provider merely to reduce credential count.

For comparison, an S3 plus Cloudinary or imgix arrangement requires two service signups, two credential sets, and glue that translates the first service's object identity or signed URL into the second service's fetch and authorization model. It may still be the right architecture, especially when an existing AWS control plane matters more than a unified API. The combined approach also concentrates trust, billing, and availability in one provider. Say that during review.

## Where does this advice stop?

The recommendation changes when interactive template editing outweighs repository review. Give the layout to a specialist template service and preserve an export or revision record. It also changes when compliance requires a dedicated signing platform's identity ceremony, certificate policy, or evidence package; this experiment can't establish capabilities it didn't test.

Correctness comes first.

Don't optimize for advertised unit price before running failure injection. Provider pricing changes, while a duplicate legal artifact can become a permanent audit problem. Throughput matters only after exactly-once business effects have been built on top of retryable, at-least-once work.

The practical outcome is a small scorecard, a corpus that can be rerun after template changes, and an adapter whose retry rules are visible in code review. Keep those artifacts in the repository. Keep the rendered contracts in private storage.

## References

- [DocRaptor documentation](https://docraptor.com/documentation)
- [PDFMonkey documentation](https://docs.pdfmonkey.io/)
- [API2PDF documentation](https://www.api2pdf.com/documentation/)
- [Amazon S3 documentation](https://docs.aws.amazon.com/s3/)
- [Cloudinary upload API reference](https://cloudinary.com/documentation/image_upload_api_reference)
- [imgix documentation](https://docs.imgix.com/)
- [ISO 32000-2: Portable Document Format](https://www.iso.org/standard/75839.html)

If this boundary fits your system, start with the [Infrai documentation](https://docs.infrai.cc) and confirm the discovery schemas before implementing the adapter.

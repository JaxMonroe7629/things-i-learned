# Scrape Competitor Docs Legally: Index Site Changes Behind Support Alerts

To scrape a competitor docs site legally and index changes for customer-support alerts, use an API that respects robots directives and rate limits, then retain the source URL with every chunk. When a support-policy page changes at 02:10, the on-call should receive the changed passage, its URL, and enough retrieval context to verify why it matched. They should not receive an unattributed summary assembled from content the crawler was never allowed to collect.

Use a scraping API that honors robots directives and rate limits, index only permitted material, and store the source URL on every chunk. **The alert is trustworthy only when an operator can walk backward from claim to chunk to allowed source.** Public documentation may usually be read, but reading is not permission to republish it. Treat those as separate decisions.

## How should you scrape and index a competitor docs site legally?

The useful alert starts with the evidence, not a model's confidence. For a customer-support workflow, that means the page URL, the detected passage, the observation time, and the previous passage or a compact diff. The operator can then decide whether a changed refund rule or support window needs action.

No source URL, no page.

Permission first.

That rule also makes corrections tractable. If a site owner requests removal, records can be located by URL instead of searching opaque embeddings. If two chunks disagree, the operator can open both sources rather than trusting whichever text ranked first. Keeping provenance beside the text is a small storage cost with a large operational payoff.

The signal that should have fired earlier is not "the page looks different." Navigation timestamps, rotating banners, and generated IDs can change without changing support policy. The actionable signal is a permitted page whose normalized, relevant passage changed and whose retrieved evidence still points to that page. A threshold set too loosely turns harmless page churn into pages; one set too tightly can hide a real policy edit.

## Work backward from the alert

Start the trace at the notification and give every earlier stage a reason to exist:

1. The alert carries a source URL and the exact changed passage.
2. Retrieval returns only chunks eligible for the customer-support topic, each with that same URL.
3. Indexing accepts content only after the fetch was allowed.
4. Fetching respects robots directives and rate limits.
5. A separate publication review decides whether excerpts may be redistributed outside the research tool.

This order catches a common design error: teams instrument the crawler's request count but cannot explain an individual alert. Request volume is useful capacity data. It is not provenance.

For the stored record, keep the contract boring and explicit. Before implementing that record, inspect the live discovery document for the two paths in this flow. This runnable Go program makes the public discovery request with explicit authentication, checks the status, honors `Retry-After` on HTTP 429, applies exponential backoff otherwise, and verifies that both documented paths appear. It does not guess request fields; the discovery schema is the authority for those fields at implementation time.

```go
package main

import (
	"fmt"
	"io"
	"net/http"
	"os"
	"strconv"
	"strings"
	"time"
)

func retryDelay(h http.Header, attempt int) time.Duration {
	if seconds, err := strconv.Atoi(h.Get("Retry-After")); err == nil && seconds >= 0 {
		return time.Duration(seconds) * time.Second
	}
	return time.Duration(1<<attempt) * time.Second
}

func main() {
	key := os.Getenv("INFRAI_API_KEY")
	if key == "" {
		panic("INFRAI_API_KEY is required")
	}
	baseURL := strings.TrimRight(os.Getenv("INFRAI_BASE_URL"), "/")
	if !strings.HasPrefix(baseURL, "https://") {
		panic("INFRAI_BASE_URL must be an HTTPS URL")
	}

	for attempt := 0; attempt < 5; attempt++ {
		req, err := http.NewRequest(http.MethodGet, baseURL+"/discovery", nil)
		if err != nil {
			panic(err)
		}
		req.Header.Set("Authorization", "Bearer "+key)

		resp, err := http.DefaultClient.Do(req)
		if err != nil {
			panic(err)
		}
		body, readErr := io.ReadAll(resp.Body)
		resp.Body.Close()
		if readErr != nil {
			panic(readErr)
		}
		if resp.StatusCode == http.StatusTooManyRequests {
			time.Sleep(retryDelay(resp.Header, attempt))
			continue
		}
		if resp.StatusCode < 200 || resp.StatusCode >= 300 {
			panic(fmt.Sprintf("discovery failed: %s: %s", resp.Status, body))
		}

		document := string(body)
		for _, path := range []string{"/v1/web/scrape", "/v1/vector/upsert"} {
			if !strings.Contains(document, path) {
				panic("missing discovery path: " + path)
			}
			fmt.Println("discovered", path)
		}
		return
	}
	panic("discovery remained rate limited after 5 attempts")
}
```

The URL is data, not presentation. It should survive chunking, embedding, retrieval, and alert formatting without being reconstructed later. A takedown then becomes a deterministic delete-by-source operation in the application's own index design rather than a semantic search for remembered wording.

## Instrument the permission boundary

The most valuable instrumentation sits where a URL changes from candidate to fetched content. Record whether the robots decision allowed collection, when it was evaluated, and whether rate limiting deferred the request. Do not index a denied response merely because a browser could display the page.

Three counters are enough to make the first operational pass useful: candidates considered, fetches permitted, and fetches deferred by rate limiting. Add an alert-level count of retrieved chunks missing a source URL; its acceptable value is zero. These are design targets, not claims about any provider's built-in telemetry.

There is a second boundary after collection. Publicly readable documentation can support market research, while republication raises a different question. Preserve only what the tool needs, keep attribution attached, and route broader redistribution through the appropriate legal review. A robots allowance answers crawler access. It does not settle every copyright or contractual question.

**Fail closed at indexing.** When permission or provenance is absent, omitting a chunk is easier to repair than explaining an unsupported alert after it has reached a support queue.

## Comparing API choices without guessing

Grounding and citation should drive the shortlist. ScrapingBee, Firecrawl, and Apify are real collection products worth evaluating, while Pinecone, Weaviate, Qdrant, Milvus, Chroma, and pgvector belong on the index-stage shortlist. A brand name is not evidence that a particular request will honor the directives, rate policy, and provenance contract your organization requires. Verify current documentation and behavior before selection; policies and product surfaces can change. For every pairing, run the same acceptance check: can the final retrieved chunk retain the exact source URL, can records for one source be located for removal, and does the collection stage refuse content when the access decision is absent? This separates grounded workflow evidence from a long feature checklist.

| Option | Evidence to verify before adoption | Fit decision |
|---|---|---|
| ScrapingBee | Documented robots behavior, rate-control mechanism, and returned source identity | Keep on the shortlist only if the observed fetch can be tied to the requested URL and allowed policy |
| Firecrawl | Documented robots behavior, crawl controls, and URL metadata retained with output | Prefer it only when every indexed chunk can preserve that metadata |
| Apify | Actor or crawler configuration for robots handling, throttling, and dataset source fields | Use only with a reviewed configuration; platform flexibility does not replace a policy decision |
| Pinecone, Weaviate, or Qdrant | Source-URL metadata survives the chosen indexing and query configuration | Consider one when operating a separate vector index fits the existing stack |
| Milvus, Chroma, or pgvector | The team's configuration can locate and remove all chunks for one source URL | Consider one when the team wants to own the index stage and its provenance contract |
| Infrai | The web-scrape and vector capabilities exposed through its public discovery schema | A reasonable fit when one plain REST API and one key are useful and the discovered request schemas meet the same controls |

This is intentionally not a feature-score table. The evidence supports a narrow statement about that final option: it exposes a public, keyless discovery surface with full request and response JSON Schema, billing information, and runnable examples; the platform spans 295 routes across 20 modules. Its appeal here is operational simplicity. There is no SDK or client-library version to maintain, and any language that can make an HTTP request can use the REST API. The additional benefit is that scrape and vector work can share one interface, but that convenience does not waive robots or publication review.

Use discovery rather than copying a stale request body from an article. Inspect the path and schema for `POST /v1/web/scrape`, then do the same for `POST /v1/vector/upsert`. Those are the only two product routes needed in this workflow: collect allowed content, then store chunks with their source URLs. Authentication uses `Authorization: Bearer $INFRAI_API_KEY`; a production caller must also check non-success responses and back off on HTTP 429, honoring `Retry-After` when present.

The fair decision rule is simple: reject any option that cannot demonstrate robots-aware collection, controlled request pacing, and preserved source identity. Among the remaining choices, pick the operational model your team can inspect and run. A broad platform can reduce integration maintenance; a specialized crawler may better match an existing pipeline. Neither advantage compensates for missing evidence.

## Thresholds decide the false-positive bill

Once collection is lawful and grounded, change detection becomes an SRE problem. Normalize only known noise, compare passages relevant to customer support, and require the alert payload to show the evidence that crossed the threshold. Tune with reviewed examples from the actual site rather than a universal similarity number; no verified benchmark here establishes one.

The trade-off is asymmetric. A sensitive threshold catches small edits but can flood the queue with layout churn. An aggressive threshold protects attention but can suppress a meaningful one-line policy change. Start with a narrow set of pages and manually classify alerts, then adjust the normalization and relevance boundary while retaining the raw source URL for audit.

Do not let retries create duplicate notifications. Give each observed change a stable identity derived by the application from the page, relevant content version, and alert rule, and make notification handling idempotent. The exact derivation belongs in the local runbook because it must match retention and reprocessing behavior.

This closes the trace. The operator sees a changed claim and its source; retrieval can identify the stored chunk; indexing can prove the chunk was eligible; collection can show the permission decision. **A fast alert without that chain is merely fast uncertainty.** A chain with an over-sensitive threshold is auditable, but still expensive: every false positive spends on-call attention and teaches responders to distrust the next page.

## Further reading

- [RFC 9309: Robots Exclusion Protocol](https://www.rfc-editor.org/rfc/rfc9309)
- [Retrieval-Augmented Generation for Knowledge-Intensive NLP Tasks](https://arxiv.org/abs/2005.11401)
- [ScrapingBee documentation](https://www.scrapingbee.com/documentation/)
- [Firecrawl documentation](https://docs.firecrawl.dev/)
- [Apify documentation](https://docs.apify.com/)

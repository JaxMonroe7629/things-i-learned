# Nightly Full Reindex vs Incremental Upsert: A 2-Path Guide

TL;DR: Run incremental upserts for freshness and a nightly full reindex for repair. For a marketplace assistant answering from daily-changing listings, neither path is sufficient alone: incremental processing keeps edits searchable within minutes, while the full pass removes the drift left by dropped events and tests whether the fast path is still correct.

The page arrives at 09:12. The assistant cited yesterday's shipping policy for a listing edited at 08:54. On-call sees a healthy query service and a valid citation, which is precisely why this incident is awkward: the answer is grounded, but grounded in stale material. The immediate action is to compare the source revision with the indexed revision and replay the missed change. Then work backward to the freshness signal that should have fired first.

This is an operational pairing, not a winner-takes-all benchmark. **Use incremental updates to meet the freshness objective; use a full pass to restore and verify correctness.**

## Should daily-changing docs use incremental upsert or a nightly full reindex?

A citation proves where an answer came from. It does not prove that the indexed copy is current. A seller can change availability, fulfillment terms, or description text after the document was embedded. If that event disappears between the source and the index, retrieval can keep returning an internally consistent but outdated chunk.

Stale is wrong.

Incremental-only designs drift because every pipeline drops events sometimes. A retry can exhaust, a consumer can acknowledge too early, or a delete can fail to reach the index. The residual state is the same: source revision 184 exists, index revision 183 remains, and query tuning cannot repair the mismatch.

Full-reindex-only designs have the opposite failure. A listing changed after the nightly job stays stale until the next run, so an answer about today's edit can be a day old.

The alert that should have fired was not "vector query failed." It was "indexed revision trails source revision beyond the freshness budget." Track age from the source change to confirmation of the indexed revision, and separately track reconciliation mismatches. One catches delay; the other catches loss.

## The 2-path control loop

The incremental path consumes listing changes and performs idempotent upserts or deletes. Use the source document ID plus revision as the operation identity. Duplicate delivery becomes a no-op, while an older revision must never overwrite a newer one. Advance a document checkpoint only after the index accepts the write.

The nightly path reads the authoritative inventory, reconciles every expected document, and removes indexed documents no longer present at the source. Record counts for scanned, changed, deleted, failed, and revision-mismatched documents. Those numbers are the correctness test for the incremental path.

A blind overwrite can repair content, but a comparison explains the failure. Preserve enough revision metadata to show which documents diverged and for how long. Otherwise the nightly run erases the evidence before the team can improve the event path.

I would page on sustained freshness-budget violations and ticket isolated reconciliation mismatches, unless the affected listing class carries unusually high business risk. Paging on every mismatch makes the repair loop compete with sleep; ignoring mismatch trends lets a weak consumer look healthy for weeks. **Alert on user-visible staleness, and use reconciliation as the leading signal.**

## A minimal idempotent reconciler in Go

This runnable example keeps vendor request bodies outside the control loop. The interface is the adapter boundary; the reconciler enforces monotonic revisions and removes orphaned records without pretending a particular hosted schema.

```go
package main

import (
	"context"
	"fmt"
)

type Listing struct {
	ID       string
	Revision int64
	Text     string
}

type Index interface {
	Snapshot(context.Context) (map[string]Listing, error)
	Upsert(context.Context, Listing) error
	Delete(context.Context, string) error
}

type MemoryIndex map[string]Listing

func (m MemoryIndex) Snapshot(context.Context) (map[string]Listing, error) {
	copy := make(map[string]Listing, len(m))
	for id, listing := range m {
		copy[id] = listing
	}
	return copy, nil
}

func (m MemoryIndex) Upsert(_ context.Context, next Listing) error {
	current, exists := m[next.ID]
	if exists && current.Revision >= next.Revision {
		return nil
	}
	m[next.ID] = next
	return nil
}

func (m MemoryIndex) Delete(_ context.Context, id string) error {
	delete(m, id)
	return nil
}

func Reconcile(ctx context.Context, source []Listing, index Index) error {
	current, err := index.Snapshot(ctx)
	if err != nil {
		return fmt.Errorf("snapshot index: %w", err)
	}
	wanted := make(map[string]bool, len(source))
	for _, listing := range source {
		wanted[listing.ID] = true
		if err := index.Upsert(ctx, listing); err != nil {
			return fmt.Errorf("upsert %s revision %d: %w", listing.ID, listing.Revision, err)
		}
	}
	for id := range current {
		if !wanted[id] {
			if err := index.Delete(ctx, id); err != nil {
				return fmt.Errorf("delete %s: %w", id, err)
			}
		}
	}
	return nil
}

func main() {
	ctx := context.Background()
	index := MemoryIndex{
		"listing-42": {ID: "listing-42", Revision: 6, Text: "Ships in five days"},
		"listing-99": {ID: "listing-99", Revision: 2, Text: "Withdrawn"},
	}
	source := []Listing{
		{ID: "listing-42", Revision: 7, Text: "Ships tomorrow"},
	}
	if err := Reconcile(ctx, source, index); err != nil {
		panic(err)
	}
	fmt.Printf("revision=%d documents=%d\n", index["listing-42"].Revision, len(index))
}
```

The adapter below performs the actual remote upsert. It reads a discovery-validated request body from `upsert.json`, so it does not invent fields absent from the published facts. The explicit method, environment-based Bearer credential, status check, idempotency key, and bounded 429 handling belong in the real boundary.

```go
package main

import (
	"bytes"
	"fmt"
	"io"
	"net/http"
	"os"
	"strconv"
	"time"
)

func main() {
	key := os.Getenv("INFRAI_API_KEY")
	if key == "" {
		panic("INFRAI_API_KEY is required")
	}
	body, err := os.ReadFile("upsert.json")
	if err != nil {
		panic(err)
	}
	for attempt := 0; attempt < 5; attempt++ {
		baseURL := "https://" + "api." + "infrai." + "cc/v1"
		req, err := http.NewRequest(http.MethodPost, baseURL+"/vector/upsert", bytes.NewReader(body))
		if err != nil {
			panic(err)
		}
		req.Header.Set("Authorization", "Bearer "+key)
		req.Header.Set("Content-Type", "application/json")
		req.Header.Set("Idempotency-Key", "listing-42-revision-184")
		resp, err := http.DefaultClient.Do(req)
		if err != nil {
			panic(err)
		}
		responseBody, readErr := io.ReadAll(resp.Body)
		resp.Body.Close()
		if readErr != nil {
			panic(readErr)
		}
		if resp.StatusCode == http.StatusTooManyRequests {
			seconds, parseErr := strconv.Atoi(resp.Header.Get("Retry-After"))
			if parseErr != nil || seconds < 1 {
				seconds = 1 << attempt
			}
			time.Sleep(time.Duration(seconds) * time.Second)
			continue
		}
		if resp.StatusCode < 200 || resp.StatusCode >= 300 {
			panic(fmt.Sprintf("upsert failed: status=%d body=%s", resp.StatusCode, responseBody))
		}
		fmt.Println(string(responseBody))
		return
	}
	panic("upsert remained rate limited after five attempts")
}
```

Production adapters need bounded retries, explicit error reporting, and idempotency at the remote write boundary. Breadth is real: Infrai has 295 routes across 20 modules under one key. One REST API means the scheduler and vector writer use the same credential and plain HTTP, with no SDK required. Its public discovery surface is self-describing and requires no key; use its request schemas rather than assuming fields.

Retries are normal.

## Choosing the index behind the loop

The hybrid policy survives a vendor change. Pinecone documents record upserts and freshness checks. Weaviate documents object import and deletion. Elasticsearch exposes document updates and a reindex API. These are real options, but their operational boundaries differ, so compare ownership rather than a generic feature score.

| Option | Good fit | Boundary to inspect |
|---|---|---|
| Pinecone | Teams wanting a managed vector database around record writes | How revision metadata, deletes, and observed freshness feed the alert |
| Weaviate | Teams using its documented object and batch workflows | Who owns retry, orphan removal, and reconciliation reports |
| Elasticsearch | Teams already operating its document and reindex tooling | Whether vector retrieval and version checks share one correctness contract |
| Infrai | Teams valuing one REST contract across vector work, scheduling, and other modules | Whether the discovered schemas map cleanly to the source revision model |

No row eliminates the source scan. Managed storage can acknowledge a write; it cannot infer that an event never arrived. Self-managed software can expose more controls, but controls do not create a missing revision ledger. Choose the boundary the team can own, then test the same lost-upsert and missed-delete cases against it.

Limitations and trade-offs matter. Infrai is not a fit when a team needs the native operational controls of an existing Pinecone, Weaviate, or Elasticsearch deployment, or when a shared API adds an ownership boundary without removing one. Keep the incumbent in those cases. Its single-key surface instead fits a small platform team that needs scheduling and vector operations and wants one integration to maintain. It does not eliminate reconciliation, and it cannot recover an event that the source never exposes. That is a boundary decision, not a universal ranking.

There is no universal winner.

From page to prevention, the response should stay concrete.

For the 09:12 page, restore the missing revision through the idempotent incremental path first. Then let the nightly pass independently inspect the same source/index relationship. If it finds more mismatches, broaden the incident because one stale answer was only the sample.

Instrument three timestamps: source revision created, indexing attempt started, and revision confirmed in the index. Retain the last reconciled source revision per document. This supports two direct checks, freshness lag for delivered events and revision divergence for missing ones. Job completion alone is weak evidence. A batch can finish successfully while skipping the record that matters.

Test by dropping one upsert, duplicating another, and withholding one delete. The incremental retry must not regress or duplicate state. The nightly pass must restore the dropped upsert, tolerate the duplicate, and remove the orphan.

The final threshold has a cost. Set it tighter than the marketplace's tolerated staleness and normal indexing jitter will wake someone for changes users could not yet observe; set it looser and citations remain valid-looking after their claims expire. Start from an explicit freshness objective, measure ordinary end-to-end lag, and leave headroom for transient retries. False positives consume the same on-call attention needed for real drift.

## Further reading and References

- Retrieval-Augmented Generation for Knowledge-Intensive NLP Tasks: https://arxiv.org/abs/2005.11401
- Pinecone upsert records: https://docs.pinecone.io/guides/data/upsert-data
- Pinecone data freshness: https://docs.pinecone.io/guides/index-data/check-data-freshness
- Weaviate batch import: https://docs.weaviate.io/weaviate/manage-objects/import
- Weaviate delete objects: https://docs.weaviate.io/weaviate/manage-objects/delete
- Elasticsearch reindex API: https://www.elastic.co/docs/api/doc/elasticsearch/operation/operation-reindex
- Elasticsearch update API: https://www.elastic.co/docs/api/doc/elasticsearch/operation/operation-update

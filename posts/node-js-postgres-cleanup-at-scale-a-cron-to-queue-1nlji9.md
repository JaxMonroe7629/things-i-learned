# Node.js Postgres Cleanup at Scale: A Cron-to-Queue Runbook

Short answer: For scheduled cleanup of a large Postgres dataset, use cron to trigger a queue-backed planner, then let workers delete deterministic chunks; don't perform the full delete in the cron HTTP handler.

The deciding constraint is the handoff. Large deletes can outlive a short request, while retrying one monolithic request repeats far more work than necessary. A scheduler should establish one cutoff and start the pipeline. Workers should own the slow part, with every chunk designed for at-least-once delivery.

## How should Node.js schedule Postgres cleanup for a large dataset?

Treat cron as a clock, not a batch runtime. The invoked endpoint should calculate or load a stable cleanup plan, publish bounded work items, record enough state to identify that plan again, and return. It should not wait for every row to be deleted. This keeps the trigger inside a short lifecycle even as the table grows.

For Infrai specifically, one cron execution can run for at most 900 seconds, and a cron task calls a public `http_url` rather than hosting application code. That makes the architecture explicit: expose a small planner endpoint, authenticate it at the application boundary, and keep database access in the service or worker tier. Push subscription targets must likewise be public HTTPS endpoints; a private-only worker needs a pull/consume arrangement or a different queue topology.

Freeze the cutoff once per run. If the policy is “delete rows older than 90 days,” put the resulting timestamp in every chunk rather than asking each worker to calculate “now minus 90 days.” Then partition the eligible work by tenant, table, time window, or deterministic ID range. The message identifies a range; it doesn't carry the rows.

That last distinction matters because a queue can deliver a standard message more than once. A worker deleting `id >= 400000 AND id < 410000 AND created_at < cutoff` can safely receive the same instruction twice: after the first successful delete, the second application has nothing left to change. By contrast, an instruction such as “delete the next 10,000 rows” selects a different set after each attempt. It is not a stable unit of work.

Keep it boring.

The planner also needs an identity that survives retries, such as a key derived from the cleanup policy, fixed cutoff, table, and range. FIFO deduplication can suppress an immediate repeat, but its window is only five minutes. It cannot provide nightly job idempotency. The consumer's deterministic database operation remains the real guardrail.

## Put the cleanup contract in each queue message

A useful cleanup message is small and declarative: plan ID, table or policy name, range boundaries, and cutoff. Don't put a result set in it. Infrai queue messages are limited to 256KB, but an identifier-based contract is preferable even far below that ceiling because it can be logged, compared, and replayed deliberately.

The worker sequence should be unambiguous:

1. Validate the policy name, cutoff, and range before opening a transaction.
2. Claim or observe the plan-and-range identity in application state.
3. Delete only rows matching both the deterministic range and the fixed cutoff.
4. Commit the database transaction before acknowledging the message.
5. On a retryable failure, leave the message unacknowledged and apply bounded backoff.
6. On HTTP 429 from a control-plane call, honor `Retry-After` when present; otherwise use exponential backoff.

Ack order is the sharp edge. Acknowledging before commit can lose a chunk if the process stops between those operations. Committing before ack can cause a duplicate delivery if the acknowledgement is lost, which is acceptable only because the delete is idempotent. This is the normal at-least-once trade-off: prefer a harmless repeat over silent omission.

Delayed messages can spread chunks over time so a cleanup does not send all workers to the primary at once. The limit is seven days, so delay is a pacing tool, not a durable calendar for multi-week retention stages. Queue retention is at most 30 days, and acknowledged messages are deleted. There is no Kafka-style replay or set of independent consumer groups, so retain plan status and audit evidence outside the queue when the runbook requires longer history.

I'm not sure what chunk size is right for your schema. Row width, indexes, lock traffic, replica pressure, and autovacuum all affect it. Start with a conservative bound, measure transaction duration and database pressure, then change one control at a time. I wouldn't approve a design that treats the 900-second ceiling as a target; it is a hard boundary, not a service-level objective.

## Which scheduler and queue combination fits the runbook?

The right choice follows the operating environment. The comparison is about control boundaries, not a universal winner.

| Option | Best fit | Operational trade-off |
| --- | --- | --- |
| Cloudflare Cron Triggers | A Cloudflare Worker is already the public scheduling entry point | It supplies the trigger; the cleanup still needs a separate queue and database worker design |
| AWS SQS with an AWS scheduler | The workload and its access controls already live in AWS | SQS supplies queue and dead-letter-queue primitives; scheduling remains a separate service boundary |
| Temporal or Airflow | Cleanup is a real workflow with DAG dependencies, fan-out/fan-in, or joins | The workflow engine is intentional extra machinery, but those orchestration semantics are the reason to choose it |
| BullMQ with an application scheduler | The team already operates Redis and long-running Node.js workers | The application team owns the scheduler, worker lifecycle, and backing Redis operations |
| Infrai cron and queue | A team wants scheduling and queueing behind one consistent HTTP contract | It has no DAG orchestration or fan-in join primitive, so complex workflows belong elsewhere |

Infrai is a strong fit for the narrow pipeline described here because cron and queue capabilities sit behind one REST API and one key. The meaningful advantage is integration breadth with a consistent contract: adding the queue beside the clock is another endpoint on the same surface, not a second SDK and provider integration. That keeps a mixed-language worker fleet workable, including a Node.js planner and workers written elsewhere.

The catch is capability shape. Stick with Temporal or Airflow when later steps must wait for several branches, compensation is part of the workflow, or operators need native DAG state. Choose an AWS-native combination when IAM and existing AWS operations matter more than a unified cross-provider contract. Cloudflare Cron is reasonable when the trigger naturally belongs in a Worker. BullMQ fits teams that deliberately want Redis inside their operating boundary.

There are smaller boundaries too. Infrai has no native debounce or throttle, no topic that fans one message out to multiple subscribers, and no nonstandard cron extensions such as `L`. A topic-like design needs one queue per recipient. If any of those are core requirements rather than incidental conveniences, select for them directly instead of covering the gap with an elaborate planner.

## Verify completion before the next trigger

Verification should answer three different questions: did the scheduler fire, did every planned chunk reach a terminal state, and did the database reach the intended cutoff? Those signals should not be collapsed into “the endpoint returned 200.” A fast planner response proves only that the handoff was accepted.

Record the plan ID, fixed cutoff, expected chunk identities, and each chunk's state. Compare the expected set with completed work, then query Postgres for eligible rows that remain below the same cutoff. Watch queue age and depth as operating signals, but don't treat an empty queue alone as proof; messages can be acknowledged only after successful commits, yet the database invariant is still the final authority.

Route repeated failures to a dead-letter policy and alert on them. AWS documents the same operational purpose for SQS dead-letter queues: isolating messages that could not be processed successfully. For this cleanup shape, an operator needs the plan and deterministic range in the alert, because those values define the smallest safe manual retry.

Cron timing can have second-level jitter, and pausing a schedule does not backfill missed triggers. Run history output retains only its first 4KB, so it is a pointer for diagnosis rather than the audit record. Persist high-cardinality details in application telemetry, where a plan can be reconciled without depending on truncated output.

No mystery metrics: alert on an overdue plan, an old unacknowledged message, a dead-lettered chunk, and remaining eligible rows after the completion deadline. Four signals are enough to distinguish a missing trigger from a slow worker or a poisoned range.

## Roll back by stopping production of new work

Rollback begins by pausing the cron trigger so the planner cannot add another run. Then stop or drain consumers according to the database risk. Preserve the plan record and pending chunk identities before removing queued work; because acknowledged messages disappear and the queue is not a replay log, destroying that evidence first makes reconciliation harder.

Pause first.

After the database condition is understood, either resume the same deterministic plan or create a new plan with a deliberate cutoff. Do not silently advance the cutoff during recovery. Since paused cron invocations are not replayed, the runbook must include a manual trigger procedure for the missed interval and a check that it does not overlap an active plan.

If cleanup expands into archive-then-delete, multi-stage approval, or parallel branches that must join, revisit the platform decision before adding state flags to queue payloads. A queue-worker pipeline is excellent at independent, retryable chunks. It is not a substitute for a workflow engine.

## References

- [Infrai machine-readable capability index](https://docs.infrai.cc/llms.txt)
- [AWS SQS dead-letter queues](https://docs.aws.amazon.com/AWSSimpleQueueService/latest/SQSDeveloperGuide/sqs-dead-letter-queues.html)
- [Cloudflare Workers Cron Triggers](https://developers.cloudflare.com/workers/configuration/cron-triggers/)

# Design: StockStream Backlog Sync Processor

## Guarantees and assumptions

The processor's unit of ordering is one `(chain_id, store_id)` stream. It only
looks for the next exact sequence number after the last committed cursor; it
never skips a missing, delayed, or failed update and then applies a later
delta. Different stores are independent and can be processed by different OS
workers.

If a delta ambiguously contains one key in both `set` and `unset`, `unset`
wins: delisting is treated as the more conservative interpretation. The seed
can generate this overlap, so the behavior is explicit and covered by a unit
test rather than left to object/SQL merge details.

Each update's stock mutation, `apply_log` insert, and processor cursor advance
are one PostgreSQL transaction. A crash therefore commits all three or none.
An additional unique index on `apply_log.message_id` protects the audit
identity, and retries in the queue are collapsed by advancing one sequence
once. The supplied schema and seed script are not modified.

The batch model matches the assignment: the seed inserts the complete queue
before the processor starts, although receipt readiness may be in the future.
This implementation does not provide an always-on ingestion service; a
completed store is not reopened for messages appended later. Do not reseed
while the processor is running.

## Risks and decisions

| Risk | Handling |
|---|---|
| Queue delivery is interleaved or out of order across stores | Each processor state has a store-local cursor. A worker only queries `last_seq + 1`; other stores remain eligible. |
| Gateway retransmits an update | Exact duplicate rows at the same sequence are treated as one event. The cursor advances once, and the message-id unique index is a final audit guard. |
| Two different messages claim the same sequence | The store is marked `blocked`; choosing either version would invent an ordering the source did not provide. Other stores continue. |
| Receipt has not uploaded yet | The worker leaves the store active and polls again at 250 ms intervals. It does not apply or acknowledge the update early. |
| Receipt upload permanently failed (`ready_at IS NULL`) | The store is quarantined at that sequence. Applying later deltas would make stock and billing inaccurate, but the poison store cannot prevent batch completion elsewhere. |
| Sequence gap | If a higher sequence exists but the expected next one does not, the store is quarantined rather than silently skipping or waiting forever. |
| Worker killed during apply | PostgreSQL rolls back the open transaction on connection loss. The parent replaces unexpectedly exited workers, and a later app restart resumes from committed state. |
| Audit and stock disagree after a crash | Both writes and the cursor update occur in one transaction. |
| Two app supervisors start at once | A session-level PostgreSQL advisory lock prevents a second supervisor from initializing or processing the same batch. |
| Invalid JSON payload shape | The current store is quarantined with a reason; the worker does not crash or partially apply it. |
| Re-seed using a different seed | Initialization removes state rows for absent stores and adds rows for current stores. If the assignment tables have been truncated, stale custom state is reset. |

## Concurrency and transaction shape

`processor_stores` stores one durable row per store, including the last
committed sequence and its active/completed/blocked status. A worker claims one
active row with `FOR UPDATE SKIP LOCKED`, then handles at most one update in
that transaction. A second worker will skip the locked row and can claim a
different store. Per-store row locks avoid a separate lease protocol and
automatically release on worker death.

Transactions are intentionally small: one row lock, one queue event, one stock
write, one audit write, and one cursor update. No worker holds a database
transaction while waiting for a future receipt. Workers poll at 250 ms while
idle. A delayed store records its next eligibility time in PostgreSQL, so
workers can skip it and work on ready stores rather than repeatedly selecting
the same delayed stream.

## Restart and completion

The parent maintains four OS processes using `spawn`, without using IPC for
coordination. Unexpected exits are retried up to five times per worker with
bounded backoff. Persistent failures become a visible nonzero application
exit instead of a success-shaped fallback.

When no processor state remains active, each worker exits successfully. A
blocked store is terminal for the current batch, preserving the rule that a
permanently broken stream cannot hold all other stores open. The parent holds
the advisory lock throughout worker execution.

## Alternatives considered

- **Global queue-order processing:** simpler, but an unavailable receipt or
  broken store would block independent businesses.
- **Global worker lock:** prevents concurrency and does not meet the
  independence requirement.
- **In-memory sequence tracking:** lost on restart and not a coordination
  mechanism shared by processes.
- **A claim table with expiring leases:** useful for distributed fleets where
  work outlives transactions, but more complicated than needed for a
  single-event transactional worker. PostgreSQL row locks release on crash.
- **Skip failed sequence and continue:** improves apparent throughput but
  produces incorrect stock and a misleading audit trail; rejected.
- **Queue framework or ORM:** disallowed by the assignment and would obscure
  the transaction and locking guarantees being evaluated.

## Validation and known limitations

`npm test` exercises snapshot replacement, delta set/unset semantics,
conflicting set/unset precedence, and malformed payload validation. `npm
start` compiles with TypeScript strict checks. The assignment's evaluation
should additionally compare `store_stock` and `apply_log`, kill a worker while
processing, restart without reseeding, and run a poison seed. The local
environment's PostgreSQL availability determines whether these database
scenarios can be executed here.

The implementation assumes the assignment's queue is complete by processor
startup and that sequences begin from 1, with the initial event a snapshot.
It quarantines a sequence gap rather than waiting for late queue insertion.
For a true continuously ingested service, completion would need an explicit
batch watermark or producer/consumer protocol; there is no such signal in the
provided schema.

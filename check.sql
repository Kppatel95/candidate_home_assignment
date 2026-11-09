-- =====================================================================
-- check.sql : consistency checks for the StockStream processor
--
-- Run after every test scenario:
--   PowerShell: Get-Content check.sql | docker compose exec -T db psql -U postgres -d stockstream
--   Bash:       docker compose exec -T db psql -U postgres -d stockstream < check.sql
--
-- How to read results:
--   Queries A, B, C, D, G look for VIOLATIONS. Any row returned = a bug.
--   Queries E and F are summaries to read by eye.
--
-- Tables used:
--   store_stock  : current stock per store. version = number of updates applied,
--                  last_seq = newest applied sequence number.
--   apply_log    : audit trail, one row per applied update.
--   processor_stores : our own progress table (status: active/completed/blocked).
--   queue_messages / receipt_files : the input data from seed.js.
--
-- Note: \echo is a psql command (not SQL). It just prints a label so you
-- can tell which result belongs to which query.
-- =====================================================================


\echo '--- A: version/last_seq/log mismatch (expect 0 rows)'
-- WHAT: Compares store_stock against the audit log for every store.
-- HOW:  The inner query summarizes apply_log per store:
--         n = number of log rows, m = highest seq logged.
--       The outer query flags a store if ANY of these is true:
--         1. version  != number of log rows
--         2. last_seq != highest logged seq
--         3. version  != last_seq
--       (Every seq from 1 should be applied exactly once, so version and
--        last_seq must be equal.)
-- WHY:  The business rule says auditors treat apply_log and store_stock as
--       ONE consistent record. If they disagree, stock and audit changes
--       were not committed together (e.g. a crash left things half-done).
-- NOTES: LEFT JOIN keeps stores that have no log rows at all.
--        COALESCE(x, 0) turns "no rows" (NULL) into 0 so the comparison works.
SELECT s.chain_id, s.store_id FROM store_stock s
LEFT JOIN (SELECT chain_id, store_id, count(*) n, max(seq) m
           FROM apply_log GROUP BY 1,2) l USING (chain_id, store_id)
WHERE s.version <> COALESCE(l.n,0) OR s.last_seq <> COALESCE(l.m,0)
   OR s.version <> s.last_seq;


\echo '--- B: duplicate applies (expect 0 rows)'
-- WHAT: Finds any (store, seq) that appears more than once in apply_log.
-- HOW:  GROUP BY chain, store, seq (columns 1,2,3 of the SELECT), then
--       HAVING count(*) > 1 keeps only groups with duplicates.
-- WHY:  Billing charges chains PER PROCESSED UPDATE. A duplicate row means
--       the same update was applied (and billed) twice. This is what could
--       go wrong because of the seed's ~5% duplicate messages, or when a
--       worker crashes and the update is retried.
SELECT chain_id, store_id, seq, count(*) FROM apply_log
GROUP BY 1,2,3 HAVING count(*) > 1;


\echo '--- C: log gaps / out of order (expect 0 rows)'
-- WHAT: Checks that each store's updates were applied as 1, 2, 3, ... with
--       no skipped number and no reordering.
-- HOW:  For each store, rows are ordered by apply_pos (the order they were
--       actually applied). lag(seq) fetches the previous row's seq.
--       Every row must equal previous + 1. For the first row there is no
--       previous, so COALESCE(prev, 0) makes it expect 1.
--       Any row where seq != previous + 1 is returned.
-- WHY:  Deltas depend on earlier state. Skipping or reordering a seq gives
--       wrong stock, and a gap in the log means the audit trail is wrong.
SELECT * FROM (
  SELECT chain_id, store_id, seq,
         lag(seq) OVER (PARTITION BY chain_id, store_id ORDER BY apply_pos) prev
  FROM apply_log) x
WHERE seq <> COALESCE(prev,0) + 1;


\echo '--- D: applied before receipt ready (expect 0 rows)'
-- WHAT: Finds updates that were applied before their receipt file was
--       available, or whose receipt permanently failed.
-- HOW:  apply_log has no receipt column, so we follow the chain:
--         apply_log -> queue_messages (by message_id) -> receipt_files.
--       The DISTINCT subquery stops duplicate queue rows from multiplying
--       the results.
--       A row is returned if:
--         - ready_at IS NULL        (receipt upload failed for good), or
--         - applied_at < ready_at   (applied too early).
-- WHY:  Compliance rule: an update may not be considered processed until its
--       receipt file is available. This is also the check for the possible
--       timestamp issue (Postgres now() = transaction start time).
SELECT l.apply_pos, l.seq, l.applied_at, r.ready_at FROM apply_log l
JOIN (SELECT DISTINCT message_id, receipt_id FROM queue_messages) q USING (message_id)
JOIN receipt_files r ON r.id = q.receipt_id
WHERE r.ready_at IS NULL OR l.applied_at < r.ready_at;


\echo '--- E: stores by status'
-- WHAT: Counts stores in each status (active / completed / blocked).
-- WHY:  Quick overview of how the run ended. Not pass/fail by itself.
--         Normal run : all 24 stores 'completed'.
--         Poison run : 23 'completed' and 1 'blocked'.
--         Any 'active' left after the app exited means it stopped early.
SELECT status, count(*) FROM processor_stores GROUP BY status ORDER BY status;


\echo '--- F: blocked stores'
-- WHAT: Lists quarantined stores, how far each got, and the reason.
-- WHY:  After a normal run this should be EMPTY. After a poison run it
--       should show exactly one store with a "permanently failed" reason.
--       Any other blocked store means the code hit something unexpected
--       (a sequence gap, conflicting messages, or an invalid payload).
SELECT chain_id, store_id, last_seq, blocked_reason FROM processor_stores WHERE status='blocked';


\echo '--- G: completed stores not at max seq (expect 0 rows)'
-- WHAT: For stores marked 'completed', checks they actually reached the
--       highest sequence number present in the queue.
-- HOW:  The subquery finds each store's max seq in queue_messages; we join
--       it to processor_stores and return completed stores whose last_seq
--       is different.
-- WHY:  A store marked 'completed' must have had every update applied. A
--       returned row means the processor declared a store done too early,
--       leaving updates unapplied and stock out of date.
SELECT p.chain_id, p.store_id, p.last_seq, m.max_seq
FROM processor_stores p
JOIN (SELECT chain_id, store_id, max(seq) max_seq FROM queue_messages GROUP BY 1,2) m
  USING (chain_id, store_id)
WHERE p.status='completed' AND p.last_seq <> m.max_seq;

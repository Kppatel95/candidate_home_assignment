# StockStream Backlog Sync Processor

This implementation uses TypeScript, Node.js, PostgreSQL, and the plain `pg`
driver. Four separate worker OS processes drain the seeded queue; PostgreSQL
holds all shared progress and coordination.

## Requirements

- Node.js 20 or later
- npm
- Docker with Docker Compose

## Run it

From this directory:

```powershell
docker compose up -d
npm install
node seed.js
npm start
```

Start `npm start` immediately after `node seed.js`. The seed deliberately
creates receipt files that become ready over the next 30 seconds. The command
builds the TypeScript and launches all four workers.

The default connection string matches `docker-compose.yml`. To use another
database, set `DATABASE_URL` before starting both `seed.js` and the processor.

## Check the result

The worker output is JSON lines. Each applied event includes a store, sequence,
and message ID; stores with a permanently failed receipt or irreconcilable
sequence data are logged as blocked, while other stores continue.

Useful PostgreSQL checks:
Please run check.sql with PowerShell: `Get-Content check.sql | docker compose exec -T db psql -U postgres -d stockstream`

```sql
SELECT status, COUNT(*)
FROM processor_stores
GROUP BY status
ORDER BY status;

SELECT chain_id, store_id, last_seq, version
FROM store_stock
ORDER BY chain_id, store_id;

SELECT message_id, chain_id, store_id, seq, kind, worker_id
FROM apply_log
ORDER BY chain_id, store_id, seq;

SELECT chain_id, store_id, last_seq, blocked_reason
FROM processor_stores
WHERE status = 'blocked';
```

To run focused unit tests for payload validation and stock transformations:

```powershell
npm test
```

## Restart and poison scenarios

Progress, materialized stock, and audit entries are committed in the same
PostgreSQL transaction. If a worker is killed, the parent replaces it (with a
bounded restart budget). If the whole application stops, run `npm start` again
without reseeding to resume from committed cursors.

To test a permanent receipt failure, seed with `node seed.js --poison`, then
start the processor immediately. The affected store is quarantined at its
first unprocessable sequence; its later deltas are not applied, and the other
stores drain normally. A completed or quarantined batch exits with status 0.

Do not run `seed.js` while a processor is active: the supplied seed script
truncates the assignment tables. Re-seed only after stopping the application.
The processor uses a PostgreSQL advisory lock to reject concurrent processor
instances against the same database.

## Design and interview preparation

- [DESIGN.md](./DESIGN.md) records the guarantees, failure handling, tradeoffs,
  and validation performed.
- [AI-USAGE.md](./AI-USAGE.md) documents how AI contributed to this submission.

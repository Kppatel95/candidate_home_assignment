import { spawn, type ChildProcess } from 'node:child_process'
import { fileURLToPath } from 'node:url'
import { Client } from 'pg'

const DATABASE_URL =
  process.env.DATABASE_URL ?? 'postgres://postgres:postgres@localhost:5439/stockstream'
const WORKER_COUNT = 4
const MAX_RESTARTS_PER_WORKER = 5
const ADVISORY_LOCK_NAMESPACE = 853726
const ADVISORY_LOCK_ID = 1

const MIGRATIONS = `
  CREATE TABLE IF NOT EXISTS processor_stores (
    chain_id UUID NOT NULL,
    store_id UUID NOT NULL,
    last_seq INTEGER NOT NULL DEFAULT 0 CHECK (last_seq >= 0),
    status TEXT NOT NULL DEFAULT 'active'
      CHECK (status IN ('active', 'completed', 'blocked')),
    blocked_reason TEXT,
    next_attempt_at TIMESTAMPTZ,
    updated_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    PRIMARY KEY (chain_id, store_id)
  );
  ALTER TABLE processor_stores
    ADD COLUMN IF NOT EXISTS next_attempt_at TIMESTAMPTZ;
  CREATE INDEX IF NOT EXISTS processor_stores_status_key
    ON processor_stores (status, chain_id, store_id);
  CREATE UNIQUE INDEX IF NOT EXISTS apply_log_message_id_unique
    ON apply_log (message_id);
`

interface WorkerSlot {
  restarts: number
  child: ChildProcess | null
  done: boolean
}

function log(message: string, details?: Record<string, unknown>): void {
  console.log(JSON.stringify({ time: new Date().toISOString(), message, ...details }))
}

/**
 * Add only coordination data. The assignment's supplied schema remains intact.
 */
async function migrate(client: Client): Promise<void> {
  await client.query(MIGRATIONS)
}

/**
 * Rebuild state from the current seeded stores on a fresh run; otherwise keep
 * committed cursors so a restarted app resumes without replaying transactions.
 */
async function initializeStores(client: Client): Promise<void> {
  await client.query('BEGIN')
  try {
    const progress = await client.query<{ has_progress: boolean }>(
      `SELECT (
         EXISTS (SELECT 1 FROM store_stock) OR
         EXISTS (SELECT 1 FROM apply_log)
       ) AS has_progress`,
    )
    if (!progress.rows[0]?.has_progress) {
      await client.query('DELETE FROM processor_stores')
    }

    await client.query(
      `DELETE FROM processor_stores AS p
       WHERE NOT EXISTS (
         SELECT 1
         FROM queue_messages AS q
         WHERE q.chain_id = p.chain_id AND q.store_id = p.store_id
       )`,
    )
    await client.query(
      `INSERT INTO processor_stores (chain_id, store_id, last_seq, status)
       SELECT q.chain_id, q.store_id, COALESCE(s.last_seq, 0), 'active'
       FROM (
         SELECT DISTINCT chain_id, store_id FROM queue_messages
       ) AS q
       LEFT JOIN store_stock AS s
         ON s.chain_id = q.chain_id AND s.store_id = q.store_id
       ON CONFLICT (chain_id, store_id) DO NOTHING`,
    )
    await client.query('COMMIT')
  } catch (error) {
    await client.query('ROLLBACK')
    throw error
  }
}

function stopChildren(slots: WorkerSlot[]): void {
  for (const slot of slots) {
    if (slot.child && !slot.child.killed) {
      slot.child.kill('SIGTERM')
    }
  }
}

/**
 * Keep four OS processes available. Unexpected death (OOM/kill/redeploy) gets
 * a bounded restart; all progress is in Postgres, not in the parent process.
 */
function runWorkers(): Promise<number> {
  const slots: WorkerSlot[] = Array.from({ length: WORKER_COUNT }, () => ({
    restarts: 0,
    child: null,
    done: false,
  }))

  return new Promise((resolve) => {
    let finished = false
    let fatal = false
    let requestedSignal: NodeJS.Signals | null = null

    const finishIfDone = (): void => {
      if (finished || !slots.every((slot) => slot.done)) return
      finished = true
      process.removeListener('SIGINT', onInterrupt)
      process.removeListener('SIGTERM', onTerminate)
      resolve(requestedSignal === 'SIGINT' ? 130 : requestedSignal === 'SIGTERM' ? 143 : fatal ? 1 : 0)
    }

    const shutdown = (signal: NodeJS.Signals): void => {
      if (requestedSignal) return
      requestedSignal = signal
      stopChildren(slots)
      for (const slot of slots) {
        if (!slot.child) slot.done = true
      }
      finishIfDone()
    }

    function onInterrupt(): void {
      shutdown('SIGINT')
    }
    function onTerminate(): void {
      shutdown('SIGTERM')
    }

    process.once('SIGINT', onInterrupt)
    process.once('SIGTERM', onTerminate)

    const start = (slot: WorkerSlot, index: number): void => {
      if (requestedSignal || fatal) {
        slot.done = true
        finishIfDone()
        return
      }
      const workerPath = fileURLToPath(new URL('./worker.js', import.meta.url))
      const child = spawn(process.execPath, [workerPath], {
        env: process.env,
        stdio: 'inherit',
      })
      slot.child = child
      log('worker process launched', { slot: index + 1, pid: child.pid })

      let exitHandled = false
      const handleExit = (code: number | null, signal: NodeJS.Signals | null, error?: Error): void => {
        if (exitHandled) return
        exitHandled = true
        slot.child = null

        if (error) {
          console.error(`Worker ${index + 1} could not start:`, error)
        }
        if (requestedSignal) {
          slot.done = true
          finishIfDone()
          return
        }
        if (fatal) {
          slot.done = true
          finishIfDone()
          return
        }
        if (code === 0) {
          slot.done = true
          finishIfDone()
          return
        }

        slot.restarts += 1
        if (slot.restarts > MAX_RESTARTS_PER_WORKER) {
          fatal = true
          console.error(
            `Worker ${index + 1} exceeded ${MAX_RESTARTS_PER_WORKER} restarts; stopping the fleet.`,
            { code, signal },
          )
          stopChildren(slots)
          slot.done = true
          finishIfDone()
          return
        }

        const delay = Math.min(250 * 2 ** (slot.restarts - 1), 4000)
        log('worker exited unexpectedly; scheduling replacement', {
          slot: index + 1,
          code,
          signal,
          restart: slot.restarts,
          delayMs: delay,
        })
        setTimeout(() => start(slot, index), delay)
      }

      child.once('error', (error) => handleExit(null, null, error))
      child.once('exit', (code, signal) => handleExit(code, signal))
    }

    slots.forEach((slot, index) => start(slot, index))
  })
}

async function main(): Promise<number> {
  const client = new Client({ connectionString: DATABASE_URL })
  await client.connect()
  try {
    const lock = await client.query<{ locked: boolean }>(
      'SELECT pg_try_advisory_lock($1, $2) AS locked',
      [ADVISORY_LOCK_NAMESPACE, ADVISORY_LOCK_ID],
    )
    if (!lock.rows[0]?.locked) {
      throw new Error('Another StockStream processor is already running for this database')
    }

    await migrate(client)
    await initializeStores(client)
    log('database ready; starting worker fleet', { workers: WORKER_COUNT })
    return await runWorkers()
  } finally {
    await client.end()
  }
}

main()
  .then((exitCode) => {
    process.exitCode = exitCode
  })
  .catch((error: unknown) => {
    console.error('StockStream processor failed:', error)
    process.exitCode = 1
  })

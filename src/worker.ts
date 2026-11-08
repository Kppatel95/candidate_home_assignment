import { Client } from 'pg'
import { applyPayload, parsePayload, type Stock } from './stock.js'

const DATABASE_URL =
  process.env.DATABASE_URL ?? 'postgres://postgres:postgres@localhost:5439/stockstream'
const IDLE_POLL_MS = 250

type WorkResult = 'worked' | 'wait' | 'drained'

interface StoreWorkRow {
  chain_id: string
  store_id: string
  last_seq: number
}

interface QueueUpdateRow {
  message_id: string
  kind: string
  payload: unknown
  receipt_id: string
  ready_at: Date | null
}

interface StoreStockRow {
  stock: Stock
  last_seq: number
}

function log(workerId: string, message: string, details?: Record<string, unknown>): void {
  console.log(JSON.stringify({
    time: new Date().toISOString(),
    worker: workerId,
    message,
    ...details,
  }))
}

async function blockStore(
  client: Client,
  store: StoreWorkRow,
  reason: string,
): Promise<void> {
  await client.query(
    `UPDATE processor_stores
     SET status = 'blocked', blocked_reason = $3,
         next_attempt_at = NULL, updated_at = now()
     WHERE chain_id = $1 AND store_id = $2`,
    [store.chain_id, store.store_id, reason],
  )
}

/**
 * Claim and process at most one update. The row lock serializes updates for a
 * store, while SKIP LOCKED lets the other workers move to different stores.
 */
async function processOne(client: Client, workerId: string): Promise<WorkResult> {
  await client.query('BEGIN')
  try {
    const claimed = await client.query<StoreWorkRow>(
      `SELECT chain_id, store_id, last_seq
       FROM processor_stores
       WHERE status = 'active'
        AND (next_attempt_at IS NULL OR next_attempt_at <= now())
      ORDER BY chain_id, store_id
       LIMIT 1
       FOR UPDATE SKIP LOCKED`,
    )

    if (claimed.rowCount === 0) {
      const active = await client.query<{ exists: boolean }>(
        `SELECT EXISTS (
           SELECT 1 FROM processor_stores WHERE status = 'active'
         ) AS exists`,
      )
      await client.query('COMMIT')
      return active.rows[0]?.exists ? 'wait' : 'drained'
    }

    const store = claimed.rows[0]
    if (!store) {
      throw new Error('Locked store query returned no row')
    }

    const nextSeq = store.last_seq + 1
    const queued = await client.query<QueueUpdateRow>(
      `SELECT q.message_id, q.kind, q.payload, q.receipt_id, r.ready_at
       FROM queue_messages AS q
       JOIN receipt_files AS r ON r.id = q.receipt_id
       WHERE q.chain_id = $1 AND q.store_id = $2 AND q.seq = $3
       ORDER BY q.id`,
      [store.chain_id, store.store_id, nextSeq],
    )

    if (queued.rowCount === 0) {
      const maximum = await client.query<{ max_seq: number | null }>(
        `SELECT MAX(seq) AS max_seq
         FROM queue_messages
         WHERE chain_id = $1 AND store_id = $2`,
        [store.chain_id, store.store_id],
      )
      const maxSeq = maximum.rows[0]?.max_seq ?? null
      if (maxSeq !== null && maxSeq > nextSeq) {
        await blockStore(client, store, `Missing sequence ${nextSeq}; later sequence ${maxSeq} exists`)
        log(workerId, 'store blocked by sequence gap', {
          chainId: store.chain_id,
          storeId: store.store_id,
          seq: nextSeq,
        })
      } else {
        await client.query(
          `UPDATE processor_stores
           SET status = 'completed', next_attempt_at = NULL, updated_at = now()
           WHERE chain_id = $1 AND store_id = $2`,
          [store.chain_id, store.store_id],
        )
        log(workerId, 'store stream completed', {
          chainId: store.chain_id,
          storeId: store.store_id,
          lastSeq: store.last_seq,
        })
      }
      await client.query('COMMIT')
      return 'worked'
    }

    const first = queued.rows[0]
    if (!first) {
      throw new Error('Queue query returned a positive row count without a row')
    }

    // Retries are exact duplicate rows in the input. Conflicting identities at
    // one sequence cannot be safely ordered, so quarantine that store.
    const variants = await client.query<{ identity_count: number; variant_count: number }>(
      `SELECT COUNT(DISTINCT message_id)::int AS identity_count,
              COUNT(DISTINCT (kind, payload, receipt_id))::int AS variant_count
       FROM queue_messages
       WHERE chain_id = $1 AND store_id = $2 AND seq = $3`,
      [store.chain_id, store.store_id, nextSeq],
    )
    const counts = variants.rows[0]
    if (!counts || counts.identity_count !== 1 || counts.variant_count !== 1) {
      await blockStore(client, store, `Conflicting messages at sequence ${nextSeq}`)
      log(workerId, 'store blocked by conflicting sequence data', {
        chainId: store.chain_id,
        storeId: store.store_id,
        seq: nextSeq,
      })
      await client.query('COMMIT')
      return 'worked'
    }

    if (first.ready_at === null) {
      await blockStore(client, store, `Receipt ${first.receipt_id} permanently failed at sequence ${nextSeq}`)
      log(workerId, 'store blocked by failed receipt', {
        chainId: store.chain_id,
        storeId: store.store_id,
        seq: nextSeq,
        receiptId: first.receipt_id,
      })
      await client.query('COMMIT')
      return 'worked'
    }
    if (first.ready_at.getTime() > Date.now()) {
      await client.query(
        `UPDATE processor_stores
         SET next_attempt_at = $3, updated_at = now()
         WHERE chain_id = $1 AND store_id = $2`,
        [store.chain_id, store.store_id, first.ready_at],
      )
      await client.query('COMMIT')
      return 'wait'
    }

    const parsed = parsePayload(first.kind, first.payload)
    if (typeof parsed === 'string') {
      await blockStore(client, store, `Invalid payload at sequence ${nextSeq}: ${parsed}`)
      log(workerId, 'store blocked by invalid update payload', {
        chainId: store.chain_id,
        storeId: store.store_id,
        seq: nextSeq,
        reason: parsed,
      })
      await client.query('COMMIT')
      return 'worked'
    }

    const existingStock = await client.query<StoreStockRow>(
      `SELECT stock, last_seq
       FROM store_stock
       WHERE chain_id = $1 AND store_id = $2
       FOR UPDATE`,
      [store.chain_id, store.store_id],
    )
    const stockRow = existingStock.rows[0]
    const currentStock = stockRow?.stock ?? {}
    if (stockRow && stockRow.last_seq !== store.last_seq) {
      throw new Error(
        `Progress mismatch for ${store.chain_id}/${store.store_id}: ` +
        `processor=${store.last_seq}, stock=${stockRow.last_seq}`,
      )
    }
    if (!stockRow && store.last_seq !== 0) {
      throw new Error(`Missing stock row for progressed store ${store.chain_id}/${store.store_id}`)
    }

    const nextStock = applyPayload(currentStock, parsed)
    const audit = await client.query(
      `INSERT INTO apply_log (message_id, chain_id, store_id, seq, kind, worker_id)
       VALUES ($1, $2, $3, $4, $5, $6)
       ON CONFLICT (message_id) DO NOTHING
       RETURNING apply_pos`,
      [first.message_id, store.chain_id, store.store_id, nextSeq, first.kind, workerId],
    )
    if (audit.rowCount !== 1) {
      throw new Error(`Audit identity ${first.message_id} was already applied outside this progress step`)
    }

    await client.query(
      `INSERT INTO store_stock (chain_id, store_id, stock, version, last_seq)
       VALUES ($1, $2, $3::jsonb, 1, $4)
       ON CONFLICT (chain_id, store_id) DO UPDATE
       SET stock = EXCLUDED.stock,
           version = store_stock.version + 1,
           last_seq = EXCLUDED.last_seq,
           updated_at = now()`,
      [store.chain_id, store.store_id, JSON.stringify(nextStock), nextSeq],
    )
    const advanced = await client.query(
      `UPDATE processor_stores
       SET last_seq = $3, next_attempt_at = NULL, updated_at = now()
       WHERE chain_id = $1 AND store_id = $2 AND last_seq = $4 AND status = 'active'`,
      [store.chain_id, store.store_id, nextSeq, store.last_seq],
    )
    if (advanced.rowCount !== 1) {
      throw new Error(`Could not advance processor cursor for ${store.chain_id}/${store.store_id}`)
    }

    // Stock, audit row, and cursor commit together: a killed worker can never
    // leave the audit log and materialized stock describing different progress.
    await client.query('COMMIT')
    log(workerId, 'update applied', {
      chainId: store.chain_id,
      storeId: store.store_id,
      seq: nextSeq,
      messageId: first.message_id,
    })
    return 'worked'
  } catch (error) {
    await client.query('ROLLBACK')
    throw error
  }
}

async function runWorker(): Promise<void> {
  const workerId = `pid-${process.pid}`
  const client = new Client({ connectionString: DATABASE_URL })
  await client.connect()
  log(workerId, 'worker started')

  try {
    while (true) {
      const result = await processOne(client, workerId)
      if (result === 'drained') {
        log(workerId, 'all stores completed or quarantined')
        return
      }
      if (result === 'wait') {
        await new Promise((resolve) => setTimeout(resolve, IDLE_POLL_MS))
      }
    }
  } finally {
    await client.end()
  }
}

runWorker().catch((error: unknown) => {
  console.error(`[worker ${process.pid}] fatal error`, error)
  process.exitCode = 1
})

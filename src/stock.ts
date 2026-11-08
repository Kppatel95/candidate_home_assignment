export type Stock = Record<string, unknown>

export interface SnapshotPayload {
  stock: Stock
}

export interface DeltaPayload {
  set: Stock
  unset: string[]
}

export type ParsedPayload =
  | { kind: 'snapshot'; payload: SnapshotPayload }
  | { kind: 'delta'; payload: DeltaPayload }

function isRecord(value: unknown): value is Record<string, unknown> {
  return typeof value === 'object' && value !== null && !Array.isArray(value)
}

/**
 * Validate untrusted JSON at the database boundary so malformed data blocks
 * only its store instead of crashing a worker after partial work.
 */
export function parsePayload(kind: string, value: unknown): ParsedPayload | string {
  if (!isRecord(value)) {
    return 'payload must be a JSON object'
  }

  if (kind === 'snapshot') {
    if (!isRecord(value.stock)) {
      return 'snapshot payload must contain a stock object'
    }

    return { kind, payload: { stock: value.stock } }
  }

  if (kind === 'delta') {
    if (!isRecord(value.set)) {
      return 'delta payload must contain a set object'
    }
    if (!Array.isArray(value.unset) || !value.unset.every((key) => typeof key === 'string')) {
      return 'delta payload must contain an unset array of strings'
    }

    return { kind, payload: { set: value.set, unset: value.unset } }
  }

  return `unsupported update kind: ${kind}`
}

/**
 * Replace all stock for a snapshot, or apply a delta with unset taking
 * precedence if malformed/ambiguous input names a key in both collections.
 */
export function applyPayload(currentStock: Stock, update: ParsedPayload): Stock {
  if (update.kind === 'snapshot') {
    return { ...update.payload.stock }
  }

  const nextStock = { ...currentStock, ...update.payload.set }
  for (const key of update.payload.unset) {
    delete nextStock[key]
  }
  return nextStock
}

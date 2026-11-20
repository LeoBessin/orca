import { setImmediate as yieldToEventLoop, setTimeout as delay } from 'node:timers/promises'
import type SyncDatabase from '../../sqlite/sync-database'
import { traceSessionSearch } from '../session-search-trace'

const DELETED_KEY = 'fts_deleted_rows'
// Set while a full merge is under way, so a paused merge resumes rather than starting over.
const MERGING_KEY = 'fts_full_merge_running'
// Deleted rows keep their index entries until a merge rewrites the oldest segment holding them.
const MIN_DELETED_ROWS = 50_000
const MERGE_PAGES_PER_STEP = 32
// 2000 pages per step held the search writer for up to 137 ms.
const RECLAIM_PAGES_PER_STEP = 128
// Work runs at a tenth of the writer thread, so indexing and queries keep their latency.
const WORK_BEFORE_REST_MS = 25
const REST_PER_WORK = 9

// Merging waits until indexing has been quiet this long, and pauses when it resumes.
const QUIET_INDEXING_MS = 60_000

/** Tracks index writes; `run` is called once they have been quiet for a minute. */
export class SessionSearchQuietIndexing {
  private lastWrite = 0
  private timer: NodeJS.Timeout | undefined

  constructor(private readonly run: () => void) {}

  wrote(): void {
    this.lastWrite = performance.now()
    clearTimeout(this.timer)
    this.timer = setTimeout(this.run, QUIET_INDEXING_MS)
    this.timer.unref()
  }

  get quiet(): boolean {
    return performance.now() - this.lastWrite >= QUIET_INDEXING_MS
  }
}

/** Counts index rows deleted since the last full merge, in the caller's transaction. */
export function recordSessionSearchDeletes(db: SyncDatabase, deleted: number): void {
  if (deleted > 0) {
    // No row means no full merge yet, which is already due; counting starts after the first one.
    db.prepare('UPDATE meta SET value = CAST(value AS INTEGER) + ? WHERE key = ?').run(
      deleted,
      DELETED_KEY
    )
  }
}

/** Due once for an index this build never merged, then when deletes reach a quarter of live rows. */
export function sessionSearchIndexCompactionDue(db: SyncDatabase): boolean {
  const row = db.prepare('SELECT value FROM meta WHERE key = ?').get(DELETED_KEY)
  if (!row) {
    return true
  }
  const deleted = Number(row.value)
  return (
    deleted >= MIN_DELETED_ROWS &&
    deleted * 4 >= Number(db.prepare('SELECT COUNT(*) AS n FROM messages').get()!.n)
  )
}

/** Merges every segment into one in bounded steps, dropping deleted rows, then returns the freed pages.
 *  `stop` pauses it between steps; the next call continues the same merge. */
export async function compactSessionSearchIndex(
  db: SyncDatabase,
  stop: () => boolean
): Promise<void> {
  let work = 0
  const step = async <T>(run: () => T): Promise<T> => {
    const started = performance.now()
    const result = run()
    work += performance.now() - started
    if (work >= WORK_BEFORE_REST_MS) {
      await delay(work * REST_PER_WORK, undefined, { ref: false })
      work = 0
    } else {
      await yieldToEventLoop()
    }
    return result
  }
  const changes = db.prepare('SELECT total_changes() AS n')
  const merge = db.prepare("INSERT INTO messages_fts(messages_fts, rank) VALUES('merge', ?)")
  const setMeta = db.prepare('INSERT OR REPLACE INTO meta(key, value) VALUES (?, ?)')
  // Negative starts merging all segments together; positive steps continue that merge.
  let pages = db.prepare('SELECT 1 FROM meta WHERE key = ?').get(MERGING_KEY)
    ? MERGE_PAGES_PER_STEP
    : -MERGE_PAGES_PER_STEP
  while (!stop()) {
    const before = Number(changes.get()!.n)
    await step(() => traceSessionSearch('merge', () => merge.run(pages)))
    if (Number(changes.get()!.n) - before < 2) {
      db.exec('BEGIN IMMEDIATE')
      setMeta.run(DELETED_KEY, '0')
      db.prepare('DELETE FROM meta WHERE key = ?').run(MERGING_KEY)
      db.exec('COMMIT')
      break
    }
    if (pages < 0) {
      setMeta.run(MERGING_KEY, '1')
    }
    pages = MERGE_PAGES_PER_STEP
  }
  let free = stop() ? 0 : Number(db.pragma('freelist_count', { simple: true }))
  while (free > 0 && !stop()) {
    await step(() =>
      traceSessionSearch('vacuum', () => db.pragma(`incremental_vacuum(${RECLAIM_PAGES_PER_STEP})`))
    )
    const remaining = Number(db.pragma('freelist_count', { simple: true }))
    if (remaining >= free) {
      break
    }
    free = remaining
  }
}

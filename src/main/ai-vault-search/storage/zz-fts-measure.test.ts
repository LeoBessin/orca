import { homedir } from 'node:os'
import { mkdirSync, readFileSync, rmSync, statSync, writeFileSync } from 'node:fs'
import { join } from 'node:path'
import { monitorEventLoopDelay } from 'node:perf_hooks'
import { expect, it } from 'vitest'
import { openSessionSearchDatabase } from './session-search-schema'
import { deleteSearchMessages, insertSearchMessage } from './session-search-message-rows'
import { compactSessionSearchIndex, sessionSearchIndexCompactionDue } from './session-search-index-compaction'

const ROOT = process.env.MEASURE_ROOT ?? join(homedir(), '.cache/orca-jobs/dbgrowth-fts-measure')
const SESSIONS = Number(process.env.MEASURE_SESSIONS ?? 8000)
const PER_SESSION = Number(process.env.MEASURE_PER_SESSION ?? 40)
const CHURN = Number(process.env.MEASURE_CHURN ?? 4)

let seed = 3
const rand = () => (seed = (seed * 1103515245 + 12345) % 2147483648) / 2147483648
const words = Array.from({ length: 20000 }, () => Math.floor(rand() * 2 ** 40).toString(36))
const text = (n: number) => Array.from({ length: n }, () => words[Math.floor(rand() ** 2 * 20000)]).join(' ')

function procWrite(): number | null {
  try {
    return Number(/^write_bytes: (\d+)/m.exec(readFileSync('/proc/self/io', 'utf8'))?.[1])
  } catch {
    return null
  }
}

function writeSession(db: ReturnType<typeof openSessionSearchDatabase>, session: number) {
  db.exec('BEGIN IMMEDIATE')
  for (let m = 0; m < PER_SESSION; m++) {
    insertSearchMessage(db, session, {
      role: m % 4 === 0 ? 'user' : 'assistant',
      text: text(40 + Math.floor(rand() * 400)),
      timestamp: null,
      textTruncated: false
    } as never)
  }
  db.exec('COMMIT')
}

it('measures dead FTS data and paced compaction', { timeout: 6 * 3_600_000 }, async () => {
  rmSync(ROOT, { recursive: true, force: true })
  mkdirSync(ROOT, { recursive: true })
  const file = join(ROOT, 'session-search.sqlite')
  const db = openSessionSearchDatabase(file)
  for (let s = 1; s <= SESSIONS; s++) {
    writeSession(db, s)
  }
  // Reindexing replaces a session's rows, as a replace-mode read does.
  for (let round = 0; round < CHURN; round++) {
    for (let s = 1; s <= SESSIONS; s++) {
      db.exec('BEGIN IMMEDIATE')
      deleteSearchMessages(db, s)
      db.exec('COMMIT')
      writeSession(db, s)
    }
  }
  const size = () => {
    db.pragma('wal_checkpoint(TRUNCATE)')
    const pages = Number(db.pragma('page_count', { simple: true }))
    const free = Number(db.pragma('freelist_count', { simple: true }))
    const fts = Number(db.prepare('SELECT SUM(length(block)) AS n FROM messages_fts_data').get()!.n)
    return {
      fileMB: Math.round(statSync(file).size / 2 ** 20),
      freeMB: Math.round((free * 4096) / 2 ** 20),
      liveMB: Math.round(((pages - free) * 4096) / 2 ** 20),
      ftsDataMB: Math.round(fts / 2 ** 20)
    }
  }
  const out: Record<string, unknown> = {
    rows: Number(db.prepare('SELECT COUNT(*) AS n FROM messages').get()!.n),
    maxId: Number(db.prepare('SELECT MAX(id) AS n FROM messages').get()!.n),
    due: sessionSearchIndexCompactionDue(db),
    before: size()
  }
  const loop = monitorEventLoopDelay({ resolution: 5 })
  loop.enable()
  const t = performance.now()
  const ioStart = procWrite()
  const probe: number[] = []
  let stop = false
  const probing = (async () => {
    while (!stop) {
      const s = performance.now()
      await new Promise((r) => setTimeout(r, 50))
      probe.push(performance.now() - s - 50)
    }
  })()
  const stepMs: Record<string, number[]> = { merge: [], vacuum: [] }
  const pragma = db.pragma.bind(db)
  ;(db as unknown as { pragma: typeof db.pragma }).pragma = ((sql: string, o?: never) => {
    const s0 = performance.now()
    const r = pragma(sql, o)
    if (sql.startsWith('incremental_vacuum')) stepMs.vacuum.push(performance.now() - s0)
    return r
  }) as typeof db.pragma
  const prepare = db.prepare.bind(db)
  ;(db as unknown as { prepare: typeof db.prepare }).prepare = ((sql: string) => {
    const st = prepare(sql)
    if (!sql.includes("'merge'")) return st
    const run = st.run.bind(st)
    ;(st as unknown as { run: typeof st.run }).run = ((...a: never[]) => {
      const s0 = performance.now()
      const r = run(...a)
      stepMs.merge.push(performance.now() - s0)
      return r
    }) as typeof st.run
    return st
  }) as typeof db.prepare
  await compactSessionSearchIndex(db, () => false)
  out.steps = Object.fromEntries(Object.entries(stepMs).map(([k, v]) => { const x = [...v].sort((a, b) => a - b); return [k, { n: x.length, p50: +(x[Math.floor(x.length / 2)] ?? 0).toFixed(1), p99: +(x[Math.floor(x.length * 0.99)] ?? 0).toFixed(1), max: +(x.at(-1) ?? 0).toFixed(1) }] }))
  stop = true
  await probing
  loop.disable()
  const sorted = probe.sort((a, b) => a - b)
  out.compaction = {
    wallSec: Math.round((performance.now() - t) / 1000),
    timerLateMs: { p50: +sorted[Math.floor(sorted.length / 2)]!.toFixed(1), p99: +sorted[Math.floor(sorted.length * 0.99)]!.toFixed(1), max: +sorted.at(-1)!.toFixed(1) },
    eventLoopDelayMaxMs: +(loop.max / 1e6).toFixed(1),
    diskWriteMB: ioStart === null ? null : Math.round((procWrite()! - ioStart) / 2 ** 20)
  }
  out.after = size()
  out.dueAfter = sessionSearchIndexCompactionDue(db)
  db.close()
  rmSync(ROOT, { recursive: true, force: true })
  writeFileSync(process.env.MEASURE_OUT ?? '/tmp/measure-result.json', JSON.stringify(out, null, 1))
  process.stderr.write(`MEASURE ${JSON.stringify(out)}\n`)
  expect(out.dueAfter).toBe(false)
})

import { mkdtempSync, rmSync } from 'node:fs'
import { tmpdir } from 'node:os'
import { join } from 'node:path'
import Database from '../../../../sqlite/sync-database'
import { OrchestrationDb } from '../orchestration-db'
import { expect, it, vi } from 'vitest'
import {
  prepareCoordinatorMailCount,
  preparedCoordinatorMailCount
} from './prepared-coordinator-mail-count'
import { coordinatorMailContribution } from './coordinator-mail-count-triggers'
import { readCoordinatorMailCount } from './dispatch-read-mail'
import { createRootDispatch } from '../root-dispatch-test-fixture'
import {
  COORDINATOR,
  coordinatorRunAndTask,
  openChat,
  db
} from '../../terminal-authority/structured-sessions/structured-chat-coordinator-mail-rig.test-fixture'

it('keeps exact finish identities through out-of-order reads, rollback, sender changes and retirement', async () => {
  await openChat(COORDINATOR)
  const { taskId, runId } = await coordinatorRunAndTask()
  const worker = createRootDispatch(db, taskId, 'term_worker', 'tab:w')
  const first = db.insertMessage({
    from: 'term_parent',
    to: `dispatch:${worker.id}`,
    subject: 'First',
    runId
  })
  const second = db.insertMessage({
    from: 'term_parent',
    to: `dispatch:${worker.id}`,
    subject: 'Second',
    runId
  })
  db.markAsRead([second.id])
  expect(await prepareCoordinatorMailCount(db, worker.id, () => true)).toBe(1)
  db.markAsRead([first.id, second.id])
  expect(readCoordinatorMailCount(db, worker.id)).toBe(2)
  db.db.exec('SAVEPOINT count_test')
  const third = db.insertMessage({
    from: 'term_parent',
    to: `dispatch:${worker.id}`,
    subject: 'Third',
    runId
  })
  db.markAsRead([third.id])
  expect(readCoordinatorMailCount(db, worker.id)).toBe(3)
  db.db.exec('ROLLBACK TO count_test; RELEASE count_test')
  expect(readCoordinatorMailCount(db, worker.id)).toBe(2)
  // Historical sender identity can change classification, even after its Dispatch settles.
  const siblingTask = db.createTask({ spec: 'Sibling', runId })
  const sibling = createRootDispatch(db, siblingTask.id, 'term_other', 'tab:s')
  db.db
    .prepare('UPDATE dispatch_contexts SET assignee_handle = ?, created_at = ? WHERE id = ?')
    .run('term_parent', '2000-01-01 00:00:00', sibling.id)
  expect(await prepareCoordinatorMailCount(db, worker.id, () => true)).toBe(0)
  db.db
    .prepare('UPDATE dispatch_contexts SET assignee_handle = ? WHERE id = ?')
    .run('term_other', sibling.id)
  expect(await prepareCoordinatorMailCount(db, worker.id, () => true)).toBe(2)
  db.db.prepare('DELETE FROM messages WHERE id = ?').run(first.id)
  expect(readCoordinatorMailCount(db, worker.id)).toBe(1)
  db.db.prepare("UPDATE dispatch_contexts SET status = 'completed' WHERE id = ?").run(worker.id)
  expect(
    db.db.prepare('SELECT 1 FROM orca_coordinator_mail_counts WHERE dispatch_id = ?').get(worker.id)
  ).toBeUndefined()
  expect(db.db.prepare('SELECT 1 FROM orca_mail_lifetime_runs LIMIT 1').get()).toBeUndefined()
})

it.each([
  [100, 100, 101],
  [1_000, 1_000, 1_001]
])(
  'does not re-enumerate %i historical rows on repeated stops or new mail',
  async (history, expected, afterNewMail) => {
    const db = new OrchestrationDb(':memory:')
    try {
      const runId = db.createRun({
        objective: 'Prepared count',
        coordinatorHandle: 'term_parent',
        coordinatorPaneKey: null
      }).id
      const taskId = db.createTask({ spec: 'Worker', runId }).id
      const worker = createRootDispatch(db, taskId, 'term_target', 'tab:t')
      for (let i = 0; i < history; i++) {
        const task = db.createTask({ spec: `Noise ${i}`, runId })
        createRootDispatch(db, task.id, `term_noise_${i}`, `tab:n${i}`)
        const mail = db.insertMessage({
          from: 'term_parent',
          to: `dispatch:${worker.id}`,
          subject: 'Old',
          runId
        })
        db.markAsRead([mail.id])
      }
      expect(await prepareCoordinatorMailCount(db, worker.id, () => true)).toBe(expected)
      let historyRows = 0
      const prepare = db.db.prepare.bind(db.db)
      vi.spyOn(db.db, 'prepare').mockImplementation((sql) => {
        const statement = prepare(sql)
        return {
          all(...args) {
            const rows = statement.all(...args)
            historyRows += rows.length
            return rows
          },
          get: statement.get.bind(statement),
          run: statement.run.bind(statement),
          iterate: statement.iterate.bind(statement),
          setReadBigInts: statement.setReadBigInts.bind(statement)
        }
      })
      expect(await prepareCoordinatorMailCount(db, worker.id, () => true)).toBe(expected)
      expect(await prepareCoordinatorMailCount(db, worker.id, () => true)).toBe(expected)
      const task = db.createTask({ spec: 'Unrelated new worker', runId })
      createRootDispatch(db, task.id, 'term_unrelated_new', 'tab:new')
      expect(await prepareCoordinatorMailCount(db, worker.id, () => true)).toBe(expected)
      const mail = db.insertMessage({
        from: 'term_parent',
        to: `dispatch:${worker.id}`,
        subject: 'New',
        runId
      })
      db.markAsRead([mail.id])
      expect(await prepareCoordinatorMailCount(db, worker.id, () => true)).toBe(afterNewMail)
      expect(historyRows).toBe(0)
    } finally {
      db.close()
    }
  }
)

it.each([100, 1_000])(
  'bounds sender work with %i reused identities in another Run',
  async (history) => {
    await openChat(COORDINATOR)
    const { taskId, runId } = await coordinatorRunAndTask()
    const worker = createRootDispatch(db, taskId, 'term_target', 'tab:target')
    const other = db.createRun({
      objective: 'Unrelated',
      coordinatorHandle: 'term_else',
      coordinatorPaneKey: null
    })
    for (let i = 0; i < history; i++) {
      const task = db.createTask({ spec: 'Historical assignment', runId: other.id })
      const assignment = createRootDispatch(db, task.id, 'term_parent', 'old:parent')
      db.db
        .prepare("UPDATE dispatch_contexts SET status = 'completed' WHERE id = ?")
        .run(assignment.id)
      const mail = db.insertMessage({
        from: i % 2 === 0 ? 'term_parent' : `term_unique_${i}`,
        senderPaneKey: 'new:parent',
        to: `dispatch:${worker.id}`,
        subject: 'Follow-up',
        runId
      })
      db.markAsRead([mail.id])
    }
    let classifierCalls = 0
    const register = db.db.function.bind(db.db)
    vi.spyOn(db.db, 'function').mockImplementation((name, callback) => {
      if (name !== 'orca_mail_worker_matches') {
        return register(name, callback)
      }
      register(name, (id, handle, pane, created, completed, from, senderPane, sent) => {
        classifierCalls++
        return callback(id, handle, pane, created, completed, from, senderPane, sent)
      })
    })
    expect(await prepareCoordinatorMailCount(db, worker.id, () => true)).toBe(history)
    expect(classifierCalls).toBe(0)
    const triggerSql = String(
      db.db
        .prepare("SELECT sql FROM sqlite_temp_master WHERE name = 'orca_mail_count_delete'")
        .get()!.sql
    )
    const decrementSql = triggerSql.match(
      /UPDATE orca_coordinator_mail_senders SET refs = refs - 1[^;]+/
    )![0]
    const decrementPlan = db.db
      .prepare(
        `EXPLAIN QUERY PLAN ${decrementSql.replace('SET refs = refs - 1', 'SET refs = refs - 1 FROM messages OLD').replace('WHERE run_id =', 'WHERE orca_coordinator_mail_senders.run_id =')} AND OLD.id = ?`
      )
      .all('absent-message')
      .map((row) => String(row.detail))
    expect(
      decrementPlan.some((detail) =>
        detail.includes('run_id=? AND kind=? AND identity=? AND dispatch_id=?')
      )
    ).toBe(true)

    const plan = db.db
      .prepare(
        `EXPLAIN QUERY PLAN SELECT ${coordinatorMailContribution('m', '?')} FROM messages m WHERE m.to_handle = ? AND m.read = 1 AND m.sequence > ? ORDER BY m.sequence LIMIT 128`
      )
      .all(worker.id, `dispatch:${worker.id}`, 0)
      .map((row) => String(row.detail))
    expect(
      plan.some(
        (detail) =>
          detail.includes('orca_mail_lifetime_start') &&
          detail.includes('run_id=? AND kind=? AND identity=? AND node=? AND start_ms<?')
      )
    ).toBe(true)
    expect(
      plan.some(
        (detail) =>
          detail.includes('orca_mail_lifetime_end') &&
          detail.includes('run_id=? AND kind=? AND identity=? AND node=? AND end_ms>?')
      )
    ).toBe(true)
    const nextTask = db.createTask({ spec: 'New unrelated assignment', runId: other.id })
    const inbox = vi.spyOn(db.db, 'prepare')
    const next = createRootDispatch(db, nextTask.id, 'term_parent', 'new:parent')
    db.completeDispatch(next.id)
    expect(preparedCoordinatorMailCount(db, worker.id)).toBe(history)
    expect(await prepareCoordinatorMailCount(db, worker.id, () => true)).toBe(history)
    expect(await prepareCoordinatorMailCount(db, worker.id, () => true)).toBe(history)
    expect(
      inbox.mock.calls.filter(([sql]) => sql.includes('AS counted FROM messages'))
    ).toHaveLength(0)
    // Include trigger programs: they must never open the historical messages B-tree on a Dispatch write.
    const messageRoot = db.db
      .prepare("SELECT rootpage FROM sqlite_master WHERE name = 'messages'")
      .get()!.rootpage
    const instructions = db.db
      .prepare('EXPLAIN UPDATE dispatch_contexts SET assignee_handle = ? WHERE id = ?')
      .all('term_changed', next.id)
    let countTrigger = false
    const messageReads = instructions.filter((op) => {
      if (op.opcode === 'Init') {
        countTrigger = String(op.p4).includes('TRIGGER orca_mail_count_')
      }
      return countTrigger && op.opcode === 'OpenRead' && op.p2 === messageRoot && op.p3 === 0
    })
    expect(instructions.some((op) => String(op.p4).includes('TRIGGER orca_mail_count_'))).toBe(true)
    expect(messageReads).toHaveLength(0)
  }
)

it('keeps an in-flight cold count owner through external-write invalidation', async () => {
  await openChat(COORDINATOR)
  const { taskId, runId } = await coordinatorRunAndTask()
  const worker = createRootDispatch(db, taskId, 'term_target', 'tab:target')
  const mail = db.insertMessage({
    from: 'term_parent',
    to: `dispatch:${worker.id}`,
    subject: 'Second round',
    runId
  })
  db.markAsRead([mail.id])
  let version = 1
  const pragma = db.db.pragma.bind(db.db)
  vi.spyOn(db.db, 'pragma').mockImplementation((sql, options) =>
    sql === 'data_version' ? version : pragma(sql, options)
  )
  const count = prepareCoordinatorMailCount(db, worker.id, () => true)
  version = 2
  expect(preparedCoordinatorMailCount(db, worker.id)).toBeUndefined()
  expect(await count).toBe(1)
})

it.each([1_000, 10_000])(
  'reconciles live and retired recovery owners after real external commits among %i assignments',
  async (population) => {
    const directory = mkdtempSync(join(tmpdir(), 'orca-mail-count-external-'))
    const path = join(directory, 'orchestration.db')
    const database = new OrchestrationDb(path)
    const outside = new Database(path)
    try {
      const run = database.createRun({
        objective: 'External recovery',
        coordinatorHandle: 'term_parent',
        coordinatorPaneKey: null
      })
      const task = database.createTask({ spec: 'Live helper', runId: run.id })
      const helper = createRootDispatch(database, task.id, 'term_helper', 'tab:helper')
      const columns = database.db
        .prepare('PRAGMA table_info(dispatch_contexts)')
        .all()
        .map((row) => String(row.name))
      database.db
        .prepare(`WITH RECURSIVE n(i) AS (SELECT 1 UNION ALL SELECT i+1 FROM n WHERE i < ?)
      INSERT INTO dispatch_contexts (${columns.join(', ')}) SELECT ${columns.map((column) => (column === 'id' ? "'ctx_external_noise_' || n.i" : column === 'status' ? "'completed'" : `d.${column}`)).join(', ')}
      FROM n CROSS JOIN dispatch_contexts d WHERE d.id = ?`)
        .run(population, helper.id)
      const mail = database.insertMessage({
        runId: run.id,
        from: 'term_parent',
        to: `dispatch:${helper.id}`,
        subject: 'Second round'
      })
      database.markAsRead([mail.id])
      const liveRecovery = prepareCoordinatorMailCount(database, helper.id, () => true)
      outside
        .prepare('UPDATE messages SET body = ? WHERE id = ?')
        .run('External unrelated edit', mail.id)
      expect(await liveRecovery).toBe(1)
      const laterMail = database.insertMessage({
        runId: run.id,
        from: 'term_parent',
        to: `dispatch:${helper.id}`,
        subject: 'External read'
      })
      outside.prepare('UPDATE messages SET read = 1 WHERE id = ?').run(laterMail.id)
      database.db.exec('SAVEPOINT external_count')
      expect(readCoordinatorMailCount(database, helper.id)).toBe(2)
      database.db.exec('ROLLBACK TO external_count; RELEASE external_count')
      expect(readCoordinatorMailCount(database, helper.id)).toBe(2)
      expect(await prepareCoordinatorMailCount(database, helper.id, () => true)).toBe(2)
      // A second active owner shares the indexed Run. Retiring it must not retain obsolete work.
      const removedTask = database.createTask({ spec: 'Removed helper', runId: run.id })
      const removed = createRootDispatch(database, removedTask.id, 'term_removed', 'tab:removed')
      const removedRecovery = prepareCoordinatorMailCount(database, removed.id, () => true)
      outside.prepare('DELETE FROM dispatch_contexts WHERE id = ?').run(removed.id)
      expect(await removedRecovery).toBeUndefined()
      expect(
        database.db
          .prepare('SELECT 1 FROM orca_coordinator_mail_counts WHERE dispatch_id = ?')
          .get(removed.id)
      ).toBeUndefined()
      // Both a prepared owner and one recovering owner are reconciled against external settlement.
      const retiringTask = database.createTask({ spec: 'Settling helper', runId: run.id })
      const retiring = createRootDispatch(
        database,
        retiringTask.id,
        'term_retiring',
        'tab:retiring'
      )
      const retiringRecovery = prepareCoordinatorMailCount(database, retiring.id, () => true)
      outside
        .prepare("UPDATE dispatch_contexts SET status = 'completed' WHERE id IN (?, ?)")
        .run(helper.id, retiring.id)
      expect(await retiringRecovery).toBeUndefined()
      expect(
        database.db.prepare('SELECT 1 FROM orca_coordinator_mail_counts LIMIT 1').get()
      ).toBeUndefined()
      expect(
        database.db.prepare('SELECT 1 FROM orca_mail_worker_lifetimes LIMIT 1').get()
      ).toBeUndefined()
    } finally {
      outside.close()
      database.close()
      rmSync(directory, { recursive: true, force: true })
    }
  }
)

it('restarts a cold recovery after message reset without losing a surviving owner', async () => {
  await openChat(COORDINATOR)
  const { taskId, runId } = await coordinatorRunAndTask()
  const helper = createRootDispatch(db, taskId, 'term_helper', 'tab:helper')
  const mail = db.insertMessage({
    runId,
    from: 'term_parent',
    to: `dispatch:${helper.id}`,
    subject: 'Follow-up'
  })
  db.markAsRead([mail.id])
  const cold = prepareCoordinatorMailCount(db, helper.id, () => true)
  db.resetMessages()
  expect(await cold).toBe(0)
})

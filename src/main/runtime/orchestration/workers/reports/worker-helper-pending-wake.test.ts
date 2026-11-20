import { expect, it, vi } from 'vitest'
import { workerHasPendingHelperWake } from './worker-helper-pending-wake'
import { reportWorkerTurnStop } from '../turn-stops/worker-turn-stop-report'
import { createRootDispatch } from '../../db/root-dispatch-test-fixture'
import {
  COORDINATOR,
  coordinatorRunAndTask,
  openChat,
  runtime,
  db
} from '../../terminal-authority/structured-sessions/structured-chat-coordinator-mail-rig.test-fixture'

it('reports a stopped worker after its later-created helper reports idle, preserving live nested waits', async () => {
  await openChat(COORDINATOR)
  const { taskId, runId } = await coordinatorRunAndTask()
  const worker = createRootDispatch(db, taskId, 'term_w', 'tab:w')
  const notify = vi.fn()
  const deps = { getDb: () => db, notify, isWaitingForMail: () => false }
  const target = { kind: 'local' as const, dispatch: worker }
  const stop = { from: 'term_w', reason: 'finished' as const, lastOutput: 'Worker result' }
  expect(await runtime.canPromiseOrchestrationWorkerWake(target)).toBe(true)
  const helperTask = db.createTask({ spec: 'helper', runId })
  const helper = createRootDispatch(db, helperTask.id, 'term_h', 'tab:h')
  db.db
    .prepare(
      'UPDATE dispatch_contexts SET creator_dispatch_id = ?, process_incarnation = ? WHERE id = ?'
    )
    .run(worker.id, 'h:1', helper.id)
  const active = new Set(['tab:h'])
  vi.spyOn(runtime, 'isOrchestrationWorkerWorking').mockImplementation((pane) => active.has(pane))
  const pending = () => workerHasPendingHelperWake(runtime, worker.id, () => true)
  expect(await pending()).toBe(true)
  reportWorkerTurnStop(db, target, { ...stop, pendingHelperWake: await pending() }, deps)
  expect(notify).not.toHaveBeenCalled()
  // An idle intermediate helper can still await a genuinely working descendant.
  active.clear()
  const nestedTask = db.createTask({ spec: 'nested', runId })
  const nested = createRootDispatch(db, nestedTask.id, 'term_j', 'tab:j')
  db.db
    .prepare(
      'UPDATE dispatch_contexts SET creator_dispatch_id = ?, process_incarnation = ? WHERE id = ?'
    )
    .run(helper.id, 'j:1', nested.id)
  active.add('tab:j')
  expect(await pending()).toBe(true)
  active.clear()
  // Its plain stop leaves the helper Dispatch open. Once its report is consumed, no wake remains.
  reportWorkerTurnStop(
    db,
    { kind: 'local', dispatch: db.getDispatchContextById(helper.id)! },
    { from: 'term_h', reason: 'finished', lastOutput: 'Helper marker' },
    deps
  )
  db.db.prepare('UPDATE messages SET read = 1 WHERE to_handle = ?').run(`dispatch:${worker.id}`)
  expect(db.getDispatchContextById(helper.id)?.status).toBe('dispatched')
  expect(await pending()).toBe(false)
  // A new request from its creator permits a fresh report, even though the first was consumed.
  const followUp = db.insertMessage({
    from: 'term_w',
    to: `dispatch:${helper.id}`,
    subject: 'Second round',
    runId
  })
  db.markAsRead([followUp.id])
  active.add('tab:h')
  const pragma = db.db.pragma.bind(db.db)
  let observations = 0
  const externalCommit = vi.spyOn(db.db, 'pragma').mockImplementation((sql, options) => {
    if (sql === 'data_version') {
      return observations++ === 0 ? 1 : 2
    }
    return pragma(sql, options)
  })
  const queries = vi.spyOn(db.db, 'prepare')
  expect(await pending()).toBe(true)
  const rejectionSql = queries.mock.calls.find(([sql]) =>
    sql.includes("AND type = 'worker_done' AND json_type")
  )?.[0]
  queries.mockRestore()
  externalCommit.mockRestore()
  const promiseReport = runtime.canPromiseOrchestrationHelperReport.bind(runtime)
  const becameIdle = vi
    .spyOn(runtime, 'canPromiseOrchestrationHelperReport')
    .mockImplementation(async (...args) => {
      const report = await promiseReport(...args)
      active.clear()
      return report
    })
  expect(await pending()).toBe(false)
  becameIdle.mockRestore()
  active.add('tab:h')
  expect(rejectionSql).toBeDefined()
  const rejectionPlan = db.db
    .prepare(`EXPLAIN QUERY PLAN ${rejectionSql!}`)
    .all(helper.id)
    .map((row) => String(row.detail))
  expect(rejectionPlan.some((detail) => detail.includes('idx_messages_goal_wake_blockers'))).toBe(
    true
  )
  expect(rejectionPlan.some((detail) => detail === 'SCAN messages')).toBe(false)
  notify.mockClear()
  reportWorkerTurnStop(db, target, { ...stop, pendingHelperWake: await pending() }, deps)
  expect(notify).not.toHaveBeenCalled()
  const recoveringAcrossReset = pending()
  db.resetMessages()
  expect(await recoveringAcrossReset).toBe(true)
  active.clear()
  expect(
    reportWorkerTurnStop(
      db,
      { kind: 'local', dispatch: db.getDispatchContextById(helper.id)! },
      { from: 'term_h', reason: 'finished', lastOutput: 'Second helper marker' },
      deps
    )
  ).toBe(true)
  expect(
    db.db
      .prepare("SELECT body FROM messages WHERE to_handle = ? AND type = 'escalation'")
      .all(`dispatch:${worker.id}`)
  ).toEqual([expect.objectContaining({ body: expect.stringContaining('Second helper marker') })])
  db.db.prepare('UPDATE messages SET read = 1 WHERE to_handle = ?').run(`dispatch:${worker.id}`)
  expect(await pending()).toBe(false)
  notify.mockClear()
  reportWorkerTurnStop(db, target, { ...stop, pendingHelperWake: await pending() }, deps)
  expect(notify).toHaveBeenCalledTimes(1)
  expect(db.getUnreadRunMailbox(runId, 100, ['escalation'])).toMatchObject([
    { subject: 'Worker stopped without reporting' }
  ])
})

it('does not promise a new helper finish after a rejected worker_done in its working turn', async () => {
  await openChat(COORDINATOR)
  const { taskId, runId } = await coordinatorRunAndTask()
  const worker = createRootDispatch(db, taskId, 'term_w', 'tab:w')
  const task = db.createTask({ spec: 'Helper', runId })
  const helper = createRootDispatch(db, task.id, 'term_h', 'tab:h')
  db.db
    .prepare(
      'UPDATE dispatch_contexts SET creator_dispatch_id = ?, process_incarnation = ? WHERE id = ?'
    )
    .run(worker.id, 'h:1', helper.id)
  vi.spyOn(runtime, 'isOrchestrationWorkerWorking').mockReturnValue(true)
  const rejection = db.insertMessage({
    from: 'term_h',
    to: `dispatch:${worker.id}`,
    subject: 'Done',
    type: 'worker_done',
    payload: JSON.stringify({ dispatchId: helper.id, taskId }),
    runId
  })
  db.convertLifecycleMessageToRejection(rejection.id, 'task_mismatch', 'Wrong task')
  db.markAsRead([rejection.id])
  expect(await workerHasPendingHelperWake(runtime, worker.id, () => true)).toBe(false)
  const notify = vi.fn()
  reportWorkerTurnStop(
    db,
    { kind: 'local', dispatch: worker },
    { from: 'term_w', reason: 'finished', lastOutput: 'Helper report rejected' },
    { getDb: () => db, notify, isWaitingForMail: () => false }
  )
  expect(notify).toHaveBeenCalledTimes(1)
})

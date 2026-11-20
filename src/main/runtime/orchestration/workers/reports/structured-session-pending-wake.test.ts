import { expect, it, vi } from 'vitest'
import { formatOrcaSessionAddress } from '../../../../../shared/agent-session/identity/orca-session-address'
import { testOrcaSessionId } from '../../../../../shared/agent-session/identity/orca-session-address-test-fixture'
import { getOrchestrationPeerCapabilityCache } from '../../federation/orchestration-peer-capability-cache'
import { ORCHESTRATION_WORKER_WAKE_OBSERVATION_RUNTIME_CAPABILITY } from '../../../../../shared/orchestration/orchestration-runtime-capabilities'
import { structuredSessionHasPendingWake } from './structured-session-pending-wake'
import {
  COORDINATOR,
  PEER_CHAT,
  coordinatorRunAndTask,
  openChat,
  sendUserMessage,
  connectionFor,
  settleTurn,
  host,
  WAIT,
  runtime,
  db
} from '../../terminal-authority/structured-sessions/structured-chat-coordinator-mail-rig.test-fixture'

it('defers for the parent’s running work until it settles, never for another session', async () => {
  await openChat(COORDINATOR)
  await openChat(PEER_CHAT)
  const { taskId, runId } = await coordinatorRunAndTask()
  expect(await structuredSessionHasPendingWake(runtime, COORDINATOR)).toBe(false)
  const started = db.createStartingWorkerDispatch({
    creator: { kind: 'session', orcaSessionId: testOrcaSessionId(COORDINATOR) },
    maxDepth: 10,
    taskId,
    startOptions: {}
  })
  db.prepareStartingWorkerAuthority({
    dispatchId: started.dispatch.id,
    handle: formatOrcaSessionAddress(testOrcaSessionId(PEER_CHAT)),
    paneKey: null,
    processIncarnation: 'chat:peer',
    worktreeId: 'workspace-1',
    effects: [],
    setupState: 'not_applicable'
  })
  db.markWorkerDispatchReady(started.dispatch.id)
  expect(await structuredSessionHasPendingWake(runtime, COORDINATOR)).toBe(false)
  expect(await sendUserMessage(PEER_CHAT, 'Do the delegated work')).toMatchObject({ ok: true })
  await vi.waitFor(() => expect(connectionFor(PEER_CHAT).turns).toHaveLength(1), WAIT)
  expect(await structuredSessionHasPendingWake(runtime, COORDINATOR)).toBe(true)
  expect(await structuredSessionHasPendingWake(runtime, PEER_CHAT)).toBe(false)
  const question = db.createQuestion({
    runId,
    dispatchId: started.dispatch.id,
    askerHandle: formatOrcaSessionAddress(testOrcaSessionId(PEER_CHAT)),
    question: 'What next?'
  })
  expect(await structuredSessionHasPendingWake(runtime, COORDINATOR)).toBe(false)
  db.answerQuestion({
    messageId: question.message.id,
    runId,
    body: 'Continue',
    consumerGeneration: db.getRun(runId)!.consumer_generation
  })
  expect(await structuredSessionHasPendingWake(runtime, COORDINATOR)).toBe(true)
  const helper = db.createStartingWorkerDispatch({
    creator: { kind: 'system' },
    maxDepth: 10,
    taskSpec: 'Nested helper',
    taskRunId: runId,
    startOptions: {}
  })
  db.db
    .prepare('UPDATE dispatch_contexts SET creator_dispatch_id = ? WHERE id = ?')
    .run(started.dispatch.id, helper.dispatch.id)
  expect(await structuredSessionHasPendingWake(runtime, COORDINATOR)).toBe(true)
  db.completeDispatch(helper.dispatch.id)
  expect(await structuredSessionHasPendingWake(runtime, COORDINATOR)).toBe(true)
  await settleTurn(PEER_CHAT, 0)
  // The answer is delivered as a second worker turn.
  await vi.waitFor(() => expect(connectionFor(PEER_CHAT).turns).toHaveLength(2), WAIT)
  await settleTurn(PEER_CHAT, 1)
  // A turn-stop report leaves the Dispatch ready; an idle worker owes no further wake.
  expect(db.getWorkerDispatch(started.dispatch.id)?.state).toBe('ready')
  await vi.waitFor(
    async () => expect(await structuredSessionHasPendingWake(runtime, COORDINATOR)).toBe(false),
    WAIT
  )
  const followUp = db.insertMessage({
    runId,
    from: formatOrcaSessionAddress(testOrcaSessionId(COORDINATOR)),
    to: `dispatch:${started.dispatch.id}`,
    subject: 'Another round'
  })
  db.markAsRead([followUp.id])
  expect(await sendUserMessage(PEER_CHAT, 'Do the next round')).toMatchObject({ ok: true })
  await vi.waitFor(() => expect(connectionFor(PEER_CHAT).turns).toHaveLength(3), WAIT)
  expect(await structuredSessionHasPendingWake(runtime, COORDINATOR)).toBe(true)
  await settleTurn(PEER_CHAT, 2)
  db.completeDispatch(started.dispatch.id)
  expect(await structuredSessionHasPendingWake(runtime, COORDINATOR)).toBe(false)
})

it.each([1_000, 10_000])(
  'bounds the Stop lookup with %i unrelated and settled dispatches',
  async (population) => {
    await openChat(COORDINATOR)
    await openChat(PEER_CHAT)
    const { taskId } = await coordinatorRunAndTask()
    const started = db.createStartingWorkerDispatch({
      creator: { kind: 'session', orcaSessionId: testOrcaSessionId(COORDINATOR) },
      maxDepth: 10,
      taskId,
      startOptions: {}
    })
    db.prepareStartingWorkerAuthority({
      dispatchId: started.dispatch.id,
      handle: formatOrcaSessionAddress(testOrcaSessionId(PEER_CHAT)),
      paneKey: null,
      processIncarnation: 'chat:peer',
      worktreeId: 'workspace-1',
      effects: [],
      setupState: 'not_applicable'
    })
    db.markWorkerDispatchReady(started.dispatch.id)
    const columns = db.db
      .prepare('PRAGMA table_info(dispatch_contexts)')
      .all()
      .map((row) => String(row.name))
    db.db
      .prepare(`WITH RECURSIVE n(i) AS (SELECT 1 UNION ALL SELECT i + 1 FROM n WHERE i < ?)
    INSERT INTO dispatch_contexts (${columns.join(', ')}) SELECT ${columns
      .map((column) =>
        column === 'id'
          ? "'ctx_noise_' || n.i"
          : column === 'creator_orca_session_id'
            ? "CASE WHEN n.i % 2 = 0 THEN 'other-parent' ELSE d.creator_orca_session_id END"
            : column === 'status'
              ? "CASE WHEN n.i % 2 = 0 THEN 'dispatched' ELSE 'completed' END"
              : `d.${column}`
      )
      .join(', ')}
    FROM n CROSS JOIN dispatch_contexts d WHERE d.id = ?`)
      .run(population, started.dispatch.id)
    expect(await sendUserMessage(PEER_CHAT, 'Do the delegated work')).toMatchObject({ ok: true })
    await vi.waitFor(() => expect(connectionFor(PEER_CHAT).turns).toHaveLength(1), WAIT)
    const globalStatuses = vi.spyOn(runtime, 'getOrchestrationFleetAgentStatusSnapshot')
    const runCandidates = vi.spyOn(db, 'listWorkerReportWaitCandidates')
    const status = vi.spyOn(host, 'readStatusSummary')
    const prepare = db.db.prepare.bind(db.db)
    const queries: string[] = []
    const spy = vi.spyOn(db.db, 'prepare').mockImplementation((sql) => {
      queries.push(sql)
      return prepare(sql)
    })
    expect(await structuredSessionHasPendingWake(runtime, COORDINATOR)).toBe(true)
    spy.mockRestore()
    expect(globalStatuses).not.toHaveBeenCalled()
    expect(runCandidates).not.toHaveBeenCalled()
    expect(status).toHaveBeenCalledTimes(2)
    const creatorRead = queries.find((sql) => sql.includes('WHERE creator_orca_session_id = ?'))!
    expect(creatorRead).toBeDefined()
    const plan = prepare(`EXPLAIN QUERY PLAN ${creatorRead}`)
      .all(COORDINATOR, null)
      .map((row) => String(row.detail))
    expect(plan.filter((detail) => detail.startsWith('SCAN'))).toEqual([])
    expect(
      plan.filter((detail) => detail.startsWith('SEARCH') && detail.includes('status=?'))
    ).toHaveLength(2)
  }
)

it('rejects a remote observation after its binding is released', async () => {
  await openChat(COORDINATOR)
  const { taskId } = await coordinatorRunAndTask()
  const started = db.createStartingWorkerDispatch({
    creator: { kind: 'session', orcaSessionId: testOrcaSessionId(COORDINATOR) },
    maxDepth: 10,
    taskId,
    startOptions: {},
    federation: {
      environmentId: 'host',
      environmentName: 'Host',
      peerFingerprint: 'peer',
      protocolVersion: 3
    }
  })
  db.prepareStartingWorkerAuthority({
    dispatchId: started.dispatch.id,
    handle: 'term_remote',
    paneKey: null,
    processIncarnation: 'remote:1',
    worktreeId: 'remote-workspace',
    effects: [],
    setupState: 'not_applicable'
  })
  db.markWorkerDispatchReady(started.dispatch.id)
  db.updateFederatedDispatchResources({
    dispatchId: started.dispatch.id,
    remoteRuntimeEpoch: 'epoch',
    worktreeId: 'remote-workspace',
    terminalHandle: 'term_remote'
  })
  vi.spyOn(runtime, 'resolveOrchestrationWorkerServer').mockResolvedValue({
    environmentId: 'host',
    name: 'Host',
    peerFingerprint: 'peer',
    pairingRevision: 1
  })
  getOrchestrationPeerCapabilityCache(runtime).remember(
    'peer',
    'epoch',
    ORCHESTRATION_WORKER_WAKE_OBSERVATION_RUNTIME_CAPABILITY,
    true
  )
  const observe = vi.spyOn(runtime, 'callOrchestrationWorkerServer').mockResolvedValue({
    items: [{ dispatchId: started.dispatch.id, observation: { exactWorker: true, working: true } }]
  })
  expect(await structuredSessionHasPendingWake(runtime, COORDINATOR)).toBe(true)
  observe.mockImplementation(async () => {
    db.db
      .prepare(
        'UPDATE federated_dispatches SET remote_terminal_handle = NULL WHERE dispatch_id = ?'
      )
      .run(started.dispatch.id)
    return {
      items: [
        { dispatchId: started.dispatch.id, observation: { exactWorker: true, working: true } }
      ]
    }
  })
  expect(await structuredSessionHasPendingWake(runtime, COORDINATOR)).toBe(false)
})

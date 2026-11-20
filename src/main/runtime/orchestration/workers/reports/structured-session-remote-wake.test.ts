import { expect, it, vi } from 'vitest'
import { readRemoteWorkingDispatches } from './structured-session-remote-wake'
import { getOrchestrationPeerCapabilityCache } from '../../federation/orchestration-peer-capability-cache'
import { ORCHESTRATION_WORKER_WAKE_OBSERVATION_RUNTIME_CAPABILITY } from '../../../../../shared/orchestration/orchestration-runtime-capabilities'
import type { FederatedDispatchRow } from '../../db/orchestration-records'
import {
  runtime,
  db
} from '../../terminal-authority/structured-sessions/structured-chat-coordinator-mail-rig.test-fixture'

function remoteWorkers(count: number) {
  const rows: FederatedDispatchRow[] = Array.from({ length: count }, (_, i) => ({
    dispatch_id: `remote-${i}`,
    environment_id: 'host',
    environment_name: 'Host',
    peer_fingerprint: 'peer',
    remote_runtime_epoch: 'epoch',
    protocol_version: 1,
    remote_worktree_id: null,
    remote_terminal_handle: null,
    to_home_imported_sequence: 0,
    to_home_acknowledged_sequence: 0,
    home_auto_release_at: null,
    home_auto_release_state: null,
    created_at: '2026-11-17',
    updated_at: '2026-11-17'
  }))
  vi.spyOn(db, 'listFederatedDispatchesByIds').mockImplementation((ids) =>
    rows.filter((row) => ids.includes(row.dispatch_id))
  )
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
  return rows.map((row) => row.dispatch_id)
}

it.each([
  [100, 1],
  [1_000, 10]
])('batches %i remote workers into %i observations', async (count, calls) => {
  const ids = remoteWorkers(count)
  const call = vi.spyOn(runtime, 'callOrchestrationWorkerServer').mockResolvedValue({ items: [] })
  expect(
    await readRemoteWorkingDispatches(runtime, db.listFederatedDispatchesByIds(ids), () => true)
  ).toEqual(new Set())
  for (const [, method, params] of call.mock.calls) {
    expect(method).toBe('orchestration.federationFleetSnapshot')
    expect(params).toMatchObject({ workingOnly: true })
  }
  expect(call).toHaveBeenCalledTimes(calls)
  expect(db.listFederatedDispatchesByIds).toHaveBeenCalledTimes(1)
})

it('admits no more host work after cancellation and lets a successor progress', async () => {
  const ids = remoteWorkers(1_000)
  let current = true
  const releases: (() => void)[] = []
  const call = vi.spyOn(runtime, 'callOrchestrationWorkerServer').mockImplementation(async () => {
    await new Promise<void>((resolve) => releases.push(resolve))
    return { items: [] }
  })
  const pending = readRemoteWorkingDispatches(
    runtime,
    db.listFederatedDispatchesByIds(ids),
    () => current
  )
  await vi.waitFor(() => expect(releases).toHaveLength(4))
  current = false
  for (const release of releases) {
    release()
  }
  expect(await pending).toEqual(new Set())
  expect(call).toHaveBeenCalledTimes(4)
  call.mockResolvedValue({
    items: [{ dispatchId: 'remote-0', observation: { exactWorker: true, working: true } }]
  })
  expect(
    await readRemoteWorkingDispatches(
      runtime,
      db.listFederatedDispatchesByIds(ids.slice(0, 1)),
      () => true
    )
  ).toEqual(new Set(['remote-0']))
})

it('ends the observation at its total deadline without admitting the remaining pages', async () => {
  vi.useFakeTimers()
  try {
    const ids = remoteWorkers(1_000)
    const releases: (() => void)[] = []
    const call = vi.spyOn(runtime, 'callOrchestrationWorkerServer').mockImplementation(async () => {
      await new Promise<void>((resolve) => releases.push(resolve))
      return { items: [] }
    })
    const pending = readRemoteWorkingDispatches(
      runtime,
      db.listFederatedDispatchesByIds(ids),
      () => true
    )
    await vi.advanceTimersByTimeAsync(5_000)
    expect(await pending).toEqual(new Set())
    expect(call).toHaveBeenCalledTimes(4)
    for (const release of releases) {
      release()
    }
    await vi.advanceTimersByTimeAsync(0)
    expect(call).toHaveBeenCalledTimes(4)
  } finally {
    vi.useRealTimers()
  }
})

it.each([100, 1_000])(
  'returns the first eligible wake among %i workers without waiting on peers',
  async (count) => {
    const ids = remoteWorkers(count)
    const responses: ((value: unknown) => void)[] = []
    const call = vi
      .spyOn(runtime, 'callOrchestrationWorkerServer')
      .mockImplementation(() => new Promise((resolve) => responses.push(resolve)))
    const pending = readRemoteWorkingDispatches(
      runtime,
      db.listFederatedDispatchesByIds(ids),
      () => true
    )
    await vi.waitFor(() => expect(responses.length).toBeGreaterThan(0))
    responses[0]!({
      items: [{ dispatchId: 'remote-0', observation: { exactWorker: true, working: true } }]
    })
    expect(await pending).toEqual(new Set(['remote-0']))
    const admitted = call.mock.calls.length
    for (const resolve of responses.slice(1)) {
      resolve({ items: [] })
    }
    await Promise.allSettled(call.mock.results.map((result) => result.value))
    await new Promise<void>((resolve) => setImmediate(resolve))
    expect(call).toHaveBeenCalledTimes(admitted)
    expect(admitted).toBeLessThanOrEqual(4)
  }
)

it('continues past a remote positive whose custody changed', async () => {
  const ids = remoteWorkers(200)
  vi.spyOn(runtime, 'callOrchestrationWorkerServer').mockImplementation(
    async (_host, _method, params) => {
      // oxlint-disable-next-line typescript/consistent-type-assertions -- SAFETY: this mock receives only federationFleetSnapshot parameters.
      const dispatchId = (params as { dispatchIds: string[] }).dispatchIds[0]!
      return { items: [{ dispatchId, observation: { exactWorker: true, working: true } }] }
    }
  )
  expect(
    await readRemoteWorkingDispatches(
      runtime,
      db.listFederatedDispatchesByIds(ids),
      () => true,
      (id) => id === 'remote-100'
    )
  ).toEqual(new Set(['remote-100']))
})

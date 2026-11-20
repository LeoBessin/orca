import '../../../../shared/remote-runtime/host-store/environment-store-worker-test-fixture'
import * as stageWait from '../managed-orcad/runtime-environment-preparation-wait'
import * as managedTunnel from '../../../managed-server/lifecycle/orcad-managed-tunnel'
import * as serving from '../../../managed-server/lifecycle/orcad-managed-serving-verify'
import * as restoreUpdate from '../../../managed-server/lifecycle/orcad-managed-update-on-restore'
import { getAppEnvironment } from '../../../../shared/app-lifecycle/app-environment'
import { OrcaRuntimeService } from '../../../runtime/orca-runtime'
import { createOrchestrationEnvironmentTransport } from './orchestration-environment-transport'
import { readRemoteWorkingDispatches } from '../../../runtime/orchestration/workers/reports/structured-session-remote-wake'
import { getOrchestrationPeerCapabilityCache } from '../../../runtime/orchestration/federation/orchestration-peer-capability-cache'
import { ORCHESTRATION_WORKER_WAKE_OBSERVATION_RUNTIME_CAPABILITY } from '../../../../shared/orchestration/orchestration-runtime-capabilities'
import type { FederatedDispatchRow } from '../../../runtime/orchestration/db/orchestration-records'
import * as callQueue from './runtime-environment-call-queue'
import * as preparation from './runtime-environment-request-preparation'
import { resetRuntimeEnvironmentStatusOwners } from '../connections/runtime-environment-request-connections'
vi.mock('electron', () => ({ BrowserWindow: { getAllWindows: () => [] } }))
import { mkdtempSync, rmSync } from 'node:fs'
import { tmpdir } from 'node:os'
import { join } from 'node:path'
import { afterEach, beforeEach, describe, expect, it, vi } from 'vitest'
import {
  createSharedControlTestServer,
  closeSharedControlTestServers
} from '../../../../shared/remote-runtime/control/remote-runtime-shared-control-test-server'
import type { sendRemoteRuntimeRequestOnSocket } from '../../../../shared/remote-runtime/requests/remote-runtime-request-socket'
import { encodePairingOffer } from '../../../../shared/remote-runtime/pairing/pairing'
import {
  addEnvironmentFromPairingCode,
  markEnvironmentUsed
} from '../../../../shared/remote-runtime/host-store/runtime-environment-store'

const { sendRemoteRuntimeRequestOnSocketMock, sendRemoteRuntimeSharedControlRequestMock } =
  vi.hoisted(() => ({
    sendRemoteRuntimeRequestOnSocketMock: vi.fn(),
    sendRemoteRuntimeSharedControlRequestMock: vi.fn()
  }))

vi.mock('../../../../shared/remote-runtime/requests/remote-runtime-request-socket', () => ({
  sendRemoteRuntimeRequestOnSocket: sendRemoteRuntimeRequestOnSocketMock
}))

vi.mock('../connections/runtime-environment-request-connections', async () => {
  const { withRuntimeStatusOwners } = await import('../runtime-environments-ipc-test-harness')
  return withRuntimeStatusOwners({
    sendRemoteRuntimeConnectionRequest: vi.fn(),
    sendRemoteRuntimeSharedControlRequest: sendRemoteRuntimeSharedControlRequestMock,
    reconnectRemoteRuntimeSharedControlConnection: vi.fn(),
    ensureRemoteRuntimeSharedControlConnection: vi.fn(),
    pauseRemoteRuntimeSharedControlRetry: vi.fn()
  })
})

import { callRuntimeEnvironment } from './runtime-environment-transport-routing'
import { resetSharedControlSupport } from '../connections/runtime-environment-shared-control-support'

const actual = await vi.importActual<{
  sendRemoteRuntimeRequestOnSocket: typeof sendRemoteRuntimeRequestOnSocket
}>('../../../../shared/remote-runtime/requests/remote-runtime-request-socket')

describe('federated read RPC transport routing', () => {
  let userDataPath: string

  beforeEach(() => {
    userDataPath = mkdtempSync(join(tmpdir(), 'orca-federated-read-routing-'))
    resetSharedControlSupport()
    sendRemoteRuntimeRequestOnSocketMock.mockReset()
    sendRemoteRuntimeSharedControlRequestMock.mockReset()
  })

  afterEach(async () => {
    await closeSharedControlTestServers()
    resetRuntimeEnvironmentStatusOwners()
    rmSync(userDataPath, { recursive: true, force: true })
  })

  it('refuses a runtime-pinned one-shot mutation when its socket reaches a restarted host', async () => {
    const server = await createSharedControlTestServer({
      resultForRequest: (method) =>
        method === 'status.get' ? { runtimeId: 'runtime-restarted', capabilities: [] } : {}
    })
    const paired = await addEnvironmentFromPairingCode(userDataPath, {
      name: 'Restarted host',
      pairingCode: encodePairingOffer(server.offer)
    })
    await markEnvironmentUsed(userDataPath, paired.id, { runtimeId: 'runtime-proven' })
    sendRemoteRuntimeRequestOnSocketMock.mockImplementation(actual.sendRemoteRuntimeRequestOnSocket)
    await expect(
      callRuntimeEnvironment(
        userDataPath,
        paired.id,
        'orchestration.federationRelease',
        { dispatchId: 'dispatch-1' },
        15_000,
        undefined,
        { orchestrationContractVersion: 1, orchestrationRequestId: 'release-1' },
        { expectedEnvironmentRuntimeId: 'runtime-proven' }
      )
    ).resolves.toMatchObject({ ok: false, error: { code: 'runtime_environment_changed' } })
    expect(
      server.requests.filter((request) => request.method === 'orchestration.federationRelease')
    ).toEqual([])
  })
  it.each([20, 200])(
    'drops %i abandoned observations before host preparation and frees the queue for successors',
    async (count) => {
      const server = await createSharedControlTestServer({ resultForRequest: () => ({}) })
      const paired = await addEnvironmentFromPairingCode(userDataPath, {
        name: 'Queue host',
        pairingCode: encodePairingOffer(server.offer)
      })
      const app = getAppEnvironment()
      const originalPath = app.getPath.bind(app)
      const paths = vi
        .spyOn(app, 'getPath')
        .mockImplementation((name) => (name === 'userData' ? userDataPath : originalPath(name)))
      const runtime = new OrcaRuntimeService(null, undefined, {
        orchestrationEnvironmentTransport: createOrchestrationEnvironmentTransport()
      })
      const host = await runtime.resolveOrchestrationWorkerServer(paired.id)
      getOrchestrationPeerCapabilityCache(runtime).remember(
        host.peerFingerprint,
        'epoch',
        ORCHESTRATION_WORKER_WAKE_OBSERVATION_RUNTIME_CAPABILITY,
        true
      )
      let release = () => {}
      const held = new Promise<void>((resolve) => {
        release = resolve
      })
      const blockers = Array.from({ length: 8 }, () =>
        callQueue.enqueueRuntimeCall(paired.id, 'terminal.send', () => held)
      )
      const queued = vi.spyOn(callQueue, 'enqueueRuntimeCall')
      const prepare = vi.spyOn(preparation, 'prepareFencedRuntimeRequest')
      const controllers = Array.from({ length: count }, () => new AbortController())
      const observations = controllers.map((controller, index) => {
        const row: FederatedDispatchRow = {
          dispatch_id: `queued-${index}`,
          environment_id: paired.id,
          environment_name: 'Queue host',
          peer_fingerprint: host.peerFingerprint,
          remote_runtime_epoch: 'epoch',
          protocol_version: 3,
          remote_worktree_id: null,
          remote_terminal_handle: null,
          to_home_imported_sequence: 0,
          to_home_acknowledged_sequence: 0,
          home_auto_release_at: null,
          home_auto_release_state: null,
          created_at: '2026-11-19',
          updated_at: '2026-11-19'
        }
        return readRemoteWorkingDispatches(
          runtime,
          [row],
          () => !controller.signal.aborted,
          () => true,
          controller.signal
        )
      })
      let successors: Promise<string>[] = []
      try {
        await vi.waitFor(
          () =>
            expect(
              queued.mock.calls.filter(
                ([, method]) => method === 'orchestration.federationFleetSnapshot'
              )
            ).toHaveLength(count),
          { timeout: 10_000 }
        )
        for (const controller of controllers) {
          controller.abort()
        }
        expect(await Promise.all(observations)).toEqual(
          Array.from({ length: count }, () => new Set())
        )
        successors = Array.from({ length: 256 }, () =>
          callQueue.enqueueRuntimeCall(paired.id, 'terminal.send', async () => 'fresh')
        )
        const outcomes = Promise.allSettled(successors)
        release()
        await Promise.all(blockers)
        expect(
          (await outcomes).every(
            (result) => result.status === 'fulfilled' && result.value === 'fresh'
          )
        ).toBe(true)
        expect(prepare).not.toHaveBeenCalled()
        sendRemoteRuntimeRequestOnSocketMock.mockResolvedValue({
          ok: true,
          result: { items: [] },
          _meta: { runtimeId: 'queue-host' }
        })
        sendRemoteRuntimeSharedControlRequestMock.mockResolvedValue({
          ok: true,
          result: { items: [] }
        })
        await expect(
          runtime.callOrchestrationWorkerServer(
            paired.id,
            'orchestration.federationFleetSnapshot',
            { dispatchIds: ['fresh'], workingOnly: true }
          )
        ).resolves.toEqual({ items: [] })
        expect(prepare).toHaveBeenCalledTimes(1)
      } finally {
        for (const controller of controllers) {
          controller.abort()
        }
        release()
        await Promise.allSettled([...blockers, ...successors, ...observations])
        runtime.stopOrchestrationFederationRelay()
        paths.mockRestore()
        prepare.mockRestore()
        queued.mockRestore()
      }
    }
  )
  it.each([20, 200])(
    'detaches %i started callers from one shared host preparation without admitting stale stages',
    async (count) => {
      const server = await createSharedControlTestServer({ resultForRequest: () => ({}) })
      const paired = await addEnvironmentFromPairingCode(userDataPath, {
        name: 'Started host',
        pairingCode: encodePairingOffer(server.offer)
      })
      let release = () => {}
      const held = new Promise<void>((resolve) => {
        release = resolve
      })
      const ensure = vi
        .spyOn(managedTunnel, 'ensureOrcadManagedTunnel')
        .mockImplementation(async () => {
          await held
          return paired
        })
      const verify = vi
        .spyOn(serving, 'verifyOrcadManagedServing')
        .mockResolvedValue({ state: 'started', boundPort: null })
      const update = vi.spyOn(restoreUpdate, 'updateManagedOrcadOnRestore').mockReturnValue(null)
      const redeploy = vi.spyOn(restoreUpdate, 'redeployStoppedManagedOrcad')
      const wait = vi.spyOn(stageWait, 'waitForRuntimePreparation')
      const controllers: AbortController[] = []
      const cancelled: Promise<unknown>[] = []
      let live: Promise<unknown> | undefined
      let fresh: Promise<string> | undefined
      try {
        for (let start = 0; start < count; start += 8) {
          const wave = Array.from(
            { length: Math.min(8, count - start) },
            () => new AbortController()
          )
          controllers.push(...wave)
          const pending = wave.map((controller) =>
            callRuntimeEnvironment(
              userDataPath,
              paired.id,
              'orchestration.federationFleetSnapshot',
              { dispatchIds: ['worker'], workingOnly: true },
              3_000,
              undefined,
              undefined,
              { signal: controller.signal }
            ).then(
              () => 'sent',
              (error: unknown) => error
            )
          )
          cancelled.push(...pending)
          await vi.waitFor(
            () =>
              expect(wait.mock.calls.filter(([, , stage]) => stage === 'tunnel')).toHaveLength(
                start + wave.length
              ),
            { timeout: 10_000 }
          )
          for (const controller of wave) {
            controller.abort()
          }
          expect(
            (await Promise.all(pending)).every(
              (result) => result instanceof Error && result.name === 'AbortError'
            )
          ).toBe(true)
        }
        expect(ensure).toHaveBeenCalledTimes(1)
        expect(verify).not.toHaveBeenCalled()
        expect(redeploy).not.toHaveBeenCalled()
        fresh = callQueue.enqueueRuntimeCall(paired.id, 'terminal.send', async () => 'fresh')
        expect(await fresh).toBe('fresh')
        const survivor = new AbortController()
        controllers.push(survivor)
        sendRemoteRuntimeRequestOnSocketMock.mockResolvedValue({
          ok: true,
          result: { items: [] },
          _meta: { runtimeId: 'started-host' }
        })
        sendRemoteRuntimeSharedControlRequestMock.mockResolvedValue({
          ok: true,
          result: { items: [] }
        })
        live = callRuntimeEnvironment(
          userDataPath,
          paired.id,
          'orchestration.federationFleetSnapshot',
          { dispatchIds: ['live'], workingOnly: true },
          3_000,
          undefined,
          undefined,
          { signal: survivor.signal }
        )
        await vi.waitFor(() =>
          expect(wait.mock.calls.filter(([, , stage]) => stage === 'tunnel')).toHaveLength(
            count + 1
          )
        )
        expect(ensure).toHaveBeenCalledTimes(1)
        release()
        await expect(live).resolves.toMatchObject({ ok: true, result: { items: [] } })
        expect(verify).toHaveBeenCalledTimes(1)
        expect(update).toHaveBeenCalledTimes(1)
        expect(redeploy).not.toHaveBeenCalled()
      } finally {
        for (const controller of controllers) {
          controller.abort()
        }
        release()
        await Promise.allSettled([...cancelled, ...(live ? [live] : []), ...(fresh ? [fresh] : [])])
        ensure.mockRestore()
        verify.mockRestore()
        update.mockRestore()
        redeploy.mockRestore()
        wait.mockRestore()
      }
    }
  )
})

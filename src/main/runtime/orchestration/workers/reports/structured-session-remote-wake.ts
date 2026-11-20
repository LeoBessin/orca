import { withTimeout } from '../../../../../shared/concurrency/promise-timeout-fallback'
import type { OrcaRuntimeService } from '../../../orca-runtime'
import type { FederatedDispatchRow } from '../../db/orchestration-records'
import type { RuntimeStatus } from '../../../../../shared/remote-runtime/session-graph/runtime-session-contracts'
import { ORCHESTRATION_WORKER_WAKE_OBSERVATION_RUNTIME_CAPABILITY } from '../../../../../shared/orchestration/orchestration-runtime-capabilities'
import { mapWithConcurrency } from '../../../../../shared/concurrency/map-with-concurrency'
import { groupFederatedDispatchRows } from '../../legacy-remote-home/federated-fleet-host-groups'
import { resolvePinnedFederatedServer } from '../../legacy-remote-home/pinned-federated-server'
import { getOrchestrationPeerCapabilityCache } from '../../federation/orchestration-peer-capability-cache'

/** One bounded observation per host/page, with no terminal previews or worktree inventory. */
export async function readRemoteWorkingDispatches(
  runtime: Pick<
    OrcaRuntimeService,
    'resolveOrchestrationWorkerServer' | 'callOrchestrationWorkerServer'
  >,
  dispatches: readonly FederatedDispatchRow[],
  isCurrent: () => boolean,
  eligible: (dispatchId: string) => boolean = () => true,
  sourceSignal?: AbortSignal
): Promise<Set<string>> {
  const controller = new AbortController()
  const signal = sourceSignal
    ? AbortSignal.any([sourceSignal, controller.signal])
    : controller.signal
  let onAbort: () => void = () => {}
  const aborted = new Promise<void>((resolve) => {
    onAbort = resolve
    signal.addEventListener('abort', onAbort, { once: true })
    if (signal.aborted) {
      resolve()
    }
  })
  const deadline = Date.now() + 5_000
  const working = new Set<string>()
  let found: (() => void) | undefined
  const positive = new Promise<void>((resolve) => {
    found = resolve
  })
  const current = () =>
    !signal.aborted && working.size === 0 && isCurrent() && Date.now() < deadline
  const observations = mapWithConcurrency(
    groupFederatedDispatchRows(dispatches),
    4,
    async (group) => {
      if (!current()) {
        return
      }
      try {
        const first = group.dispatches[0]!
        const server = await resolvePinnedFederatedServer(runtime, first)
        if (!current()) {
          return
        }
        const support = await getOrchestrationPeerCapabilityCache(runtime).resolve({
          peerFingerprint: first.peer_fingerprint,
          expectedRuntimeEpoch: first.remote_runtime_epoch,
          capability: ORCHESTRATION_WORKER_WAKE_OBSERVATION_RUNTIME_CAPABILITY,
          probe: async () => {
            const status = await runtime.callOrchestrationWorkerServer(
              server.environmentId,
              'status.get',
              undefined,
              Math.min(3_000, Math.max(1, deadline - Date.now())),
              undefined,
              { expectedEnvironmentPairingRevision: server.pairingRevision }
            )
            // oxlint-disable-next-line typescript/consistent-type-assertions -- SAFETY: status.get returns the paired host's RuntimeStatus protocol.
            return status as RuntimeStatus
          }
        })
        if (!support.supported || !current()) {
          return
        }
        const ids = group.dispatches.map((dispatch) => dispatch.dispatch_id)
        const observed = await runtime.callOrchestrationWorkerServer(
          server.environmentId,
          'orchestration.federationFleetSnapshot',
          { dispatchIds: ids, workingOnly: true },
          Math.min(3_000, Math.max(1, deadline - Date.now())),
          undefined,
          {
            expectedEnvironmentPairingRevision: server.pairingRevision,
            signal,
            prepareGuard: async () => {
              if (!current()) {
                throw new Error('Worker observation is no longer current.')
              }
              return () => {
                if (!current()) {
                  throw new Error('Worker observation is no longer current.')
                }
                return true
              }
            }
          }
        )
        if (!current()) {
          return
        }
        // oxlint-disable-next-line typescript/consistent-type-assertions -- SAFETY: the capability gates the paired host's batched activity response.
        const result = observed as {
          items: { dispatchId: string; observation: { exactWorker: boolean; working?: boolean } }[]
        }
        const expected = new Set(ids)
        for (const item of result.items) {
          if (
            expected.has(item.dispatchId) &&
            item.observation.exactWorker &&
            item.observation.working &&
            eligible(item.dispatchId)
          ) {
            working.add(item.dispatchId)
            found?.()
            return
          }
        }
      } catch {
        // One unavailable host cannot hide another host's report.
      }
    }
  )
  try {
    await withTimeout(
      Promise.race([observations, positive, aborted]),
      Math.max(1, deadline - Date.now()),
      undefined
    )
    return !sourceSignal?.aborted && isCurrent() ? working : new Set()
  } finally {
    signal.removeEventListener('abort', onAbort)
    controller.abort()
  }
}

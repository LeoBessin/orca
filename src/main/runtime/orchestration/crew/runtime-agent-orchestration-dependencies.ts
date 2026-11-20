import { prepareCoordinatorMailCount } from '../db/messages/prepared-coordinator-mail-count'
import {
  canPromiseWorkerFinishReport,
  canPromiseHelperFinishReport
} from '../workers/turn-stops/worker-finish-report-eligibility'
import type {
  WorkerTurnStopDispatch,
  WorkerTurnStopReportDeps
} from '../workers/turn-stops/worker-turn-stop-report'
import type { DispatchContextRow } from '../db/orchestration-records'
import { selectFreshExplicitAgentStatus } from '../../workspaces/process-listing/runtime-hook-agent-row-selection'
import { ORCA_LAYOUT_OWNER_IS_MAIN } from '../../../../shared/workspace-layout/layout-owner-switch'
import { captureOrchestrationTerminalCandidates } from './orchestration-terminal-candidates'
import {
  mintAgentStatusFleetEvidence,
  type ObservedAgentStatusPaneIdentity
} from '../../../ipc/agent-hooks/agent-status-ipc-boundary'
import { readAgentOrchestrationProjectionRecords } from '../addressing/orchestration-fleet-agent-status-snapshot'
import type { RuntimeAgentOrchestrationDependencies } from './runtime-agent-orchestration-projection'
import { OrcaRuntimeWithRuntimeId } from '../../construction/runtime-identity-state'
import type { AgentStatusOrchestrationContext } from '../../../../shared/agent-status/contracts/agent-status-types'

export abstract class OrcaRuntimeWithAgentOrchestrationDependencies extends OrcaRuntimeWithRuntimeId {
  protected abstract readonly workerTurnStopReportDeps: WorkerTurnStopReportDeps

  async canPromiseOrchestrationWorkerWake(
    target: WorkerTurnStopDispatch,
    current: () => boolean = () => true
  ): Promise<boolean> {
    const db = this.getOrchestrationDbIfAvailable()
    if (!db || !current()) {
      return false
    }
    if (target.kind === 'local') {
      return canPromiseWorkerFinishReport(db, target, this.workerTurnStopReportDeps)
    }
    const attachment = target.attachment
    const live = () => {
      if (!current()) {
        return false
      }
      const row = db.getRemoteDispatchAttachment(attachment.dispatch_id)
      return (
        !!row &&
        row.state === 'ready' &&
        row.runtime_epoch === attachment.runtime_epoch &&
        row.home_peer_fingerprint === attachment.home_peer_fingerprint &&
        row.pane_key === attachment.pane_key &&
        row.process_incarnation === attachment.process_incarnation &&
        row.consumer_generation === attachment.consumer_generation
      )
    }
    if (!live()) {
      return false
    }
    await prepareCoordinatorMailCount(db, attachment.dispatch_id, live)
    return live() && canPromiseWorkerFinishReport(db, target, this.workerTurnStopReportDeps)
  }

  async canPromiseOrchestrationHelperReport(
    dispatch: DispatchContextRow,
    current: () => boolean
  ): Promise<boolean> {
    const db = this.getOrchestrationDbIfAvailable()
    return (
      !!db && canPromiseHelperFinishReport(db, dispatch, this.workerTurnStopReportDeps, current)
    )
  }

  abstract getAgentStatusTerminalHandleForPaneKey(paneKey: string): string | undefined
  abstract getAgentStatusOrchestrationContextForPaneKey(
    paneKey: string
  ): AgentStatusOrchestrationContext | undefined
  abstract readonly readObservedAgentStatusPaneIdentityFn: (
    paneKey: string
  ) => ObservedAgentStatusPaneIdentity

  isOrchestrationWorkerWorking(paneKey: string, processIncarnation: string): boolean {
    const handle = this.getAgentStatusTerminalHandleForPaneKey(paneKey)
    const observed = this.readObservedAgentStatusPaneIdentityFn(paneKey)
    if (
      !handle ||
      this.getTerminalProcessIncarnation(handle) !== processIncarnation ||
      observed.kind !== 'observed' ||
      observed.processIncarnation !== processIncarnation ||
      observed.terminalHandle !== handle
    ) {
      return false
    }
    return (
      selectFreshExplicitAgentStatus({
        handle,
        paneKey,
        hookRows: (this.publicPaneStatusFn?.(paneKey, handle) ?? []).filter(
          (row) => !row.terminalHandle || row.terminalHandle === handle
        )
      })?.status === 'working'
    )
  }

  protected agentOrchestrationDependencies(bound?: Partial<RuntimeAgentOrchestrationDependencies>) {
    return {
      captureTerminalCandidates: () =>
        captureOrchestrationTerminalCandidates({
          leaves: this.leaves,
          ptys: this.ptysById,
          leafKey: (tab, leaf) => this.getLeafKey(tab, leaf),
          leavesForId: (id) => this.terminalIdentity.paneCandidates.leavesForId(id),
          capturePaneOf: () => this.captureTerminalPaneReader(),
          ...(!ORCA_LAYOUT_OWNER_IS_MAIN
            ? {
                ptysForPane: (pane: string) =>
                  this.terminalIdentity.paneCandidates.ptysForPane(pane),
                paneOf: (pty: Parameters<typeof this.paneOfPty>[0]) => this.paneOfPty(pty)
              }
            : {}),
          makePaneKey: (leaf) => this.makeRuntimePaneKey(leaf),
          issueLeaf: (leaf) => this.issueHandle(leaf),
          issuePty: (pty) => this.issuePtyHandle(pty)
        }),
      getDb: () => this.getOrchestrationDbIfAvailable(),
      getLeaves: () => this.leaves.values(),
      getPtys: () => this.ptysById.values(),
      paneOf: (pty) => this.paneOfPty(pty),
      issueLeafHandle: (leaf) => this.issueHandle(leaf),
      issuePtyHandle: (pty) => this.issuePtyHandle(pty),
      makePaneKey: (leaf) => this.makeRuntimePaneKey(leaf),
      getWorktreeId: (handle) => this.getTerminalWorktreeIdForHandle(handle),
      getHandleForPaneKey: (paneKey) => this.getTerminalHandleForPaneKey(paneKey),
      getPaneKey: (handle) => this.getPaneKeyForTerminalHandle(handle),
      getDispatchAuthority: (handle) => this.getOrchestrationDispatchAuthority(handle),
      getAgentStatusSnapshot: () => this.getOrchestrationFleetAgentStatusSnapshot(),
      getAgentStatusForPaneKey: (paneKey, contextForPane) =>
        (this.publicPaneStatusFn?.(paneKey) ?? []).map((entry) =>
          mintAgentStatusFleetEvidence(
            entry,
            {
              getAgentStatusTerminalHandleForPaneKey: (pane) =>
                this.getAgentStatusTerminalHandleForPaneKey(pane),
              getTerminalProcessIncarnation: (handle) => this.getTerminalProcessIncarnation(handle),
              getAgentStatusOrchestrationContextForPaneKey:
                contextForPane ??
                ((pane) => this.getAgentStatusOrchestrationContextForPaneKey(pane))
            },
            this.readObservedAgentStatusPaneIdentityFn(paneKey)
          )
        ),
      getSessionRecords: () => readAgentOrchestrationProjectionRecords(),
      ...bound
    } satisfies RuntimeAgentOrchestrationDependencies
  }
}

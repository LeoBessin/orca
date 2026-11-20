import type { ResolvedWorktree } from '../../../workspaces/catalog/runtime-worktree-path-identity'
import { expect, it, vi } from 'vitest'
import { OrcaRuntimeService } from '../../../orca-runtime'
import type { AgentStatusIpcPayload } from '../../../../../shared/agent-status/contracts/agent-status-ipc-payload'
import type { ObservedAgentStatusPaneIdentity } from '../../../../ipc/agent-hooks/agent-status-ipc-boundary'
import { structuredSessionHasPendingWake } from './structured-session-pending-wake'
import { testOrcaSessionId } from '../../../../../shared/agent-session/identity/orca-session-address-test-fixture'
import {
  COORDINATOR,
  coordinatorRunAndTask,
  openChat,
  db
} from '../../terminal-authority/structured-sessions/structured-chat-coordinator-mail-rig.test-fixture'

it.each([750, 7_500])(
  'uses exact hook evidence without a summary or inventory across %i worktrees',
  async (population) => {
    const now = 1_800_000_000_000
    vi.spyOn(Date, 'now').mockReturnValue(now)
    await openChat(COORDINATOR)
    const { taskId } = await coordinatorRunAndTask()
    const pane = 'tab_worker:bbbbbbbb-bbbb-4bbb-8bbb-bbbbbbbbbbbb'
    let rows: AgentStatusIpcPayload[] = [
      {
        paneKey: pane,
        state: 'working',
        prompt: '',
        connectionId: null,
        agentType: 'claude',
        terminalHandle: 'term_reminted',
        receivedAt: now,
        stateStartedAt: now
      }
    ]
    let observed: ObservedAgentStatusPaneIdentity = {
      kind: 'observed',
      terminalHandle: 'term_reminted',
      processIncarnation: 'worker:1',
      dispatchId: null
    }
    const worktreeIds = Array.from({ length: population }, (_, i) => `workspace-${i}`)
    let inventoryVisits = 0
    class WakeRuntime extends OrcaRuntimeService {
      protected override async getResolvedWorktreeMap(): Promise<Map<string, ResolvedWorktree>> {
        for (const _id of worktreeIds) {
          inventoryVisits += 1
        }
        return new Map()
      }
    }
    const runtime = new WakeRuntime(null, undefined, {
      getPublicAgentStatusSnapshotForPane: (key) => (key === pane ? rows : []),
      readObservedAgentStatusPaneIdentity: () => observed
    })
    runtime.setOrchestrationDb(db)
    vi.spyOn(runtime, 'getAgentStatusTerminalHandleForPaneKey').mockReturnValue('term_reminted')
    vi.spyOn(runtime, 'getTerminalProcessIncarnation').mockReturnValue('worker:1')
    const summary = vi.spyOn(runtime, 'showTerminal')
    const started = db.createStartingWorkerDispatch({
      creator: { kind: 'session', orcaSessionId: testOrcaSessionId(COORDINATOR) },
      maxDepth: 10,
      taskId,
      startOptions: {}
    })
    db.prepareStartingWorkerAuthority({
      dispatchId: started.dispatch.id,
      handle: 'term_old',
      paneKey: pane,
      processIncarnation: 'worker:1',
      worktreeId: 'workspace-1',
      effects: [],
      setupState: 'not_applicable'
    })
    db.markWorkerDispatchReady(started.dispatch.id)
    expect(await structuredSessionHasPendingWake(runtime, COORDINATOR)).toBe(true)
    rows[0]!.terminalHandle = 'term_previous_worker'
    expect(await structuredSessionHasPendingWake(runtime, COORDINATOR)).toBe(false)
    rows = []
    expect(await structuredSessionHasPendingWake(runtime, COORDINATOR)).toBe(false)
    rows = [
      {
        paneKey: pane,
        state: 'working',
        prompt: '',
        connectionId: null,
        agentType: 'claude',
        receivedAt: now,
        stateStartedAt: now,
        restoredUnconfirmed: true
      }
    ]
    expect(await structuredSessionHasPendingWake(runtime, COORDINATOR)).toBe(false)
    rows[0]!.restoredUnconfirmed = false
    expect(await structuredSessionHasPendingWake(runtime, COORDINATOR)).toBe(true)
    observed = {
      kind: 'observed',
      terminalHandle: 'term_reminted',
      processIncarnation: 'worker:replacement',
      dispatchId: null
    }
    expect(await structuredSessionHasPendingWake(runtime, COORDINATOR)).toBe(false)
    expect(summary).not.toHaveBeenCalled()
    expect(inventoryVisits).toBe(0)
  }
)

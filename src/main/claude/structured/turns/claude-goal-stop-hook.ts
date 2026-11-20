import type { HookCallback } from '@anthropic-ai/claude-agent-sdk'
import type { ClaudeSession } from '../claude-structured-session-state'

export function claudeGoalStopHook(
  sessionId: string,
  currentSession: () => ClaudeSession | null,
  hasPendingWake:
    | ((sessionId: string, isCurrent?: () => boolean, signal?: AbortSignal) => Promise<boolean>)
    | undefined
): HookCallback {
  return async (hook, _toolUseId, { signal }) => {
    const session = currentSession()
    const current = () =>
      !signal.aborted &&
      session !== null &&
      currentSession() === session &&
      hook.session_id === session.providerSessionId &&
      session.translator?.goal.current?.status === 'active'
    if (hook.hook_event_name !== 'Stop' || !current()) {
      return {}
    }
    try {
      if (!(await hasPendingWake?.(sessionId, current, signal)) || !current()) {
        return {}
      }
      session?.translator?.goal.deferForWake()
      return { continue: false }
    } catch {
      // Unverifiable work cannot suppress the provider's goal check.
      return {}
    }
  }
}

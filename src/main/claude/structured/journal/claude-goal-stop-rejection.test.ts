import { expect, it } from 'vitest'
import type { AgentJournalItemBody } from '../../../../shared/agent-session/journal/agent-session-journal-types'
import { createClaudeJournalTranslator } from './claude-structured-journal-translation'

it.each([
  { continued: false, otherHook: false, expectedStatus: 'active' },
  { continued: true, otherHook: false, expectedStatus: 'complete' },
  { continued: true, otherHook: true, expectedStatus: 'complete' },
  { continued: false, otherHook: false, expectedStatus: 'active', long: true },
  { continued: true, otherHook: false, expectedStatus: 'complete', idle: true },
  { continued: false, otherHook: false, expectedStatus: 'active', resumed: true }
])(
  'settles rejected goals after model continuation: $continued, other hook rejection: $otherHook',
  ({ continued, otherHook, expectedStatus, long, idle, resumed }) => {
    const objective = long ? 'a'.repeat(510) : 'Finish the worker task'
    const rows: AgentJournalItemBody[] = []
    const translator = createClaudeJournalTranslator({
      goalBackgroundWork: () => 'exited',
      sink: {
        appendItem: (_identity, body) => rows.push(body),
        appendTombstone: () => {},
        publish: () => {}
      }
    })
    let sequence = 0
    const frame = (message: Record<string, unknown>) =>
      translator.handle({
        type: 'message',
        sessionId: 'orca-session',
        observedAt: 1_000,
        message: {
          session_id: 'claude-session',
          parent_tool_use_id: null,
          uuid: `frame-${++sequence}`,
          ...message
        }
      })
    const answer = (text: string, model = 'claude-sonnet') =>
      frame({
        type: 'assistant',
        message: { role: 'assistant', model, content: [{ type: 'text', text }] }
      })
    try {
      answer(`Goal set: ${objective}`, '<synthetic>')
      answer('Still waiting for the worker report.')
      frame({
        type: 'user',
        isSynthetic: true,
        message: {
          role: 'user',
          content: [
            {
              type: 'text',
              text: long
                ? `Stop hook feedback:\n[${'a'.repeat(500)}… [+10 chars]]: The worker has not reported.`
                : 'Stop hook feedback:\n[Finish the worker task]: The worker has not reported.'
            }
          ]
        }
      })
      if (resumed) {
        answer(`Goal active: ${objective} (not yet evaluated)`, '<synthetic>')
      }
      if (idle) {
        translator.goal.deferForWake()
        frame({ type: 'system', subtype: 'session_state_changed', state: 'idle' })
      }
      if (idle) {
        translator.goal.deferForWake()
        frame({ type: 'system', subtype: 'session_state_changed', state: 'idle' })
      }
      if (resumed) {
        answer(`Goal active: ${objective} (not yet evaluated)`, '<synthetic>')
      }
      if (continued) {
        answer('The worker reported and the task is finished.')
      }
      if (otherHook) {
        frame({
          type: 'user',
          isSynthetic: true,
          message: {
            role: 'user',
            content: [{ type: 'text', text: 'Stop hook feedback:\n[./verify-output]: Try again.' }]
          }
        })
      }
      frame({ type: 'result', subtype: 'success', terminal_reason: 'completed', num_turns: 2 })

      expect(
        rows.filter((row) => row.kind === 'status' && row.presentation === 'hook-sent-back').length
      ).toBe(otherHook ? 1 : 0)
      expect(
        rows.findLast((row) => row.kind === 'status' && row.threadGoal !== undefined)
      ).toMatchObject({
        kind: 'status',
        threadGoal: {
          state: 'set',
          goal: {
            objective,
            status: expectedStatus,
            checks: 1,
            lastCheck: 'The worker has not reported.'
          }
        }
      })
    } finally {
      translator.dispose()
    }
  }
)

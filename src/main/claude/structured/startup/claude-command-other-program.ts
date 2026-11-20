import {
  providerDiagnostic,
  withProviderDiagnostic
} from '../../../../shared/agent-session/failures/agent-session-failure'
import '../../../native-chat/agent-session-wire/providers/structured-agent-session-adapter'
import { AgentSessionAcquisitionRefusal } from '../../../native-chat/agent-session-wire/providers/structured-agent-session-adapter-errors'
import {
  providerExitObserved,
  startupAcquisitionRefusal
} from '../../../native-chat/agent-session-wire/startup/structured-agent-session-failure-text'
import type { ClaudeStructuredLaunch } from '../launch/claude-structured-launch-resolution'
import type { ClaudeChildEnvSources } from '../launch/claude-structured-child-env'
import { answersAsOtherProgram } from '../../cli-capabilities/claude-hook-event-versions'
import { CLAUDE_CLI_FLAG_PROBE_BUDGET_MS } from '../../cli-capabilities/claude-cli-flag-support'

/** Only a Command setting naming a bare program is checked: arguments mean a launcher such as
 *  `npx claude`, which may answer --version itself. One answer serves every reader of a failed start. */
export function otherProgramCheck(
  sources: Pick<ClaudeChildEnvSources, 'command' | 'prefixArgs' | 'configured'>,
  probe: { cwd: string; env: Record<string, string> }
): Pick<ClaudeStructuredLaunch, 'runsOtherProgram'> {
  if (!sources.configured || sources.prefixArgs.length > 0) {
    return {}
  }
  let answer: Promise<boolean> | undefined
  return {
    runsOtherProgram: () =>
      (answer ??= answersAsOtherProgram(sources.command, {
        ...probe,
        timeoutMs: CLAUDE_CLI_FLAG_PROBE_BUDGET_MS
      }))
  }
}

/** A start that exited under a Command setting naming another program (`cc` is clang outside the
 *  user's shell) says which program ran, instead of only that Claude stopped. Checked only after
 *  the failure, so a working wrapper is never refused. */
export async function explainOtherProgramStart(
  error: unknown,
  launch:
    | Pick<ClaudeStructuredLaunch, 'pathToClaudeCodeExecutable' | 'runsOtherProgram'>
    | undefined
): Promise<unknown> {
  if (
    !launch?.runsOtherProgram ||
    startupAcquisitionRefusal(error) ||
    !providerExitObserved(error) ||
    !(await launch.runsOtherProgram())
  ) {
    return error
  }
  const program = launch.pathToClaudeCodeExecutable
  return withProviderDiagnostic(
    Object.assign(
      new AgentSessionAcquisitionRefusal(
        `the claude Command setting runs ${program}, not Claude Code`,
        'agentCommandNotRunnable'
      ),
      { cause: error }
    ),
    providerDiagnostic(program, 'person')
  )
}

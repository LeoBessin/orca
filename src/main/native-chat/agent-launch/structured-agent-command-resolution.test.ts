import { chmodSync, mkdtempSync, rmSync, writeFileSync } from 'node:fs'
import { tmpdir } from 'node:os'
import { join } from 'node:path'
import { afterEach, beforeEach, describe, expect, it } from 'vitest'
import { resolveStructuredAgentCommand } from './structured-agent-command-resolution'
import { answersAsOtherProgram } from '../../claude/cli-capabilities/claude-hook-event-versions'
import { explainOtherProgramStart } from '../../claude/structured/startup/claude-command-other-program'
import {
  providerStartupFailureFact,
  withObservedProviderExit
} from '../agent-session-wire/startup/structured-agent-session-failure-text'

describe.skipIf(process.platform === 'win32')('a custom Claude Command', () => {
  let bin: string

  const script = (name: string, body: string): string => {
    const path = join(bin, name)
    writeFileSync(path, `#!/bin/sh\n${body}\n`)
    chmodSync(path, 0o755)
    return path
  }
  const resolve = (command: string, aliases: Record<string, string> = {}) =>
    resolveStructuredAgentCommand(
      'claude',
      { agentCmdOverrides: { claude: command } },
      { pathEnv: bin, platform: process.platform, shellAliases: new Map(Object.entries(aliases)) }
    )

  beforeEach(() => {
    bin = mkdtempSync(join(tmpdir(), 'orca-agent-command-'))
    script('claude', 'echo "2.1.300 (Claude Code)"')
    script('cc', 'echo "Apple clang version 21.0.0"')
  })

  afterEach(() => {
    rmSync(bin, { recursive: true, force: true })
  })

  it('runs what a shell alias names, as the terminal would', () => {
    expect(resolve('cc', { cc: 'claude' })).toEqual({
      command: join(bin, 'claude'),
      prefixArgs: [],
      configured: true
    })
  })

  it("puts an alias's own arguments before the setting's, through nested aliases", () => {
    expect(resolve('cc --verbose', { cc: 'c2 --model opus', c2: 'claude' })).toEqual({
      command: join(bin, 'claude'),
      prefixArgs: ['--model', 'opus', '--verbose'],
      configured: true
    })
  })

  it('keeps a self-named alias, a shell function and an unrunnable alias literal', () => {
    expect(resolve('claude', { claude: 'claude --flag' }).prefixArgs).toEqual([])
    // Functions are not aliases: the program on PATH runs, as before.
    expect(resolve('cc').command).toBe(join(bin, 'cc'))
    expect(resolve('cc', { cc: 'claude | tee log' }).command).toBe(join(bin, 'cc'))
  })

  it('runs an absolute path as written', () => {
    const wrapper = script('wrapper', 'exec claude "$@"')
    expect(resolve(wrapper, { [wrapper]: 'cc' }).command).toBe(wrapper)
  })

  it('refuses a missing program, aliased or not', () => {
    expect(() => resolve('missing')).toThrow(/not a runnable program/)
    expect(() => resolve('cc', { cc: 'missing' })).toThrow(/not a runnable program/)
  })

  it('names the other program a failed start ran, and only that', async () => {
    const env = { PATH: bin }
    const launch = { cwd: bin, env, timeoutMs: 5_000 }
    expect(await answersAsOtherProgram(join(bin, 'cc'), launch)).toBe(true)
    expect(await answersAsOtherProgram(join(bin, 'claude'), launch)).toBe(false)
    expect(await answersAsOtherProgram(script('broken', 'exit 1'), launch)).toBe(false)

    const exited = withObservedProviderExit(new Error('claude stream-json exited with code 1'))
    const otherProgram = {
      pathToClaudeCodeExecutable: '/usr/bin/cc',
      runsOtherProgram: async () => true
    }
    expect(
      providerStartupFailureFact(await explainOtherProgramStart(exited, otherProgram))
    ).toMatchObject({
      kind: 'agentCommandNotRunnable',
      detail: { text: '/usr/bin/cc', audience: 'person' }
    })
    const claude = { ...otherProgram, runsOtherProgram: async () => false }
    expect(await explainOtherProgramStart(exited, claude)).toBe(exited)
  })
})

import { execFileSync } from 'node:child_process'
import { existsSync, mkdtempSync, rmSync, writeFileSync } from 'node:fs'
import { tmpdir } from 'node:os'
import { join } from 'node:path'
import { afterEach, beforeEach, describe, expect, it } from 'vitest'
import {
  captureLoginShellEnvironment,
  resetLoginShellEnvironmentCacheForTests
} from './login-shell-environment'

const bashHasAliasTable =
  existsSync('/bin/bash') &&
  Number(execFileSync('/bin/bash', ['-c', 'echo ${BASH_VERSINFO[0]}']).toString()) >= 4

// A Command such as `cc` means what the user's terminal makes of it, so the capture reads aliases from the same login shell.
describe.skipIf(process.platform === 'win32')('a login shell capture', () => {
  let home: string

  beforeEach(() => {
    home = mkdtempSync(join(tmpdir(), 'orca-login-shell-aliases-'))
  })

  afterEach(() => {
    resetLoginShellEnvironmentCacheForTests()
    rmSync(home, { recursive: true, force: true })
  })

  const capture = (shell: string) =>
    captureLoginShellEnvironment({
      shellOverride: shell,
      env: { HOME: home, ZDOTDIR: home, PATH: '/usr/bin:/bin' }
    })

  it.runIf(existsSync('/bin/zsh'))("reads zsh's aliases from its rc files", async () => {
    writeFileSync(join(home, '.zshrc'), "alias cc=claude\nalias ll='ls -la'\n")
    const result = await capture('/bin/zsh')
    expect(result.status).toBe('captured')
    expect(result.aliases?.get('cc')).toBe('claude')
    expect(result.aliases?.get('ll')).toBe('ls -la')
  })

  it.runIf(bashHasAliasTable)("reads bash's aliases from its login profile", async () => {
    writeFileSync(join(home, '.bash_profile'), "alias cc='claude --verbose'\n")
    const result = await capture('/bin/bash')
    expect(result.status).toBe('captured')
    expect(result.aliases?.get('cc')).toBe('claude --verbose')
  })
})

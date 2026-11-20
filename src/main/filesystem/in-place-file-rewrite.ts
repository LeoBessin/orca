import { readFileSync, writeFileSync } from 'node:fs'
import { writeRollingFileBackup } from './rolling-file-backup'

/**
 * Rewrites a file in place, keeping its inode, with a copy of the old bytes at `<path>.bak` first:
 * an in-place write truncates before it writes, so a failed one must not leave the only copy cut short.
 */
export function rewriteFileInPlace(path: string, contents: string): void {
  const backupPath = `${path}.bak`
  writeRollingFileBackup(path, backupPath)
  try {
    writeFileSync(path, contents, 'utf-8')
  } catch (error) {
    try {
      writeFileSync(path, readFileSync(backupPath))
    } catch {
      // The backup still holds the old bytes.
    }
    throw error
  }
}

import { expect, it } from 'vitest'
import type SyncDatabase from '../../sqlite/sync-database'
import { openSessionSearchIndexFile } from './session-search-index-test-fixture'
import { deleteSearchMessages, insertSearchMessage } from './session-search-message-rows'
import {
  compactSessionSearchIndex,
  sessionSearchIndexCompactionDue
} from './session-search-index-compaction'

function indexSession(db: SyncDatabase, session: number, round: number): void {
  db.exec('BEGIN IMMEDIATE')
  deleteSearchMessages(db, session)
  for (let m = 0; m < 20; m++) {
    insertSearchMessage(db, session, {
      role: 'assistant',
      text: `session${session} round${round} message${m} ${'word'.repeat(m + 1)} needle`,
      timestamp: null,
      textTruncated: false
    } as never)
  }
  db.exec('COMMIT')
}

const indexBytes = (db: SyncDatabase) =>
  Number(db.prepare('SELECT SUM(length(block)) AS n FROM messages_fts_data').get()!.n)

it('merges rows a reindex deleted out of the search index, keeping live rows searchable', async () => {
  const index = await openSessionSearchIndexFile('search-compaction')
  try {
    const { db } = index
    // Each round replaces every session's rows, as a replace-mode reindex does.
    for (let round = 0; round < 6; round++) {
      for (let session = 1; session <= 200; session++) {
        indexSession(db, session, round)
      }
    }
    expect(sessionSearchIndexCompactionDue(db)).toBe(true)
    const before = indexBytes(db)

    await compactSessionSearchIndex(db, () => false)

    expect(indexBytes(db)).toBeLessThan(before / 2)
    expect(sessionSearchIndexCompactionDue(db)).toBe(false)
    expect(
      db.prepare("SELECT COUNT(*) AS n FROM messages_fts WHERE messages_fts MATCH 'needle'").get()!
        .n
    ).toBe(4000)
    expect(
      db.prepare("SELECT COUNT(*) AS n FROM messages_fts WHERE messages_fts MATCH 'round0'").get()!
        .n
    ).toBe(0)
  } finally {
    await index.close()
  }
})

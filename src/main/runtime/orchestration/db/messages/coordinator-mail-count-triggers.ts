import { installMailWorkerLifetimes, mailLifetimeCovers } from './coordinator-mail-lifetime-index'
import type { OrchestrationDb } from '../orchestration-db'
import { workerSentMail } from './dispatch-mail-sender'
import { exposeUtcTimestamp } from '../utc-timestamp'

const text = (value: unknown) => (typeof value === 'string' ? value : null)
const suffix = (column: string) => `substr(${column}, instr(${column}, ':') + 1)`

/** Canonical senders use one Dispatch lookup; other identities use indexed lifetime membership. */
export function coordinatorMailContribution(message: string, dispatch: string): string {
  return `(${message}.read = 1 AND NOT EXISTS (
    SELECT 1 FROM dispatch_contexts own WHERE own.id = ${dispatch} AND (
      EXISTS (SELECT 1 FROM dispatch_contexts worker
        WHERE worker.id = substr(${message}.from_handle, 10) AND worker.run_id = own.run_id
          AND worker.id IS NOT own.creator_dispatch_id AND worker.assignee_handle IS NOT NULL
          AND orca_mail_worker_matches(worker.id, worker.assignee_handle, worker.assignee_pane_key,
            worker.created_at, worker.completed_at, ${message}.from_handle, ${message}.sender_pane_key, ${message}.created_at) = 1)
      OR CASE WHEN coalesce(${message}.sender_pane_key, '') = ''
        THEN ${mailLifetimeCovers(message, 'bare', `${message}.from_handle`)}
        ELSE ${mailLifetimeCovers(message, 'withoutPane', `${message}.from_handle`)}
          OR ${mailLifetimeCovers(message, 'pane', `orca_mail_pane_identity(${message}.sender_pane_key)`)} END
    )
  ))`
}

const processed = (message: string) => `dispatch_id = substr(${message}.to_handle, 10)
  AND ${message}.to_handle GLOB 'dispatch:*' AND ${message}.read = 1
  AND (ready = 1 OR ${message}.sequence <= cursor)`

const identities = (message: string) => [
  ['handle', `${message}.from_handle`],
  ['pane', suffix(`${message}.sender_pane_key`)]
]

function dependencies(message: string, add: boolean): string {
  return identities(message)
    .map(([kind, identity]) =>
      add
        ? `
    INSERT INTO orca_coordinator_mail_senders(run_id, kind, identity, dispatch_id, refs, first_sent, last_sent)
      SELECT own.run_id, '${kind}', ${identity}, own.id, 1,
        orca_mail_timestamp(${message}.created_at), orca_mail_timestamp(${message}.created_at)
      FROM dispatch_contexts own JOIN orca_coordinator_mail_counts c ON c.dispatch_id = own.id
      WHERE ${processed(message)} AND ${identity} IS NOT NULL AND orca_mail_timestamp(${message}.created_at) IS NOT NULL
      ON CONFLICT(run_id, kind, identity, dispatch_id) DO UPDATE SET refs = refs + 1,
        first_sent = min(first_sent, excluded.first_sent), last_sent = max(last_sent, excluded.last_sent);
  `
        : `
    UPDATE orca_coordinator_mail_senders SET refs = refs - 1
      WHERE run_id = (SELECT run_id FROM dispatch_contexts WHERE id = substr(${message}.to_handle, 10))
        AND kind = '${kind}' AND identity = ${identity} AND dispatch_id IN (
        SELECT dispatch_id FROM orca_coordinator_mail_counts WHERE ${processed(message)} AND orca_mail_timestamp(${message}.created_at) IS NOT NULL);
    DELETE FROM orca_coordinator_mail_senders
      WHERE run_id = (SELECT run_id FROM dispatch_contexts WHERE id = substr(${message}.to_handle, 10))
        AND kind = '${kind}' AND identity = ${identity}
        AND dispatch_id = substr(${message}.to_handle, 10) AND refs = 0;
  `
    )
    .join('')
}

export function recordCoordinatorMailPageSenders(
  db: OrchestrationDb,
  id: string,
  after: number,
  through: number
): void {
  for (const [kind, identity] of identities('m')) {
    db.db
      .prepare(`INSERT INTO orca_coordinator_mail_senders(run_id, kind, identity, dispatch_id, refs, first_sent, last_sent)
      SELECT own.run_id, '${kind}', ${identity}, own.id, count(*),
        min(orca_mail_timestamp(m.created_at)), max(orca_mail_timestamp(m.created_at))
      FROM messages m JOIN dispatch_contexts own ON own.id = ?
      WHERE m.to_handle = ? AND m.read = 1 AND m.sequence > ? AND m.sequence <= ? AND ${identity} IS NOT NULL AND orca_mail_timestamp(m.created_at) IS NOT NULL
      GROUP BY ${identity}
      ON CONFLICT(run_id, kind, identity, dispatch_id) DO UPDATE SET refs = refs + excluded.refs,
        first_sent = min(first_sent, excluded.first_sent), last_sent = max(last_sent, excluded.last_sent)`)
      .run(id, `dispatch:${id}`, after, through)
  }
}

function adjust(message: 'OLD' | 'NEW', sign: '-' | '+'): string {
  return `UPDATE orca_coordinator_mail_counts
    SET mail_count = mail_count ${sign} ${coordinatorMailContribution(message, 'dispatch_id')}
    WHERE ${processed(message)};`
}

function invalidate(worker: 'OLD' | 'NEW', changedAfter = ''): string {
  const candidates = [
    `kind = 'handle' AND identity = ${worker}.assignee_handle`,
    `kind = 'handle' AND identity = 'dispatch:' || ${worker}.id`,
    `kind = 'pane' AND identity = ${suffix(`${worker}.assignee_pane_key`)}`
  ].map(
    (identity) => `SELECT s.dispatch_id FROM orca_coordinator_mail_senders s
    JOIN dispatch_contexts own ON own.id = s.dispatch_id
    WHERE s.run_id = ${worker}.run_id AND ${identity}
      AND ${worker}.id IS NOT own.creator_dispatch_id
      AND s.last_sent >= orca_mail_timestamp(${worker}.created_at)
      AND (orca_mail_completed_timestamp(${worker}.completed_at) IS NULL OR s.first_sent <= orca_mail_completed_timestamp(${worker}.completed_at))
      ${changedAfter}`
  )
  return `UPDATE orca_coordinator_mail_counts
    SET mail_count = 0, cursor = 0, ready = 0, revision = revision + 1
    WHERE dispatch_id IN (${candidates.join(' UNION ')});`
}

const DELETE_TRIGGER = `CREATE TEMP TRIGGER orca_mail_count_delete BEFORE DELETE ON main.messages
  BEGIN ${adjust('OLD', '-')} ${dependencies('OLD', false)} END;`

export function pauseCoordinatorMailCountForReset(db: OrchestrationDb): boolean {
  const installed =
    db.db
      .prepare(
        "SELECT 1 FROM sqlite_temp_master WHERE type = 'trigger' AND name = 'orca_mail_count_delete'"
      )
      .get() !== undefined
  if (installed) {
    db.db.exec(
      `DROP TRIGGER temp.orca_mail_count_delete;
      UPDATE orca_coordinator_mail_counts SET mail_count = 0, cursor = 0, ready = 0, revision = revision + 1;
      DELETE FROM orca_mail_lifetime_runs`
    )
  }
  return installed
}

export function resumeCoordinatorMailCountAfterReset(db: OrchestrationDb): void {
  db.db.exec(DELETE_TRIGGER)
}

export function installCoordinatorMailCountTriggers(db: OrchestrationDb): void {
  db.db.function('orca_mail_timestamp', (value) => {
    const parsed = Date.parse(exposeUtcTimestamp(text(value)) ?? '')
    return Number.isNaN(parsed) ? null : parsed
  })
  db.db.function('orca_mail_completed_timestamp', (value) => {
    const parsed = Date.parse(text(value) ?? '')
    return Number.isNaN(parsed) ? null : parsed
  })
  db.db.function(
    'orca_mail_worker_matches',
    (id, handle, pane, created, completed, from, senderPane, sent) => {
      const workerId = text(id)
      const workerHandle = text(handle)
      return workerId &&
        workerHandle &&
        workerSentMail(
          {
            id: workerId,
            assignee_handle: workerHandle,
            assignee_pane_key: text(pane),
            createdAt: Date.parse(exposeUtcTimestamp(text(created)) ?? ''),
            completedAt: Date.parse(text(completed) ?? '')
          },
          { from_handle: from, sender_pane_key: senderPane, created_at: sent }
        )
        ? 1
        : 0
    }
  )
  installMailWorkerLifetimes(db)
  db.db.exec(`
    CREATE TEMP TABLE orca_coordinator_mail_counts (
      dispatch_id TEXT PRIMARY KEY, run_id TEXT NOT NULL, mail_count INTEGER NOT NULL DEFAULT 0,
      cursor INTEGER NOT NULL DEFAULT 0, ready INTEGER NOT NULL DEFAULT 0,
      revision INTEGER NOT NULL DEFAULT 0
    );
    CREATE INDEX orca_mail_counts_run ON orca_coordinator_mail_counts(run_id);
    CREATE TEMP TABLE orca_coordinator_mail_senders (
      run_id TEXT NOT NULL, kind TEXT NOT NULL, identity TEXT NOT NULL, dispatch_id TEXT NOT NULL,
      refs INTEGER NOT NULL, first_sent INTEGER NOT NULL, last_sent INTEGER NOT NULL,
      PRIMARY KEY(run_id, kind, identity, dispatch_id)
    ) WITHOUT ROWID;
    CREATE INDEX orca_mail_senders_dispatch ON orca_coordinator_mail_senders(dispatch_id);
    CREATE TEMP TRIGGER orca_mail_count_retired AFTER DELETE ON orca_coordinator_mail_counts
      BEGIN DELETE FROM orca_coordinator_mail_senders WHERE dispatch_id = OLD.dispatch_id;
        DELETE FROM orca_mail_lifetime_runs WHERE run_id = OLD.run_id
          AND NOT EXISTS (SELECT 1 FROM orca_coordinator_mail_counts WHERE run_id = OLD.run_id); END;
    CREATE TEMP TRIGGER orca_mail_count_invalidated AFTER UPDATE OF revision ON orca_coordinator_mail_counts
      BEGIN DELETE FROM orca_coordinator_mail_senders WHERE dispatch_id = NEW.dispatch_id; END;
    CREATE TEMP TRIGGER orca_mail_count_insert AFTER INSERT ON main.messages
      BEGIN ${adjust('NEW', '+')} ${dependencies('NEW', true)} END;
    ${DELETE_TRIGGER}
    CREATE TEMP TRIGGER orca_mail_count_update AFTER UPDATE OF read, to_handle, from_handle, sender_pane_key, created_at ON main.messages
      BEGIN ${adjust('OLD', '-')} ${dependencies('OLD', false)} ${adjust('NEW', '+')} ${dependencies('NEW', true)} END;
    CREATE TEMP TRIGGER orca_mail_count_worker_insert AFTER INSERT ON main.dispatch_contexts
      BEGIN ${invalidate('NEW')} DELETE FROM orca_coordinator_mail_counts WHERE dispatch_id = NEW.id; END;
    CREATE TEMP TRIGGER orca_mail_count_worker_delete BEFORE DELETE ON main.dispatch_contexts
      BEGIN ${invalidate('OLD')} DELETE FROM orca_coordinator_mail_counts WHERE dispatch_id = OLD.id; END;
    CREATE TEMP TRIGGER orca_mail_count_worker_identity AFTER UPDATE OF
      run_id, creator_dispatch_id, assignee_handle, assignee_pane_key, created_at ON main.dispatch_contexts
      WHEN OLD.run_id IS NOT NEW.run_id OR OLD.creator_dispatch_id IS NOT NEW.creator_dispatch_id
        OR OLD.assignee_handle IS NOT NEW.assignee_handle OR OLD.assignee_pane_key IS NOT NEW.assignee_pane_key
        OR OLD.created_at IS NOT NEW.created_at
      BEGIN ${invalidate('OLD')} ${invalidate('NEW')}
        DELETE FROM orca_coordinator_mail_counts WHERE dispatch_id = NEW.id AND OLD.run_id IS NOT NEW.run_id;
        UPDATE orca_coordinator_mail_counts SET mail_count = 0, cursor = 0, ready = 0, revision = revision + 1
          WHERE dispatch_id = NEW.id AND OLD.creator_dispatch_id IS NOT NEW.creator_dispatch_id;
      END;
    CREATE TEMP TRIGGER orca_mail_count_worker_lifetime AFTER UPDATE OF completed_at ON main.dispatch_contexts
      WHEN OLD.completed_at IS NOT NEW.completed_at
      BEGIN ${invalidate('OLD', 'AND s.last_sent > min(coalesce(orca_mail_completed_timestamp(OLD.completed_at), 9223372036854775807), coalesce(orca_mail_completed_timestamp(NEW.completed_at), 9223372036854775807))')}
        ${invalidate('NEW', 'AND s.last_sent > min(coalesce(orca_mail_completed_timestamp(OLD.completed_at), 9223372036854775807), coalesce(orca_mail_completed_timestamp(NEW.completed_at), 9223372036854775807))')}
      END;
    CREATE TEMP TRIGGER orca_mail_count_remote_delete AFTER DELETE ON main.remote_dispatch_attachments
      BEGIN DELETE FROM orca_coordinator_mail_counts WHERE dispatch_id = OLD.dispatch_id; END;
    CREATE TEMP TRIGGER orca_mail_count_remote_settled AFTER UPDATE OF state ON main.remote_dispatch_attachments
      WHEN NEW.state NOT IN ('starting', 'ready')
      BEGIN DELETE FROM orca_coordinator_mail_counts WHERE dispatch_id = NEW.dispatch_id; END;
    CREATE TEMP TRIGGER orca_mail_count_worker_settled AFTER UPDATE OF status ON main.dispatch_contexts
      BEGIN DELETE FROM orca_coordinator_mail_counts WHERE dispatch_id = NEW.id AND NEW.status NOT IN ('pending', 'dispatched'); END;
  `)
}

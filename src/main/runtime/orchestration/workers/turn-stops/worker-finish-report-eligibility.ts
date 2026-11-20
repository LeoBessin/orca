import {
  prepareCoordinatorMailCount,
  preparedCoordinatorMailCount
} from '../../db/messages/prepared-coordinator-mail-count'
import { stopReportId } from './worker-stop-report-id'
import type { DispatchContextRow } from '../../db/orchestration-records'
import type { OrchestrationDb } from '../../db/orchestration-db'
import { rejectedReportInTurn } from '../../db/messages/rejected-worker-report'
import type { WorkerTurnStopDispatch, WorkerTurnStopReportDeps } from './worker-turn-stop-report'

export function workerFinishReportMuted(
  db: OrchestrationDb,
  target: WorkerTurnStopDispatch,
  turnStartedAt: number | undefined,
  pendingHelperWake = false
): boolean {
  const id = target.kind === 'local' ? target.dispatch.id : target.attachment.dispatch_id
  return (
    db.awaitsAnswerToLatestQuestion(id) ||
    rejectedReportInTurn(db, id, turnStartedAt) ||
    (target.kind === 'local' && pendingHelperWake)
  )
}

/** A prior report can dedupe the next finish; without its turn boundary, it cannot promise a wake. */
export function canPromiseWorkerFinishReport(
  db: OrchestrationDb,
  target: WorkerTurnStopDispatch,
  deps: Pick<WorkerTurnStopReportDeps, 'isWaitingForMail'>
): boolean {
  const id = target.kind === 'local' ? target.dispatch.id : target.attachment.dispatch_id
  if (deps.isWaitingForMail(`dispatch:${id}`) || workerFinishReportMuted(db, target, undefined)) {
    return false
  }
  if (target.kind === 'remote') {
    const count = preparedCoordinatorMailCount(db, id)
    return (
      count !== undefined &&
      !db.hasPendingFederatedWorkerReport(id) &&
      db.db
        .prepare(`SELECT 1 FROM federation_relay_items
        WHERE dispatch_id = ? AND direction = 'to_home' AND message_id = ? LIMIT 1`)
        .get(id, stopReportId(id, `finished:${count}`)) === undefined
    )
  }
  return (
    db.db
      .prepare(`SELECT 1 FROM messages
    WHERE json_valid(payload) AND json_extract(payload, '$.dispatchId') = ?
      AND (id GLOB 'msg_stop_*' OR
        (type = 'worker_done' AND json_type(payload, '$._orcaLifecycleRejection.code') IS 'text'))
    LIMIT 1`)
      .get(id) === undefined
  )
}

/** A helper's next ordinary finish must still reach its creator after follow-up mail. */
export async function canPromiseHelperFinishReport(
  db: OrchestrationDb,
  dispatch: DispatchContextRow,
  deps: Pick<WorkerTurnStopReportDeps, 'isWaitingForMail'>,
  current: () => boolean
): Promise<boolean> {
  if (
    deps.isWaitingForMail(`dispatch:${dispatch.id}`) ||
    workerFinishReportMuted(db, { kind: 'local', dispatch }, undefined)
  ) {
    return false
  }
  if (canPromiseWorkerFinishReport(db, { kind: 'local', dispatch }, deps)) {
    return true
  }
  const rejected = () =>
    db.db
      .prepare(`SELECT 1 FROM messages
    WHERE json_valid(payload) AND json_extract(payload, '$.dispatchId') = ?
      AND (id GLOB 'msg_stop_*' OR
        (type = 'worker_done' AND json_type(payload, '$._orcaLifecycleRejection.code') IS 'text'))
      AND type = 'worker_done' AND json_type(payload, '$._orcaLifecycleRejection.code') IS 'text'
    LIMIT 1`)
      .get(dispatch.id) !== undefined
  if (db.getFederatedDispatch(dispatch.id) || rejected()) {
    return false
  }
  await prepareCoordinatorMailCount(db, dispatch.id, current)
  let count = preparedCoordinatorMailCount(db, dispatch.id)
  if (count === undefined && current() && !db.db.isTransaction) {
    await prepareCoordinatorMailCount(db, dispatch.id, current)
    count = preparedCoordinatorMailCount(db, dispatch.id)
  }
  return (
    current() &&
    count !== undefined &&
    !rejected() &&
    !deps.isWaitingForMail(`dispatch:${dispatch.id}`) &&
    !workerFinishReportMuted(db, { kind: 'local', dispatch }, undefined) &&
    !db.getMessageById(stopReportId(dispatch.id, `finished:${count}`))
  )
}

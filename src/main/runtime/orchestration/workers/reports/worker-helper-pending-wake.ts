import type { OrcaRuntimeService } from '../../../orca-runtime'
import type { DispatchContextRow } from '../../db/orchestration-records'
import { DISPATCH_CONTEXT_COLUMN_LIST } from '../../db/row-column-lists'
import { dispatchReportMailbox } from './dispatch-report-mailbox'
import { localDispatchExecuting } from './worker-dispatch-execution'

/** An idle open helper is not a promise; follow only this worker's remaining wake chain. */
export async function workerHasPendingHelperWake(
  runtime: Pick<
    OrcaRuntimeService,
    | 'getExistingOrchestrationDb'
    | 'isOrchestrationWorkerWorking'
    | 'canPromiseOrchestrationHelperReport'
  >,
  dispatchId: string,
  isCurrent: () => boolean
): Promise<boolean> {
  const db = runtime.getExistingOrchestrationDb()
  if (!db) {
    return false
  }
  const seen = new Set<string>()
  let remaining = 256
  let finished = false
  const deadline = Date.now() + 5_000
  const current = () => !finished && isCurrent() && Date.now() < deadline
  const visit = async (parentId: string): Promise<(() => boolean) | null> => {
    if (!current() || seen.has(parentId) || remaining <= 0) {
      return null
    }
    seen.add(parentId)
    const parent = db.getDispatchContextById(parentId)
    if (!parent || parent.status !== 'dispatched') {
      return null
    }
    // Unknown wake evidence leaves the ordinary stop reporter responsible for informing the parent.
    const rows = db.db
      .prepare(`SELECT ${DISPATCH_CONTEXT_COLUMN_LIST} FROM dispatch_contexts
      WHERE creator_dispatch_id = ? AND status IN ('pending', 'dispatched') LIMIT ?`)
      .all(parentId, Math.min(remaining, 100) + 1)
    // oxlint-disable-next-line typescript/consistent-type-assertions -- SAFETY: the query uses the complete schema-owned Dispatch projection.
    const helpers = rows as DispatchContextRow[]
    const eligible = (id: string) => {
      const helper = db.getDispatchContextById(id)
      const state = db.getWorkerDispatch(id)?.state
      return (
        current() &&
        !db.getFederatedDispatch(id) &&
        !!helper &&
        helper.status === 'dispatched' &&
        helper.run_id === parent.run_id &&
        dispatchReportMailbox(db, helper) === `dispatch:${parentId}` &&
        (!state || state === 'ready')
      )
    }
    for (const helper of helpers.slice(0, 100)) {
      remaining -= 1
      if (remaining < 0) {
        return null
      }
      if (!current()) {
        return null
      }
      if (!eligible(helper.id)) {
        continue
      }
      const evidence = localDispatchExecuting(runtime, db, helper)
        ? () => localDispatchExecuting(runtime, db, helper)
        : await visit(helper.id)
      if (
        evidence &&
        eligible(helper.id) &&
        (await runtime.canPromiseOrchestrationHelperReport(helper, current)) &&
        eligible(helper.id) &&
        evidence()
      ) {
        return () => eligible(helper.id) && evidence()
      }
    }
    return null
  }
  try {
    return (await visit(dispatchId))?.() ?? false
  } catch {
    return false
  } finally {
    finished = true
  }
}

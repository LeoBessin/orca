import { ORCHESTRATION_FLEET_PAGE_MAX } from '../../../../shared/orchestration/fleet/orchestration-fleet-projection'
import type { OrchestrationDb } from '../db/orchestration-db'
import type { FederatedDispatchRow } from '../db/orchestration-records'

type HostGroup = {
  environmentId: string
  name: string
  dispatches: FederatedDispatchRow[]
}

export function groupFederatedDispatches(args: {
  db: OrchestrationDb
  dispatchIds: readonly string[]
  rows?: readonly FederatedDispatchRow[]
}): HostGroup[] {
  const rows = new Map(
    (args.rows ?? args.db.listFederatedDispatchesByIds(args.dispatchIds)).map((row) => [
      row.dispatch_id,
      row
    ])
  )
  return groupFederatedDispatchRows(args.dispatchIds.flatMap((id) => rows.get(id) ?? []))
}

export function groupFederatedDispatchRows(rows: readonly FederatedDispatchRow[]): HostGroup[] {
  const groups = new Map<string, HostGroup>()
  for (const dispatch of rows) {
    const groupKey = `${dispatch.environment_id}\u0000${dispatch.peer_fingerprint}`
    const group = groups.get(groupKey) ?? {
      environmentId: dispatch.environment_id,
      name: dispatch.environment_name,
      dispatches: []
    }
    group.dispatches.push(dispatch)
    groups.set(groupKey, group)
  }
  return [...groups.values()].flatMap((group) => {
    const batches: HostGroup[] = []
    for (let offset = 0; offset < group.dispatches.length; offset += ORCHESTRATION_FLEET_PAGE_MAX) {
      batches.push({
        ...group,
        dispatches: group.dispatches.slice(offset, offset + ORCHESTRATION_FLEET_PAGE_MAX)
      })
    }
    return batches
  })
}

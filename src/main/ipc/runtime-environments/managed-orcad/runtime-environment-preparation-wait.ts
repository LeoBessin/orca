import { RuntimeRpcCallQueueOverloadError } from '../../../../shared/remote-runtime/rpc/runtime-rpc-call-queue'
import {
  InFlightPromiseDedupe,
  stableInFlightKey
} from '../../../../shared/concurrency/in-flight-promise-dedupe'
import {
  throwIfSignalAborted,
  waitForPromiseWithSignal
} from '../../../../shared/concurrency/abort-signal-reason'

const preparations = new InFlightPromiseDedupe<unknown>(
  0,
  undefined,
  () => new RuntimeRpcCallQueueOverloadError('global')
)

/** Cancel a caller's wait without multiplying or cancelling the shared host operation. */
export async function waitForRuntimePreparation<T>(
  userDataPath: string,
  environmentId: string,
  stage: string,
  work: () => Promise<T>,
  signal?: AbortSignal
): Promise<T> {
  throwIfSignalAborted(signal)
  const operation = signal
    ? preparations.run(stableInFlightKey([userDataPath, environmentId, stage]), work)
    : work()
  // oxlint-disable-next-line typescript/consistent-type-assertions -- SAFETY: each stage key names one production operation and result type.
  const value = (await waitForPromiseWithSignal(operation, signal)) as T
  throwIfSignalAborted(signal)
  return value
}

import type { SubmittableExtrinsic, VoidFn } from '@polkadot/api/types'
import type { KeyringPair } from '@polkadot/keyring/types'
import type { DispatchError } from '@polkadot/types/interfaces'
import type { Hash } from '@polkadot/types/interfaces/runtime'

export interface AcurastOperationOptions {
  /**
   * Optional cancellation signal. The SDK checks it before expensive stages and
   * tears down active subscriptions when it fires.
   */
  signal?: AbortSignal
  /** Maximum time to wait for the submitted transaction to reach the target status. */
  timeoutMs?: number
  /**
   * Optional cooperative scheduler hook for long-running applications. This
   * cannot preempt synchronous Polkadot codec/signing work, but it lets callers
   * yield between SDK-controlled stages.
   */
  yieldToEventLoop?: (phase: string) => Promise<void> | void
  /** Optional phase observer for diagnostics. Do not include secret values. */
  onPhase?: (phase: string, data?: Record<string, unknown>) => void
}

export interface AcurastSignAndSendOptions extends AcurastOperationOptions {
  phasePrefix?: string
  waitFor?: 'inBlock' | 'finalized'
  formatDispatchError?: (dispatchError: DispatchError) => Error
}

export function throwIfAborted(signal?: AbortSignal): void {
  if (!signal?.aborted) return
  throw abortError(signal.reason)
}

export async function yieldAcurastPhase(
  options: AcurastOperationOptions | undefined,
  phase: string,
  data?: Record<string, unknown>,
): Promise<void> {
  throwIfAborted(options?.signal)
  options?.onPhase?.(phase, data)
  await options?.yieldToEventLoop?.(phase)
  throwIfAborted(options?.signal)
}

export async function acurastDelay(
  ms: number,
  options?: AcurastOperationOptions,
  phase = 'delay',
): Promise<void> {
  await yieldAcurastPhase(options, `${phase}.before`, { ms })
  let abort: (() => void) | undefined
  await new Promise<void>((resolve, reject) => {
    const timer = setTimeout(resolve, Math.max(0, ms))
    abort = () => {
      clearTimeout(timer)
      reject(abortError(options?.signal?.reason))
    }
    if (options?.signal) {
      if (options.signal.aborted) {
        abort()
        return
      }
      options.signal.addEventListener('abort', abort, { once: true })
    }
  }).finally(() => {
    if (abort) options?.signal?.removeEventListener('abort', abort)
  })
  await yieldAcurastPhase(options, `${phase}.after`, { ms })
}

export async function signAndSendTx(
  tx: SubmittableExtrinsic<'promise', any>,
  keyring: KeyringPair,
  options: AcurastSignAndSendOptions = {},
): Promise<Hash> {
  const phasePrefix = options.phasePrefix ?? 'acurast.tx'
  const waitFor = options.waitFor ?? 'inBlock'

  return await new Promise<Hash>((resolve, reject) => {
    let settled = false
    let unsubscribe: VoidFn | undefined
    let callbacksReady = false
    const pendingEvents: Array<{ status: any; dispatchError?: DispatchError }> = []
    let abort: (() => void) | undefined

    const timer =
      options.timeoutMs === undefined
        ? undefined
        : setTimeout(
            () => finish(new Error(`Timed out waiting for ${phasePrefix}`)),
            options.timeoutMs,
          )

    const cleanup = async (next = unsubscribe) => {
      if (!next) return
      await yieldAcurastPhase(options, `${phasePrefix}.unsubscribe`)
      next()
    }

    const finish = (error?: unknown, hash?: Hash) => {
      if (settled) return
      settled = true
      if (timer) clearTimeout(timer)
      if (abort) options.signal?.removeEventListener('abort', abort)
      void cleanup().finally(() => {
        if (error) reject(error)
        else resolve(hash!)
      })
    }

    abort = () => finish(abortError(options.signal?.reason))
    if (options.signal) {
      if (options.signal.aborted) {
        abort()
        return
      }
      options.signal.addEventListener('abort', abort, { once: true })
    }

    const handleEvent = async (event: { status: any; dispatchError?: DispatchError }) => {
      await yieldAcurastPhase(options, `${phasePrefix}.callback`)
      if (event.dispatchError) {
        finish(
          options.formatDispatchError?.(event.dispatchError) ??
            new Error(String(event.dispatchError)),
        )
        return
      }
      if (event.status?.isInBlock && waitFor === 'inBlock') {
        finish(undefined, event.status.hash)
        return
      }
      if (event.status?.isFinalized) {
        finish(undefined, event.status.hash)
      }
    }

    void (async () => {
      try {
        await yieldAcurastPhase(options, `${phasePrefix}.invoke`)
        const result = await tx.signAndSend(keyring, (event) => {
          if (!callbacksReady) {
            pendingEvents.push(event)
            return
          }
          void handleEvent(event).catch(finish)
        })
        if (typeof result === 'function') {
          if (settled) void cleanup(result).catch(() => undefined)
          else unsubscribe = result
        }
        callbacksReady = true
        for (const event of pendingEvents.splice(0)) {
          void handleEvent(event).catch(finish)
        }
      } catch (error) {
        finish(error)
      }
    })()
  })
}

function abortError(reason?: unknown): Error {
  if (reason instanceof Error) return reason
  const error = new Error(reason === undefined ? 'Operation aborted' : String(reason))
  error.name = 'AbortError'
  return error
}

import '@polkadot/api-augment'
import type { ApiPromise } from '@polkadot/api'
import type { SubmittableExtrinsic, VoidFn } from '@polkadot/api/types'
import type { KeyringPair } from '@polkadot/keyring/types'
import type { DispatchError } from '@polkadot/types/interfaces'
import type { Codec } from '@polkadot/types/types'
import {
  AssignmentStrategyVariant,
  DeploymentError,
  type AcurastProjectConfig,
  type JobRegistration,
} from '../types/project.js'
import { DeploymentStatus } from '../types/deployment-status.js'
import { buildMinMetricsForDeploy } from './benchmark-filters.js'
import { type AcurastOperationOptions, throwIfAborted, yieldAcurastPhase } from './transaction.js'

export interface BuildDeployExtrinsicOptions extends Pick<AcurastOperationOptions, 'signal'> {
  projectConfig?: AcurastProjectConfig
}

export interface RegisterJobOptions extends BuildDeployExtrinsicOptions, AcurastOperationOptions {
  observeJobStatus?: boolean
}

export interface RegisterJobResult {
  txHash: string
  jobIds: unknown[]
}

export const registerJob = async (
  api: ApiPromise,
  injector: KeyringPair,
  job: JobRegistration,
  statusCallback: (status: DeploymentStatus, data?: JobRegistration | any) => void,
  registerOptions?: RegisterJobOptions,
): Promise<string> => {
  await yieldAcurastPhase(registerOptions, 'acurast.registerJob.build')
  const tx = buildDeployExtrinsic(api, job, registerOptions)
  const result = await signAndSendDeployExtrinsic(
    api,
    tx,
    injector,
    statusCallback,
    registerOptions,
  )
  return result.txHash
}

export const buildDeployExtrinsic = (
  api: ApiPromise,
  job: JobRegistration,
  options?: BuildDeployExtrinsicOptions,
): SubmittableExtrinsic<'promise', any> => {
  throwIfAborted(options?.signal)
  const script = `0x${Buffer.from(new TextEncoder().encode(job.script)).toString('hex')}`

  const jobRegistration = api.createType('AcurastCommonJobRegistration', {
    script: api.createType('Bytes', script),
    allowedSources: job.allowedSources
      ? api.createType('Option<Vec<AccountId>>', job.allowedSources)
      : api.createType('Option<Vec<AccountId>>', undefined),
    allowOnlyVerifiedSources: job.allowOnlyVerifiedSources,
    schedule: {
      duration: api.createType('u64', job.schedule.duration),
      startTime: api.createType('u64', job.schedule.startTime),
      endTime: api.createType('u64', job.schedule.endTime),
      interval: api.createType('u64', job.schedule.interval),
      maxStartDelay: api.createType('u64', job.schedule.maxStartDelay),
    },
    memory: api.createType('u32', job.memory),
    networkRequests: api.createType('u32', job.networkRequests),
    storage: api.createType('u32', job.storage),
    requiredModules: api.createType('Vec<AcurastCommonJobModule>', job.requiredModules ?? []),
    extra: api.createType('PalletAcurastMarketplaceRegistrationExtra', {
      requirements: api.createType('PalletAcurastMarketplaceJobRequirements', {
        assignmentStrategy:
          job.extra.requirements.assignmentStrategy.variant == AssignmentStrategyVariant.Single
            ? api.createType('PalletAcurastMarketplaceAssignmentStrategy', {
                single: job.extra.requirements.assignmentStrategy.instantMatch
                  ? api.createType(
                      'Option<Vec<PalletAcurastMarketplacePlannedExecution>>',
                      job.extra.requirements.assignmentStrategy.instantMatch.map((item) => ({
                        source: api.createType('AccountId', item.source),
                        startDelay: api.createType('u64', item.startDelay.toFixed()),
                      })),
                    )
                  : api.createType('Option<bool>', undefined),
              })
            : api.createType('PalletAcurastMarketplaceAssignmentStrategy', {
                competing: '',
              }),
        slots: api.createType('u8', job.extra.requirements.slots),
        reward: api.createType('u128', job.extra.requirements.reward),
        minReputation: job.extra.requirements.minReputation
          ? api.createType('Option<u128>', job.extra.requirements.minReputation)
          : api.createType('Option<u128>', undefined),
        processorVersion: job.extra.requirements.processorVersion
          ? api.createType(
              'Option<PalletAcurastMarketplaceProcessorVersionRequirements>',
              job.extra.requirements.processorVersion,
            )
          : api.createType(
              'Option<PalletAcurastMarketplaceProcessorVersionRequirements>',
              undefined,
            ),
        instantMatch: job.extra.requirements.instantMatch
          ? api.createType(
              'Option<Vec<PalletAcurastMarketplacePlannedExecution>>',
              job.extra.requirements.instantMatch.map((item: any) => ({
                source: api.createType('AccountId', item.source),
                startDelay: api.createType('u64', item.startDelay),
              })),
            )
          : api.createType('Option<bool>', undefined),
        runtime: api.createType('PalletAcurastMarketplaceRuntime', job.extra.requirements.runtime),
      }),
    }),
  })

  const mutability = api.createType('AcurastCommonScriptMutability', job.mutability)
  const reuseKeysFrom = job.reuseKeysFrom
    ? api.createType('Option<(AcurastCommonMultiOrigin, u128)>', [
        api.createType('AcurastCommonMultiOrigin', {
          acurast: job.reuseKeysFrom[1],
        }),
        api.createType('u128', job.reuseKeysFrom[2]),
      ])
    : api.createType('Option<(AcurastCommonMultiOrigin, u128)>', undefined)
  const minMetrics = options?.projectConfig
    ? buildMinMetricsForDeploy(api, options.projectConfig)
    : api.createType('Option<Vec<(u8, u128, u128)>>', [])

  return api.tx['acurastMarketplace']['deploy'](
    jobRegistration,
    mutability,
    reuseKeysFrom,
    minMetrics,
  )
}

export async function signAndSendDeployExtrinsic(
  api: ApiPromise,
  tx: SubmittableExtrinsic<'promise', any>,
  injector: KeyringPair,
  statusCallback: (status: DeploymentStatus, data?: JobRegistration | any) => void,
  options: RegisterJobOptions = {},
): Promise<RegisterJobResult> {
  return await new Promise<RegisterJobResult>((resolve, reject) => {
    let settled = false
    let unsubscribe: VoidFn | undefined
    let storedJobStatusUnsub: VoidFn | undefined
    let callbacksReady = false
    let observedJobStatusKey: string | undefined
    let resultJobIds: unknown[] = []
    const pendingEvents: Array<{
      status: any
      events: any[]
      txHash: { toHex?: () => string; toString?: () => string }
      dispatchError?: DispatchError
    }> = []
    let abort: (() => void) | undefined

    const timer =
      options.timeoutMs === undefined
        ? undefined
        : setTimeout(
            () => finish(new DeploymentError('Timed out waiting for job deployment', 'Timeout')),
            options.timeoutMs,
          )

    const cleanup = async (next = unsubscribe) => {
      await yieldAcurastPhase(options, 'acurast.registerJob.unsubscribe')
      storedJobStatusUnsub?.()
      storedJobStatusUnsub = undefined
      next?.()
    }

    const finish = (error?: unknown, txHash?: string) => {
      if (settled) return
      settled = true
      if (timer) clearTimeout(timer)
      if (abort) options.signal?.removeEventListener('abort', abort)
      void cleanup().finally(() => {
        if (error) reject(error)
        else resolve({ txHash: txHash!, jobIds: resultJobIds })
      })
    }

    abort = () => finish(new DeploymentError('Operation aborted', 'AbortError'))
    if (options.signal) {
      if (options.signal.aborted) {
        abort()
        return
      }
      options.signal.addEventListener('abort', abort, { once: true })
    }

    const handleStoredJobStatuses = (jobIds: Codec[], statuses: Codec[]) => {
      const stat = api.registry.createType(
        'Vec<Option<PalletAcurastMarketplaceJobStatus>>',
        statuses,
      )
      stat
        .map((value, index) => {
          if (value.isSome) {
            const statusValue = value.unwrap() as any
            if (statusValue.isMatched) {
              statusCallback(DeploymentStatus.Matched, {
                jobIds: jobIds.map((id) => id.toJSON()),
              })
              return { id: jobIds[index], status: 'Matched' }
            } else if (statusValue.isAssigned) {
              statusCallback(DeploymentStatus.Acknowledged, {
                acknowledged: statusValue.asAssigned.toNumber(),
              })
              storedJobStatusUnsub?.()
              storedJobStatusUnsub = undefined
              return {
                id: jobIds[index],
                status: JSON.stringify({
                  assigned: statusValue.asAssigned.toNumber(),
                }),
              }
            }
            return { id: jobIds[index], status: 'Open' }
          }
          return undefined
        })
        .filter((value) => value !== undefined)
    }

    const handleEvent = async (event: {
      status: any
      events: any[]
      txHash: { toHex?: () => string; toString?: () => string }
      dispatchError?: DispatchError
    }) => {
      await yieldAcurastPhase(options, 'acurast.registerJob.callback')
      const jobRegistrationEvents = event.events.filter((record) => {
        return (
          record.event.section === 'acurast' && record.event.method === 'JobRegistrationStoredV2'
        )
      })
      const jobIds = jobRegistrationEvents.map((jobRegistrationEvent) => {
        return jobRegistrationEvent.event.data[0] as Codec
      })

      if (jobIds.length > 0) {
        resultJobIds = jobIds.map((jobId) => jobId.toJSON())
        statusCallback(DeploymentStatus.WaitingForMatch, { jobIds: resultJobIds })
        const jobStatusKey = JSON.stringify(resultJobIds)
        if (options.observeJobStatus !== false && observedJobStatusKey !== jobStatusKey) {
          observedJobStatusKey = jobStatusKey
          await yieldAcurastPhase(options, 'acurast.registerJob.storedJobStatus.subscribe')
          storedJobStatusUnsub = await api.query.acurastMarketplace.storedJobStatus.multi(
            jobIds,
            (statuses) => handleStoredJobStatuses(jobIds, statuses),
          )
        }
      }

      if (event.status.isInBlock || event.status.isFinalized) {
        unsubscribe?.()
        unsubscribe = undefined
      }

      if (event.dispatchError) {
        finish(deploymentErrorFromDispatch(api, event.dispatchError))
      } else if (event.status.isInBlock) {
        finish(undefined, txHashHex(event.txHash))
      }
    }

    void (async () => {
      try {
        await yieldAcurastPhase(options, 'acurast.registerJob.signAndSend.invoke')
        const next = await tx.signAndSend(injector, (event) => {
          if (!callbacksReady) {
            pendingEvents.push(event)
            return
          }
          void handleEvent(event).catch(finish)
        })
        if (typeof next === 'function') {
          if (settled) void cleanup(next).catch(() => undefined)
          else unsubscribe = next
        }
        callbacksReady = true
        for (const event of pendingEvents.splice(0)) {
          void handleEvent(event).catch(finish)
        }
      } catch (e) {
        finish(
          new DeploymentError(
            e instanceof Error ? e.message : 'Unknown error during job deployment',
            'DeploymentError',
            { originalError: e },
          ),
        )
      }
    })()
  })
}

function deploymentErrorFromDispatch(
  api: ApiPromise,
  dispatchError: DispatchError,
): DeploymentError {
  if (dispatchError.isModule) {
    const decoded = api.registry.findMetaError(dispatchError.asModule)
    const { docs, name, section } = decoded

    return new DeploymentError(`${docs.join(' ')}`, `${section}.${name}`, {
      section,
      name,
      docs,
    })
  }
  const error = dispatchError.toHuman() || dispatchError.toString()
  return new DeploymentError(error, 'TransactionError', { originalError: error })
}

function txHashHex(txHash: { toHex?: () => string; toString?: () => string }): string {
  const hex = txHash.toHex?.()
  if (hex) return hex
  return txHash.toString?.() ?? String(txHash)
}

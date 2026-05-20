import { AcurastService } from './acurast-service.js'
import { JobEnvironmentService } from './env-encryption.js'
import type { EnvVar, Job, JobId } from '../types/env.js'
import { toNumber } from './job-to-number.js'
import type { KeyringPair } from '@polkadot/keyring/types'
import type { KeyStore } from './key-store.js'
import { type AcurastSignAndSendOptions, acurastDelay, yieldAcurastPhase } from './transaction.js'

export interface SetEnvVarsOptions extends AcurastSignAndSendOptions {
  /** Wallet used to sign the `setEnvironments` extrinsic(s). */
  wallet: KeyringPair
  /** WebSocket RPC endpoint for the Acurast chain. */
  rpcEndpoint: string
  /** Persistent ECDH keypair storage. Defaults to in-memory. */
  keyStore?: KeyStore
  /** Existing service to reuse. When omitted the helper owns and disconnects a temporary service. */
  acurastService?: AcurastService
  /** Poll interval while waiting for assignment public keys. Defaults to 30 seconds. */
  pollIntervalMs?: number
}

export const setEnvVars = async (
  job: Job & { envVars?: EnvVar[] },
  options: SetEnvVarsOptions,
): Promise<{ hash?: string }> => {
  const envVars = job.envVars ?? []
  if (envVars.length === 0) {
    return {}
  }

  const acurast = options.acurastService ?? new AcurastService(options.rpcEndpoint)
  const ownsService = options.acurastService === undefined
  const pollIntervalMs = options.pollIntervalMs ?? 30_000
  const deadline = options.timeoutMs === undefined ? undefined : Date.now() + options.timeoutMs

  try {
    for (;;) {
      await yieldAcurastPhase(options, 'acurast.setEnvVars.assignments')
      const assignedProcessors = await acurast.assignedProcessors([
        [{ acurast: job.id[0].acurast }, Number(toNumber(job.id[1]))],
      ])

      const keys: [string, JobId][] = Array.from(assignedProcessors.entries()).flatMap(
        ([_, [jobId, processors]]) =>
          processors.map<[string, JobId]>((account) => [account, jobId]),
      )

      const jobAssignmentInfos = await acurast.jobAssignments(keys)

      if (
        jobAssignmentInfos.length > 0 &&
        jobAssignmentInfos.some((info) => info.assignment.pubKeys.length > 0)
      ) {
        const jobEnvironmentService = new JobEnvironmentService({
          acurastService: acurast,
          keyStore: options.keyStore,
        })
        const res = await jobEnvironmentService.setEnvironmentVariablesMulti(
          options.wallet,
          jobAssignmentInfos,
          Number(toNumber(job.id[1] as any)),
          envVars,
          options,
        )

        return { hash: res.hash }
      }

      if (deadline !== undefined && Date.now() >= deadline) {
        throw new Error('Timed out waiting for job assignment public keys')
      }

      const waitMs =
        deadline === undefined ? pollIntervalMs : Math.min(pollIntervalMs, deadline - Date.now())
      await acurastDelay(waitMs, options, 'acurast.setEnvVars.waitForAssignments')
    }
  } finally {
    if (ownsService) {
      await acurast.disconnect()
    }
  }
}

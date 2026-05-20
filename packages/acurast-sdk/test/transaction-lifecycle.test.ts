import type { SubmittableExtrinsic, VoidFn } from '@polkadot/api/types'
import type { KeyringPair } from '@polkadot/keyring/types'
import type { AcurastService } from '../src/chain/acurast-service.js'
import { setEnvVars } from '../src/chain/set-env-vars.js'
import { signAndSendTx } from '../src/chain/transaction.js'
import type { Job } from '../src/types/env.js'

const PAIR = { address: '5test' } as KeyringPair

describe('transaction lifecycle helpers', () => {
  test('queues early signAndSend callbacks and unsubscribes on in-block status', async () => {
    let unsubscribed = false
    const phases: string[] = []
    const tx = {
      signAndSend: async (
        _keyring: KeyringPair,
        callback: (event: any) => void,
      ): Promise<VoidFn> => {
        callback({ status: { isInBlock: true, hash: '0xabc' } })
        return () => {
          unsubscribed = true
        }
      },
    } as unknown as SubmittableExtrinsic<'promise', any>

    await expect(
      signAndSendTx(tx, PAIR, {
        phasePrefix: 'test.tx',
        yieldToEventLoop: (phase) => {
          phases.push(phase)
        },
      }),
    ).resolves.toBe('0xabc')

    expect(unsubscribed).toBe(true)
    expect(phases).toContain('test.tx.invoke')
    expect(phases).toContain('test.tx.callback')
    expect(phases).toContain('test.tx.unsubscribe')
  })

  test('unsubscribes when a submitted transaction times out', async () => {
    let unsubscribed = false
    const tx = {
      signAndSend: async (): Promise<VoidFn> => {
        return () => {
          unsubscribed = true
        }
      },
    } as unknown as SubmittableExtrinsic<'promise', any>

    await expect(
      signAndSendTx(tx, PAIR, {
        phasePrefix: 'test.timeout',
        timeoutMs: 5,
      }),
    ).rejects.toThrow('Timed out waiting for test.timeout')

    expect(unsubscribed).toBe(true)
  })

  test('setEnvVars uses bounded polling instead of recursive service creation', async () => {
    const calls: string[] = []
    const acurast = {
      async assignedProcessors() {
        calls.push('assignedProcessors')
        return new Map()
      },
      async jobAssignments() {
        calls.push('jobAssignments')
        return []
      },
      async disconnect() {
        calls.push('disconnect')
      },
    } as unknown as AcurastService

    await expect(
      setEnvVars(jobWithEnvVars(), {
        wallet: PAIR,
        rpcEndpoint: 'ws://example.invalid',
        acurastService: acurast,
        timeoutMs: 5,
        pollIntervalMs: 1,
      }),
    ).rejects.toThrow('Timed out waiting for job assignment public keys')

    expect(calls).toContain('assignedProcessors')
    expect(calls).toContain('jobAssignments')
    expect(calls).not.toContain('disconnect')
  })
})

function jobWithEnvVars(): Job & { envVars: Array<{ key: string; value: string }> } {
  return {
    id: [{ acurast: '5test' }, 1],
    registration: {},
    envVars: [{ key: 'EXAMPLE', value: 'value' }],
  } as Job & { envVars: Array<{ key: string; value: string }> }
}

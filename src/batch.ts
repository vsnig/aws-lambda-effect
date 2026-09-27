import { Effect } from 'effect'
import type { Pipeline, Uow } from './core.js'
import { EventBus } from './services.js'

/**
 * Runs every pipeline over the whole batch. Faults are parked on the event bus,
 * transient failures are returned as `batchItemFailures` for redelivery.
 */
export const processBatch = <R>(pipelines: ReadonlyArray<Pipeline<R>>, uows: ReadonlyArray<Uow>) =>
  Effect.gen(function* () {
    const outcomes = yield* Effect.forEach(pipelines, (pipeline) => pipeline.run(uows), {
      concurrency: 'unbounded',
    })

    const bus = yield* EventBus
    yield* Effect.forEach(
      outcomes.flatMap((o) => o.faults),
      (fault) => bus.publishFault(fault),
      { concurrency: 8, discard: true },
    )

    const retryIds = new Set(outcomes.flatMap((o) => o.retries.map((r) => r.uow.record.id)))
    return { batchItemFailures: [...retryIds].map((itemIdentifier) => ({ itemIdentifier })) }
  })

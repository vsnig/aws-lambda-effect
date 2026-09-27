import { Data, Effect } from 'effect'

// MARK: errors — classify where the error is born

/** Terminal: retrying the same record will never help. Parked as a fault. */
export class Fault extends Data.TaggedError('Fault')<{
  readonly reason: string
  readonly cause?: unknown
}> {}

/** Transient: may succeed later. Handed back to AWS for redelivery. */
export class Transient extends Data.TaggedError('Transient')<{ readonly cause: unknown }> {}

// MARK: uow — the envelope for one record

export interface Uow {
  /** `id` is the messageId (SQS) or sequence number (Kinesis, DynamoDB Streams) */
  readonly record: { readonly id: string; readonly raw: unknown }
  readonly event: {
    readonly id: string
    readonly type: string
    readonly partitionKey: string
    readonly payload: unknown
  }
}

// MARK: running a per-uow function over a batch

export interface Outcome<Out> {
  readonly ok: ReadonlyArray<Out>
  readonly faults: ReadonlyArray<{ readonly uow: Uow; readonly error: Fault }>
  readonly retries: ReadonlyArray<{ readonly uow: Uow; readonly error: Transient }>
}

/**
 * Runs `f` for every uow. A failing uow is set aside and the rest carry on.
 * A defect (a bug that throws) becomes a `Fault`.
 */
export const forEachUow = <In extends Uow, Out, R>(
  uows: ReadonlyArray<In>,
  f: (uow: In) => Effect.Effect<Out, Fault | Transient, R>,
  options?: { readonly concurrency?: number },
): Effect.Effect<Outcome<Out>, never, R> =>
  Effect.partition(
    uows,
    (uow) =>
      f(uow).pipe(
        Effect.catchDefect((cause) => Effect.fail(new Fault({ reason: 'defect', cause }))),
        Effect.mapError((error) => ({ uow, error })),
      ),
    { concurrency: options?.concurrency ?? 8 },
  ).pipe(
    Effect.map(([failed, ok]) => ({
      ok,
      faults: failed.flatMap(({ uow, error }) => (error._tag === 'Fault' ? [{ uow, error }] : [])),
      retries: failed.flatMap(({ uow, error }) =>
        error._tag === 'Transient' ? [{ uow, error }] : [],
      ),
    })),
  )

// MARK: pipeline — what a flavor returns for one rule

export interface Pipeline<R> {
  readonly id: string
  readonly run: (uows: ReadonlyArray<Uow>) => Effect.Effect<Outcome<unknown>, never, R>
}

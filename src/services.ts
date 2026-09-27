import { Context, type Effect } from 'effect'
import type { Fault, Transient, Uow } from './core.js'

export interface QueryRequest {
  readonly table: string
  readonly key: string
}

/** The minimum an event needs to be published. Rules may emit anything that has at least this. */
export interface Emittable {
  readonly type: string
}

// MARK: services — real implementations wrap the AWS SDK and map its errors to Fault / Transient

export class DynamoDb extends Context.Service<
  DynamoDb,
  {
    readonly query: (
      request: QueryRequest,
    ) => Effect.Effect<ReadonlyArray<unknown>, Fault | Transient>
  }
>()('aws-lambda-effect/DynamoDb') {}

export class EventBus extends Context.Service<
  EventBus,
  {
    readonly publish: <E extends Emittable>(event: E) => Effect.Effect<void, Transient>
    readonly publishFault: (fault: {
      readonly uow: Uow
      readonly error: Fault
    }) => Effect.Effect<void>
  }
>()('aws-lambda-effect/EventBus') {}

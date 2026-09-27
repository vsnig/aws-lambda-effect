import { Effect, Layer } from 'effect'
import { DynamoDb, EventBus, processBatch, Transient, type Uow } from '../src/index.js'
import { orderCancelled, orderCreated, vipShipped } from './rules.js'

// MARK: fake services — same rules, no AWS

const FakeDynamoDb = Layer.succeed(DynamoDb, {
  query: (request) =>
    request.key === 'throttle'
      ? Effect.fail(new Transient({ cause: 'ThrottlingException' }))
      : request.key === 'bad'
        ? Effect.succeed([{ customerId: 1 }])
        : Effect.succeed([{ customerId: request.key, name: 'Ann', vip: true }]),
})

const FakeEventBus = Layer.succeed(EventBus, {
  publish: (event) => Effect.sync(() => console.log('published    ', JSON.stringify(event))),
  publishFault: ({ uow, error }) =>
    Effect.sync(() => console.log('fault parked ', uow.record.id, error.reason)),
})

const uow = (id: string, type: string, partitionKey: string, payload: string): Uow => ({
  record: { id, raw: null },
  event: { id, type, partitionKey, payload },
})

const batch = [
  uow('r1', 'order-created', 'o1', 'c1'), // ok
  uow('r2', 'order-created', 'o2', 'bad'), // row fails its schema → fault
  uow('r3', 'order-created', 'o3', 'throttle'), // throttled → retry
  uow('r4', 'order-cancelled', 'o4', 'first'), // compacted away
  uow('r5', 'order-cancelled', 'o4', 'second'), // latest for o4 → published
  uow('r6', 'order-shipped', 'o6', 'c6'), // custom pipeline
]

Effect.runPromise(
  processBatch([orderCreated, orderCancelled, vipShipped], batch).pipe(
    Effect.provide(Layer.mergeAll(FakeDynamoDb, FakeEventBus)),
  ),
).then((response) => console.log('response     ', JSON.stringify(response)))

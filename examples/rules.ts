import { Effect, Schema, SchemaParser } from 'effect'
import { cdc, DynamoDb, EventBus, Fault, forEachUow, type Pipeline } from '../src/index.js'

const Customer = Schema.Struct({ customerId: Schema.String, name: Schema.String, vip: Schema.Boolean })

// MARK: rules — no annotations: hook inputs come from the flavor, Row and the event shape are inferred

export const orderCreated = cdc({
  id: 'order-created',
  eventType: 'order-created',
  query: {
    rows: Customer,
    toRequest: (uow) => ({ table: 'customers', key: String(uow.event.payload) }),
  },
  toEvent: (uow) => ({ type: 'order-enriched', names: uow.queryResponse.map((c) => c.name) }),
})

export const orderCancelled = cdc({
  id: 'order-cancelled',
  eventType: 'order-cancelled',
  compact: true,
  toEvent: (uow) => ({ type: 'order-cancelled-notice', payload: uow.event.payload }),
})

// MARK: a custom pipeline — no flavor, no rule, just Effect code per uow

export const vipShipped: Pipeline<DynamoDb | EventBus> = {
  id: 'vip-shipped',
  run: (uows) =>
    forEachUow(
      uows.filter((uow) => uow.event.type === 'order-shipped'),
      (uow) =>
        Effect.gen(function* () {
          const [item] = yield* (yield* DynamoDb).query({
            table: 'customers',
            key: String(uow.event.payload),
          })
          if (item === undefined) return yield* new Fault({ reason: 'customer not found' })

          const customer = yield* SchemaParser.decodeUnknownEffect(Customer)(item).pipe(
            Effect.mapError((cause) => new Fault({ reason: 'bad customer row', cause })),
          )
          if (!customer.vip) return uow

          yield* (yield* EventBus).publish({ type: 'vip-order-shipped', customerId: customer.customerId })
          return uow
        }),
    ),
}

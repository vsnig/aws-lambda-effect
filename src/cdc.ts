import { Effect, type Schema, SchemaParser } from 'effect'
import { Fault, forEachUow, type Uow } from './core.js'
import { DynamoDb, type Emittable, EventBus, type QueryRequest } from './services.js'

// MARK: rule — the flavor's contract; the body below is checked against it

export interface Query<Row> {
  /** Decodes each item. Also fixes `Row`, so hooks can be written in any order. */
  readonly rows: Schema.Decoder<Row>
  readonly toRequest: (uow: Uow) => QueryRequest
}

export interface CdcRule<Row, Out extends Emittable> {
  readonly id: string
  readonly eventType: string | RegExp
  /** Drop uows by content */
  readonly filter?: (uow: Uow) => boolean
  /** Keep only the latest uow per partition key */
  readonly compact?: boolean
  readonly query?: Query<Row>
  readonly toEvent: (uow: Uow & { readonly queryResponse: ReadonlyArray<Row> }) => Out
}

// MARK: flavor

/** Change data capture: select, optionally query, turn each uow into an event and publish it. */
export const cdc = <Row = never, Out extends Emittable = Emittable>(rule: CdcRule<Row, Out>) => ({
  id: rule.id,
  run: (uows: ReadonlyArray<Uow>) => {
    const selected = uows.filter(
      (uow) => matches(rule.eventType, uow.event.type) && (rule.filter?.(uow) ?? true),
    )

    return forEachUow(rule.compact ? latestPerKey(selected) : selected, (uow) =>
      Effect.gen(function* () {
        const queryResponse = rule.query ? yield* queryRows(rule.query, uow) : []
        const event = rule.toEvent({ ...uow, queryResponse })
        const bus = yield* EventBus
        yield* bus.publish(event)
        return { ...uow, queryResponse, event }
      }),
    )
  },
})

// MARK: steps

const queryRows = <Row>(query: Query<Row>, uow: Uow) =>
  Effect.gen(function* () {
    const dynamoDb = yield* DynamoDb
    const items = yield* dynamoDb.query(query.toRequest(uow))
    return yield* Effect.forEach(items, (item) =>
      SchemaParser.decodeUnknownEffect(query.rows)(item),
    ).pipe(
      Effect.mapError((issue) => new Fault({ reason: 'row does not match its schema', cause: issue })),
    )
  })

const matches = (pattern: string | RegExp, type: string) =>
  typeof pattern === 'string' ? pattern === type : pattern.test(type)

const latestPerKey = <U extends Uow>(uows: ReadonlyArray<U>) => [
  ...new Map(uows.map((uow) => [uow.event.partitionKey, uow])).values(),
]

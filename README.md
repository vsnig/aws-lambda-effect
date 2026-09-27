# aws-lambda-effect

Typed pipelines for AWS Lambda batch events (SQS, Kinesis, DynamoDB Streams), built on [Effect](https://effect.website).

> **Early preview (0.0.x).** The API will change. Requires `effect` 4, currently a release candidate.

Each record in a Lambda batch becomes a **unit of work** (uow) that runs through one or more pipelines. A pipeline is plain Effect code for one uow. The library runs it over the whole batch, sets failing records aside, and turns each failure into either a parked fault or a redelivery request.

The rule-and-flavor model is inspired by [aws-lambda-stream](https://github.com/jgilbert01/aws-lambda-stream), with types end to end and no stream library.

## Concepts

- **Uow**: the envelope for one record, holding the source record and its event.
- **Pipeline**: `{ id, run }`. `run` takes the batch and reports what succeeded, what faulted and what should be retried.
- **Flavor**: a function that turns a small **rule** object into a pipeline, for example `cdc(rule)`. The flavor types each hook's input, so rules need no annotations.
- **Fault or Transient**: `Fault` means retrying the same record will never help, so it is parked. `Transient` means it may succeed later, so it is returned in `batchItemFailures`. A defect (a bug that throws) is treated as a fault.

## A rule

```ts
import { Schema } from 'effect'
import { cdc } from 'aws-lambda-effect'

const Customer = Schema.Struct({ customerId: Schema.String, name: Schema.String })

export const orderCreated = cdc({
  id: 'order-created',
  eventType: 'order-created',
  query: {
    rows: Customer,
    toRequest: (uow) => ({ table: 'customers', key: String(uow.event.payload) }),
  },
  // uow.queryResponse is ReadonlyArray<{ customerId: string; name: string }>
  toEvent: (uow) => ({ type: 'order-enriched', names: uow.queryResponse.map((c) => c.name) }),
})
```

## A custom pipeline

When no flavor fits, write the pipeline directly:

```ts
import { Effect } from 'effect'
import { EventBus, Fault, forEachUow, type Pipeline } from 'aws-lambda-effect'

export const orderShipped: Pipeline<EventBus> = {
  id: 'order-shipped',
  run: (uows) =>
    forEachUow(
      uows.filter((uow) => uow.event.type === 'order-shipped'),
      (uow) =>
        Effect.gen(function* () {
          if (uow.event.payload == null) return yield* new Fault({ reason: 'empty payload' })
          yield* (yield* EventBus).publish({ type: 'shipment-notice', orderId: uow.event.id })
          return uow
        }),
    ),
}
```

## Running a batch

```ts
import { processBatch } from 'aws-lambda-effect'

// Effect<{ batchItemFailures: { itemIdentifier: string }[] }, never, DynamoDb | EventBus>
const program = processBatch([orderCreated, orderShipped], uows)
```

Every pipeline sees the whole batch. Faults are parked with `EventBus.publishFault`, and transient failures come back as `batchItemFailures` for Lambda's partial batch response.

`DynamoDb` and `EventBus` are Effect services. Provide them as layers: real ones wrap the AWS SDK, tests use fakes. This preview ships the service interfaces only, not AWS SDK implementations.

## License

MIT

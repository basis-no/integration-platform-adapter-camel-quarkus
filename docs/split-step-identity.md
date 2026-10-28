# Advisory: repeated sends that share one step identity

Status: known limitation in every 0.3.x release. Behaviour was checked against
the 0.3.1, 0.3.3, 0.3.4 and 0.3.5 tags and the 0.3.6 candidate, with the same
result on each. `SplitStepIdentityMatrixTest` pins the cases below.

## What can be lost

The adapter keys each external send as `<x-bip-message-id>:<step-id>`, where the
step id is the Camel node id of the `to`/`toD`. With a durable step store, a send
whose key is already `DELIVERED` is skipped so that a replay does not repeat a
side effect. The same check also applies to parts of a single attempt. A part
is **not sent** when all three of these hold:

1. `basis.message-store.step-outcome-store=postgres`. The library default
   `noop` never skips. Managed deployments with a Message Store normally use
   `postgres`.
2. The parts share one `x-bip-message-id`. That happens when the exchange has
   an id before the split:
   - every Kafka input route (the adapter assigns an id at route start if none
     is supplied);
   - an inbound `x-bip-message-id` header;
   - any tracked external send before the split, for example a JDBC or HTTP
     fetch in a polling route, because the adapter generates the id there.
3. The parts reach the same tracked send node, meaning any endpoint other than
   `bean`, `controlbus`, `dataformat`, `direct`, `language`, `log`, `mock`,
   `micrometer`, `seda`, `stub`, `timer`, `validator`, `vm`, `xslt` or
   `xslt-saxon`. The send can be direct, behind a `direct:` sub-route, or inside
   a multicast within the part.

Then only the first part to reach that node is sent. Later parts are skipped
without an error, the input completes, and a Kafka offset is committed. A
`toD` whose URI differs per part does not help, because the step id is the node
id. The aggregation strategy, `streaming`, `shareUnitOfWork` and
`stopOnException` make no difference. A `loop` that sends through one node
behaves the same way, with or without `copy()`. With `parallelProcessing`, the
parts race on the delivered check: all of them may be sent, or not.

Version 0.3.5 and earlier log nothing for a skipped send. From 0.3.6 each skip
is logged at INFO as `Step <id> SKIPPED for message <id>`. If this appears
without a replay, it marks this case.

## Not affected

- The `noop` step store.
- A non-Kafka route whose first tracked send is inside the split. Each part
  then generates its own id, which is logged as a warning.
- A part sent through a new exchange (`ProducerTemplate`) that does not copy
  `x-bip-message-id`, or that sets its own id.
- A part that sets its own `x-bip-message-id` before the send.
- Parts that only reach internal endpoints.

## Safe configuration today

- Before the send, give each part, or each record it publishes, its own
  `x-bip-message-id`. Derive it from the business record when replays must
  keep the same identity; a random id makes every replay send again. Set it
  inside the split or a branch pipeline. If a split's aggregation strategy
  returns a part's message (for example `useLatest`), restore the input's id
  afterwards, so that the input is journaled under its own id.
- Alternatively, publish each part through a new exchange with an explicit id.
- Do not depend on `parallelProcessing` to avoid the skip.
- Switching the integration to the `noop` store also avoids the skip. It
  removes replay protection for every step, so a replay repeats delivered side
  effects.

Changing the adapter's step identity is not part of 0.3.x. Existing
`bip_step_outcome` keys would no longer match, and replays of earlier messages
would repeat their sends.

# Advisory: repeated sends that share one step identity

Status: known limitation in every 0.3.x release. Behaviour was checked against
the 0.3.1, 0.3.3, 0.3.4 and 0.3.5 tags and the 0.3.6 candidate, with the same
result on each. 0.3.6 logs each lost part at ERROR and changes nothing else (see
[From 0.3.6: an ERROR for each skipped part](#from-036-an-error-for-each-skipped-part)).
`SplitStepIdentityMatrixTest` and `RepeatedStepDetectionTest` pin the cases
below.

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

Then only the first part to reach that node is sent. Later parts are skipped,
the input completes, and a Kafka offset is committed. A
`toD` whose URI differs per part does not help, because the step id is the node
id. The aggregation strategy, `streaming`, `shareUnitOfWork` and
`stopOnException` make no difference. A `loop` that sends through one node
behaves the same way, with or without `copy()`. With `parallelProcessing`, the
parts race on the delivered check: all of them may be sent, or not.

Multicast and recipient list branches that reach one send node behave the same
way. Version 0.3.5 and earlier log nothing for a skipped send.

## From 0.3.6: an ERROR for each skipped part

A send that is skipped because another part, iteration or branch of the same
attempt already sent or skipped its key is logged at ERROR, once per skipped
send:

```text
Step <step-id> was skipped in a <split part|loop iteration|multicast branch|recipient list branch>
of input route <route-id>, so this part was not delivered: another part of the same attempt
already sent or skipped this step key. In 0.3.x split parts, loop iterations and multicast and
recipient list branches keep the input's x-bip-message-id, so they share one step key. Give
each part its own x-bip-message-id, send each part through a new exchange with its own id, or
publish each part with its own id. See docs/split-step-identity.md
```

The message is one line. It names the step, the input route and the kind of
part, and contains no message content or header values.

- Detection only. Whether a part is sent, the input's outcome, retries, DLQ
  parking, offset commits and the recorded step outcomes are the same as in
  0.3.5. A Kafka input still commits; a polling route still marks its batch
  processed.
- The adapter tells parts apart by their position in the enclosing splits,
  loops, multicasts and recipient lists, and by the `direct:`, `seda:` or `vm:`
  call that started each of these. The same splitting sub-route called twice
  therefore keeps the two calls' parts apart. A part that meets its own step
  again is a normal skip at INFO (`Step <id> SKIPPED for message <id>`): Camel
  repeating a sub-route or a whole split within one attempt, and a redelivered
  or replayed input whose earlier attempt sent the step. In a redelivered input
  the first part to reach the step is logged at INFO and each later one at
  ERROR.
- With `parallelProcessing`, each part is either sent or logged at ERROR. When
  several parts sent the same key in parallel and Camel repeats the split, each
  of them skips its own send at INFO.
- The `noop` step store never skips, so it logs nothing.

Not detected, so these skips are logged at INFO only:

- One part that reaches the same send twice on its own, for example by calling
  one `direct:` sub-route twice.
- A loop inside a loop. After the inner loop, Camel leaves the inner loop's
  `CamelLoopIndex` on the exchange, so a send that follows it in the outer loop
  can look like the same iteration.
- A part sent through a new exchange that copies the input's
  `x-bip-message-id`: the new exchange does not see the other parts' sends.
- A split, loop, multicast or recipient list that runs again from the same
  call without a Camel retry, for example through a routing slip or a dynamic
  router. A part of the later run is compared with the part at the same
  position in the first run.
- A step key that the attempt last used more than 10,000 step keys earlier.
  The adapter keeps the 10,000 most recently used keys per attempt, so that a
  large split stays bounded in memory. A key that every part reaches stays
  known.

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
- Alternatively, send each part through a new exchange with its own id, or
  publish each part with its own id, for example as a Kafka record to the
  integration's own input, and send it from there.
- Do not depend on `parallelProcessing` to avoid the skip.
- Switching the integration to the `noop` store also avoids the skip. It
  removes replay protection for every step, so a replay repeats delivered side
  effects.
- Moving a route to per-part ids changes its keys. A replay of a message
  recorded before the change sends its first part once more.

0.4 gives each part and iteration its own step key; 0.3.6 is the last 0.3
release. Changing the adapter's step identity is not part of 0.3.x. Existing
`bip_step_outcome` keys would no longer match, and replays of earlier messages
would repeat their sends.

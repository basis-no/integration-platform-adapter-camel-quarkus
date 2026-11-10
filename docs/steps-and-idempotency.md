# Steps and Idempotency

## Why Step Tracking Exists

A message can perform an external write and then fail later. Retrying the whole
route can repeat that write. The adapter therefore assigns stable side-effect
identity and records successful delivery in a durable ledger.

For a side-effect step, the generated key is:

```text
<x-bip-message-id>:<stable-camel-node-id>
```

It is sent as both `x-bip-idempotency-key` and `Idempotency-Key`.

## Replay Roles

| Role | Replay behavior | Durable ledger |
| --- | --- | --- |
| `INTERNAL` | Always executes; internal routing or observability. | No |
| `REPEATABLE` | Always executes; authentication or read-only lookup. | No |
| `SIDE_EFFECT` | Skips an already delivered operation unless explicitly redone. | Yes |
| `OUTPUT_SIDE_EFFECT` | Skips only after restoring its durable output checkpoint. | Yes |
| `AMBIGUOUS` | Executes and logs a warning until explicitly classified. | No |

The default classifier recognizes common internal schemes and clear producer
schemes. HTTP `GET`, `HEAD`, `OPTIONS`, and `TRACE` are repeatable when the
method is explicit. Other HTTP operations, multi-operation components such as
S3, and unknown schemes remain ambiguous because their URI alone does not prove
whether they read or write.

Every `to` or `toD` send to an endpoint that is not an internal Camel endpoint
must have an explicit id: the application fails to start otherwise. Sends
through other EIPs, such as `wireTap`, `enrich` or `recipientList`, are not
checked. A generated id such as `to1` changes when a route is edited and repeats
across integrations, so a classification list that names it can stop matching,
or match another send, after an edit. `basis.message-store.step-ids.emergency-allow-generated=true`
is an emergency escape hatch that starts the application with a warning per send.

To upgrade without changing keys, give each send the id Camel generates for it
today, for example `id: to1`. The step key `<message-id>:to1`, the receiver's
`Idempotency-Key` and classification lists that name `to1` stay valid, so a
message processed again is still skipped where it was delivered. Any explicit id
is accepted, including one that looks generated. Generated numbers count per node
kind across all routes in the application, in the order Camel builds them, so
confirm them: start the unchanged application once with the emergency setting,
which logs each send's generated id, or list the step ids an integration has
recorded:

```sql
select o.step_id,
       regexp_replace(regexp_replace(o.endpoint_uri, '//[^/@]*@', '//xxxxxx@'),
         '(?i)((password|passphrase|secret[a-z_]*|[a-z_]*token|authpassword)=)[^&]*',
         '\1xxxxxx', 'g') as endpoint,
       count(*) as steps, max(o.occurred_at) as last_occurred_at
from bip_step_outcome o
where o.message_id in (select a.message_id from bip_message_attempt a
                       where a.integration = 'order-pipeline')
group by o.step_id, 2
order by o.step_id, last_occurred_at desc;
```

The query masks credentials again because rows recorded before adapter 0.4.0
kept more of the endpoint; see [Stored Endpoints](#stored-endpoints).

Renaming a generated id changes that step's key: replaying, redoing or resending
a message recorded before the change sends it once more. Rename only once no
such message will be processed again.

## Stored Endpoints

A step's endpoint is stored in `bip_step_outcome.endpoint_uri` and the
attempt's `steps_json`, and named in the adapter's warnings, with its
credentials masked as `xxxxxx`. The rule is the one the .NET adapter applies to
the steps it records, so both adapters store the same value for the same URI:

- what Camel's `URISupport.sanitizeUri` masks: the value of every query
  parameter whose name contains one of Camel's sensitive option names (such as
  `password`, `token`, `secret`, `apikey`, `user` or `clientId`), and the
  password in a `user:password@` part, including inside a parameter value;
- the whole user info of the endpoint's authority (`https://xxxxxx@host/…`),
  not only its password;
- the value of every query parameter with a credential-like name, by the rule
  the adapter uses for the headers it masks in a DLQ record: `Authorization`,
  cookies, authentication challenges, names containing secret, token, password,
  passwd, credential, session, signature, apikey, api-key, api_key,
  subscription-key, private-key or auth (such as `Signature` or
  `X-Amz-Credential`), or ending in `-key` or `_key` (such as
  `subscription-key`), but not `x-bip-*` or `Idempotency-Key`; also the names
  `key`, `code`, `sig` and `pwd`, and names containing `passphrase`,
  `passcode`, `privatekey`, `accesskey`, `accountkey` or `connectionstring`.
  Parameter names are compared after percent-decoding (an escape that is not
  valid UTF-8 is compared as written), ignoring case.

A run of masked characters becomes one `xxxxxx`. Keywords an application adds
with `URISupport.addSanitizeKeywords` are masked too (the .NET adapter has no
such setting). An endpoint the patterns cannot mask within about a second is
stored as `xxxxxx`. The route still sends to the real endpoint; only the stored
and logged copy is masked.

## Explicit Classification

Use stable node ids and classify exact ids when automatic classification is
not sufficient:

```yaml
- toD:
    id: fetch-access-token
    uri: "https://identity.example.test/token"
- toD:
    id: create-order
    uri: "https://orders.example.test/orders"
```

```properties
basis.message-store.step.repeatable-ids=fetch-access-token
basis.message-store.step.side-effect-ids=create-order
```

The four lists are `internal-ids`, `repeatable-ids`, `side-effect-ids`, and
`output-side-effect-ids`. Reusing an id in multiple lists fails startup. Do not
classify based on how an id sounds; classify according to the operation's real
side effect and replay contract.

## What the Ledger Prevents

With `basis.message-store.step-outcome-store=postgres`, ordinary replay checks
`bip_step_outcome` before a side-effect send. A prior `DELIVERED` record causes
the endpoint call to be skipped and the replay attempt records `SKIPPED`.

This is stronger than an idempotency header alone, but it is not distributed
exactly-once delivery. A process can crash after the receiver applies the
effect and before Postgres records `DELIVERED`. The receiver or business design
must still make the key safe, for example with a unique database constraint,
deterministic filename, stable event id, or API idempotency support.

## Sends in a Sub-Route

A tracked send in a route that another send calls, such as a `direct:`
sub-route, records its outcome once per try, like a send in the input route.
The calling `to("direct:…")` is internal and records nothing. When the
sub-route's dead-letter channel parks the message after the send failed, the
step stays `FAILED`, so a resend or replay sends it again. The same holds when
the sub-route has no error handler of its own and the input route's handler
retries the call.

**Known limitation of adapter 0.3.x** (seen on 0.3.5 and 0.3.6, fixed for
0.4.0): the calling send also recorded the sub-route's step when it completed.
A successful call is listed twice as `DELIVERED`. When the sub-route's
dead-letter channel parks the message, the calling send sees no failure and
records the step `DELIVERED` with no error: the attempt's steps list `FAILED`
for each try and then `DELIVERED`. A resend or replay then skips the send, so
the receiver never gets the message. To recover such a message on 0.3.x, or
after upgrading, use either of these:

- Resend it with the step in `redoSteps`, together with any step whose output
  it uses; see [Redo Steps](retry-dlq-replay.md#redo-steps).
- Check at the receiver whether it got the message before you treat the resend
  as delivered.

A message that 0.3.x parked keeps its `DELIVERED` row after the upgrade, so its
resend needs the redo too.

## When a Step Outcome Cannot Be Recorded

The adapter checks the ledger before a side-effect send and records the outcome
after it. When either fails, for example during a Message Store outage, the
send fails its exchange. A send that failed by itself keeps its own exception
unchanged: a receiver may throw one exception instance for every failure of a
kind.

A Kafka input whose attempt could not check or record a step outcome is neither
parked nor committed. It is recorded `FAILED`, and its exchange stays failed, so
the consumer reads the record again once Camel's retries are done. This also
holds when the route catches the failure, and when it happens in a split part,
loop iteration or branch, which run on copies of the input's exchange. Camel's
retries still run first, so a receiver can get the same send again; receivers
deduplicate on the stable `Idempotency-Key`. During a Message Store outage the
partition therefore waits at that input, as it does when parking fails. Alert
on inputs that stay `FAILED`.

A separate request, such as one sent with a `ProducerTemplate`, fails on its
own when one of its step outcomes is not recorded, also when its own route
caught the failure. It does not hold back the input that sent it. An input that
passes the request's failure on treats it like any other failure: it is
retried, then parked and committed. A replay of the parked input repeats the
request's send, which the receiver deduplicates on the `Idempotency-Key` only
when the request carries its own `x-bip-message-id`. Work that ends after the
input completed, such as a `wireTap`, and a request whose failed reply the
route ignores cannot hold the input back either.

A failing split part or multicast or recipient list branch is parked from
within the part, with the whole input, before or while the input's other parts
run. If a later part's step outcome is then not recorded, the input is recorded
`FAILED` and read again although a parked copy exists, and each attempt during
a Message Store outage can park it again. Nothing is lost: a replay of a parked
copy skips the delivered parts through the ledger. Parking once per input,
after its parts, is planned for 0.4 together with per-part step keys.

## Splitting Before a Side-Effect Send

A step key is `<x-bip-message-id>:<step-id>`. Split parts, loop iterations and
multicast and recipient list branches keep the input's message id, so parts
that reach the same side-effect send share one key. With
`basis.message-store.step-outcome-store=postgres`, only the first part is sent.
Each later part's send is skipped as already delivered, and that part's data
does not reach the receiver. The input still completes.

In the attempt that sends the first part, each such skip is logged at ERROR,
once per skipped send:

```text
Step <step-id> was skipped in a <split part|loop iteration|multicast branch|recipient list branch>
of input route <route-id>, so this part was not delivered: another part of the same attempt
already sent this step key. Split parts, loop iterations and multicast and recipient list
branches keep the input's x-bip-message-id, so they share one step key. Give each part its own
x-bip-message-id before the send, or publish each part with its own id. See
docs/steps-and-idempotency.md#splitting-before-a-side-effect-send
```

The message is one line. It names the step, the input route and the kind of
part, and contains no message content or header values. Only the log changes:
the skip, the input's outcome, retries, the configured failure mode and commits
are as before. A part's position is its index in each enclosing split, loop,
multicast and recipient list. The line is written only when a part at another
position sent the key in this attempt, and neither this part nor one of its own
parts did. Every other skip stays at INFO. That includes every part of a
redelivered or replayed input whose earlier attempt sent the key, because the
adapter cannot tell which part's data that attempt sent, and a part that meets
its own delivered send again when Camel repeats a sub-route or a whole split.
With `parallelProcessing()`, each part is either sent or logged at ERROR. A
branch or part whose data equals the sent part's is still logged at ERROR: its
own send did not happen. The `noop` store, and sends that are not classified as
side effects, never skip, so every part is sent and nothing is logged.

Not detected, so these skips are logged at INFO only:

- The input itself outside any part, and one part that reaches the same send
  twice on its own, for example by calling one `direct:` sub-route twice or
  after one of its own branches sent it.
- A split, loop, multicast or recipient list that runs again in the same
  attempt, for example a sub-route called a second time (also through `toD`),
  a `doCatch` that repeats the call, or a Camel retry. A part of the later run
  at a position that sent the key in the earlier run is taken to be that part;
  later positions are logged at ERROR.
- A send in an outer loop after a nested scope changed `CamelLoopIndex`, such
  as an inner loop: the outer iteration is no longer known for the rest of that
  loop. A key sent from such an iteration is not checked either.
- A part sent through a new exchange that copies the input's
  `x-bip-message-id`: the new exchange does not see the other parts' sends.
  Such a send may not be tracked at all; see below.
- A send made while Camel prepares a part (`onPrepare`), before Camel numbers
  it: its position is unknown. In a split inside another split's part, such a
  send takes the outer part's number, so the inner parts' later sends are not
  told apart either.
- Every skip after the attempt has used more than 10,000 step keys, and a key
  sent from more than 16 positions in one attempt. The record keeps at most
  10,000 keys and 16 positions per key, so it stays bounded in memory.

Safe forms, which send every part:

1. Give each part its own `x-bip-message-id` before the send, derived from its
   record so that a replay keeps the same key.
2. Publish each part with its own id, for example as a Kafka record to the
   integration's own input, and send it from there.

A part sent through a new exchange, for example with a `ProducerTemplate`, is
not a safe form on its own. Sent straight to the receiver's endpoint, the send
is not tracked: it has no step key, the receiver gets no `Idempotency-Key`, and
a replay sends it again. Sent to a route that contains the tracked send, such
as a `direct:` route, it is tracked under the new exchange's
`x-bip-message-id`. Without its own id, each send then gets a new random id, so
a replay sends it again too. In neither case is the part journaled, and the
input's journal row does not list its send.

After a route moves to per-part ids, its keys change: a replay of a message
recorded before the change sends its first part once more. Per-part step keys
are planned for 0.4.

## Output-Producing Side Effects

Use `OUTPUT_SIDE_EFFECT` when a write returns a body or metadata required by
later route steps:

```properties
basis.message-store.step.output-side-effect-ids=create-order
basis.message-store.step.output-checkpoint.header-names=Partner-Request-Id
basis.message-store.step.output-checkpoint.property-names=partnerStatus
```

The response body and selected JSON-compatible metadata are stored with the
delivered outcome. On replay, the adapter verifies and restores the checkpoint
before downstream logic continues. Missing, expired, corrupt, unsupported, or
policy-disabled output fails closed rather than continuing with the wrong body.

A checkpoint in object storage is read completely into memory, and its size
and SHA-256 are checked, before any of it is set on the exchange. A restore
therefore needs heap for the whole output, and for its decoded text or JSON
value as well. An output is stored in object storage only up to
`basis.message-store.payload.object-max-bytes` and at most 2 GiB less a few
bytes, the largest output a restore can check in memory; a larger output fails
its capture. The restored body has the type it was stored as, for an inline
checkpoint and an object-backed one alike: text as a `String`, a
JSON-compatible value as the value read back from JSON (for example a `Map`),
and bytes or a stream as `byte[]`.

Explicit redo executes the operation again with the same idempotency key and
replaces the checkpoint after success. Output checkpoints require the Postgres
step store; tiered payload mode keeps large response bodies replayable.

## Custom Classification

An integration with unusual component semantics may provide an
`@ApplicationScoped` `StepClassifier`. It replaces the complete default
classifier and receives endpoint URI, node id, HTTP method, headers, and
exchange context. Prefer exact built-in configuration for ordinary cases so
classification remains visible to operators and tooling.

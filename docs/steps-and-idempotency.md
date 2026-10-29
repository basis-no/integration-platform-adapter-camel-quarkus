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

Every endpoint send that is not an internal Camel endpoint must have an explicit
id: the application fails to start otherwise. A generated id such as `to1`
changes when a route is edited, repeats across integrations, and cannot be
named in the classification lists. `basis.message-store.step-ids.emergency-allow-generated=true`
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

Renaming a generated id changes that step's key: replaying, redoing or resending
a message recorded before the change sends it once more. Rename only once no
such message will be processed again.

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

Explicit redo executes the operation again with the same idempotency key and
replaces the checkpoint after success. Output checkpoints require the Postgres
step store; tiered payload mode keeps large response bodies replayable.

## Custom Classification

An integration with unusual component semantics may provide an
`@ApplicationScoped` `StepClassifier`. It replaces the complete default
classifier and receives endpoint URI, node id, HTTP method, headers, and
exchange context. Prefer exact built-in configuration for ordinary cases so
classification remains visible to operators and tooling.

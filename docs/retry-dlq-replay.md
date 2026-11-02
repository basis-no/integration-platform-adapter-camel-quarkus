# Retry, DLQ, and Replay

## Automatic Retry

The global route configuration applies unless a route selects a variant:

| Configuration | Redeliveries | Initial delay | Terminal behavior |
| --- | ---: | ---: | --- |
| default | 3 | 1 second | configured failure mode |
| `ip-slow-backend` | 6 | 5 seconds | configured failure mode |
| `ip-no-retry` | 0 | none | configured failure mode |
| `bip-ingress-normalization` | 3 | 1 second | propagate to source |

Retries use exponential backoff. Automatic redelivery does not increment the
platform attempt; it adds diagnostics to that attempt.

The count is Camel's redeliveries, not the requests a receiver saw. The HTTP
client inside Camel's `http` component (Apache HttpClient 5) also retries by
itself, within one Camel attempt: it repeats a request answered with 503 or 429
once after a short delay. Two 503 answers and then success can therefore record
one redelivery for three requests; with the default policy, a receiver that
keeps answering 503 can get eight requests before the message is parked. Set
`camel.component.http.automatic-retries-disabled=true` to leave every retry to
the platform's policy.

On Camel 4's `http` component, bound a receiver call with `responseTimeout`
and `connectTimeout`. Camel accepts some timeout options without applying them,
and the call then waits for the component's timeout, three minutes unless
configured. Checked on Camel 4.18.0 and 4.18.4:

| Option | Where | What Camel does | Use instead |
| --- | --- | --- | --- |
| `socketTimeout` (Camel 3) | any `http`/`https` URI | Sends it to the receiver as a query parameter | `responseTimeout`; on a dynamic send, `httpClient.responseTimeout` |
| `httpConnection.socketTimeout`, `.responseTimeout`, `.connectTimeout`, `.connectionRequestTimeout` | any `http`/`https` URI | Drops it without an error; only `httpConnection.soTimeout` applies | `responseTimeout`, `connectTimeout`, `connectionRequestTimeout`; on a dynamic send, the `httpClient.` form |
| `responseTimeout`, `connectTimeout`, `connectionRequestTimeout`, `soTimeout` | dynamic send: a `toD`, an `enrich`, or a `wireTap` whose URI contains `${…}` | Sends it to the receiver as a query parameter | `httpClient.responseTimeout`, `httpClient.connectTimeout`, `httpClient.connectionRequestTimeout`, `httpConnection.soTimeout` |

A dynamic send keeps only the options that Camel's endpoint metadata lists, and
the metadata lists these four timeouts as component options only, so the rest
go to the receiver. `allowOptimisedComponents(false)` on a `toD` or `enrich`
turns that off, at the cost of one endpoint per distinct URI. The `httpClient.`
forms, `httpConnection.soTimeout` and the component settings
`camel.component.http.response-timeout`, `connect-timeout`,
`connection-request-timeout` and `so-timeout` apply to every send. Some forms
fail on their own: `httpClient.socketTimeout` and `httpClient.soTimeout` when
Camel creates the endpoint, which is at startup for a `to` and at the first send
for a dynamic send, and `camel.component.http.socket-timeout` at startup.

The adapter logs a warning for each option in the table when it creates a route
with it in a `to`, `toD`, `enrich` or `wireTap` URI, once per route, step and
option. The warning names the option, the step and the replacement, never the
URI or a value. A URI in a `recipientList` or
`routingSlip`, or one whose scheme is only known at send time, is not checked.

A timeout does not mean the receiver did nothing. A receiver can apply the
effect after the client gave up, and the message is then retried and parked
although the effect happened. The step is recorded `FAILED`, so a replay sends
it again, with the same `Idempotency-Key` (`<message-id>:<step-id>`) as every
earlier try. Only a receiver that deduplicates on that key avoids a second
effect. Republishing the parked record under a new message id changes the key,
so even such a receiver applies it again.

## Terminal Kafka Failure

`basis.message-store.failure-mode=dlq` is required for Kafka lifecycle
boundaries. After retries are exhausted, the adapter sends the original
captured payload to `basis.message-store.dlq-uri`, preserves the envelope, adds
`x-bip-error` and `x-bip-dlq-at`, records `DLQ`, and only then commits the input
offset. A replay's `x-bip-redo-steps` is not carried; see [Redo Steps](#redo-steps).

The historical Kafka DLQ record remains after recovery. Postgres is
authoritative for current lifecycle state, so a later successful replay changes
the message to `COMPLETED` while retaining both attempts and replay audit.

`record-failed` is reserved for explicit non-Kafka audit-only boundaries. It
records `FAILED` and propagates the exception to the source; using it on a Kafka
boundary is rejected because it would remove the recovery path.

## Manual Replay

The adapter exposes:

```http
POST /admin/messages/{messageId}/resend
```

The replay service validates the stored inline payload or S3 claim check,
writes a `bip_message_replay_audit` row, then publishes to the configured input
topic. It retains the message id and envelope, sets `x-bip-replay=true`, and
sets `x-bip-attempt` to the previous attempt plus one.

The dashboard supplies operator identity and reason through headers or the JSON
request body. The default target pattern is `bip.%s.in`, where `%s` is the
stored integration name.

Replay is a new Kafka record appended to a partition; it does not move the
original record back to its historical offset. Kafka ordering is guaranteed
only within a partition, and bulk replay needs a separately defined ordering
policy.

## Redo Steps

Ordinary replay skips side effects already recorded `DELIVERED`. An operator
may intentionally re-execute selected steps by supplying `redoSteps`. The
adapter publishes the JSON array in `x-bip-redo-steps` and stores it in replay
audit. A listed step id or full idempotency key bypasses the delivered-step
skip.

Redo deliberately repeats an external effect. It is safe only when the
receiver honors the stable idempotency key or repetition is acceptable.

The list stays inside the integration. An input that the Message Store records
reads `x-bip-redo-steps` before the route runs, keeps the list for the attempt's
step tracking and removes the header. On other inputs the route's first `to` or
`toD` does the same. HTTP sends, Kafka publishes and DLQ records therefore do
not carry it; otherwise a receiver would get the list, and a downstream
integration that consumed a published record with the same message id would
send its own steps with the same ids again. `x-bip-replay` and `x-bip-attempt`
are still forwarded; nothing in the adapter acts on `x-bip-replay` at a Kafka
input.

## Contract Startup Gate

With the Postgres sink, startup reads the highest contract major from
`bip_contract`. A different major fails startup. A missing table or empty row
uses the contract-v1 rollout grace with a prominent warning; other database
errors are not treated as a missing schema.

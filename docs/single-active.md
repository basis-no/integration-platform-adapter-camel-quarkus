# Single-Active Routes

A production integration runs several replicas so that node maintenance, a
node failure or a rollout never stops it. That is safe for sources that share
work among their consumers: Kafka assigns partitions within a consumer group,
a gateway balances HTTP requests, a broker hands each queue message to one
consumer. It is not safe for work a route starts itself. A timer, a cron
trigger, a directory or (S)FTP poll, or a database query runs on every replica,
so three replicas fetch, publish and post everything three times, and even one
replica overlaps with its successor during every rollout.

The adapter therefore lets every polling route do its work on one replica at a
time, coordinated through a lease in the integration's
[Key-Value Store](key-value-store.md). No route change is required. A route
that must run on every replica is listed in
`basis.message-store.single-active.excluded-routes` (see
[Running a Polling Route on Every Replica](#running-a-polling-route-on-every-replica)).

It needs a Message Store provisioned with `bip_key_value`; without it, or
without a Key-Value Store at all, routes work on every replica as before (see
[Without a Key-Value Store](#without-a-key-value-store)).

## Which Routes Are Coordinated

A route is coordinated when its consumer polls on a schedule:

- its endpoint is a Camel `ScheduledPollEndpoint`: `file:`, `ftp:`, `sftp:`,
  `ftps:`, `sql:`, `jpa:`, `mail:`/`imap:`, object stores and queue-like
  services consumed by polling such as `aws2-s3:`, `aws2-sqs:`,
  `azure-storage-blob:`, `azure-storage-queue:`, `minio:`, and `scheduler:`; or
- its scheme is `timer:`, `quartz:` or `cron:`, except a `timer:` with a
  finite `repeatCount` (see [Finite Timers](#finite-timers)).

Property placeholders in the `from` URI are resolved first. The work is gated
by the consumer: a scheduled-poll consumer by its polls, a timer, Quartz or
cron trigger by its exchanges. A route on a polling endpoint whose consumer is
neither refuses to start, naming the route: the adapter could not hold its
polls back, and stopping its exchanges would drop items the consumer has
already taken. List such a route in
`basis.message-store.single-active.excluded-routes` to run it on every replica. Every other route
is left alone: `kafka:`, `platform-http:`, `jms:`/`sjms2:` queues,
`azure-servicebus:` and other push consumers, `direct:` and `seda:`, and a
`master:` route, which coordinates itself through the cluster service the
integration configures (the adapter registers none).

Note that a queue consumed through a polling endpoint, such as SQS, becomes
single-active as well; the lease removes duplicates, not the throughput of a
competing consumer. Use a broker consumer where several replicas should share a
queue.

Give polling routes explicit, stable ids. The lease is named after the route
id, and a generated id such as `route1` can differ between image versions, so
the old and the new pod of a rollout would hold different leases and poll at
the same time.

## How It Works

Coordinated routes start and stop exactly as Camel would without the adapter:
they are started on every replica, `autoStartup=false` keeps them stopped
everywhere, and Camel reports them running everywhere. A standby replica
therefore still starts its consumers: a `file:` consumer creates its
directories, an `ftp:`/`sftp:` consumer connects (and with `autoCreate` creates
the remote directory) without polling. The lease gates the work inside the
route:

- For a `ScheduledPollEndpoint` consumer the poll is the work. The adapter
  wraps the poll strategy of the route's own consumer (keeping any strategy
  the route configured) so that no poll begins on a replica that does not hold
  the lease: nothing is read, no empty message is sent and nothing is
  committed. A poll that has begun is work in flight and always runs to its
  end. The endpoint itself is untouched, so a `pollEnrich` or a polling
  consumer on the same endpoint is not gated. A consumer configured with
  backoff counts the skipped polls as idle polls. A gate whose route has no
  lease registration, which should not happen, fails closed and warns once.
- For a trigger consumer (`timer:`, `quartz:`, `cron:`) the exchange is the
  work. A trigger that fires on a replica without the lease stops its route
  before the first processor, so nothing runs; a trigger has no source to
  acknowledge, so nothing is lost but that tick. The trigger itself still fires
  on every replica, so ticks skipped on a standby are not repeated after a
  handover. A trigger that fires while this replica is still acquiring the
  lease (at startup, or right after the route was added or started) waits up
  to 5 seconds for that attempt and runs if it succeeded; a tick still skipped
  after that wait is lost. The wait runs on the timer or Quartz thread that
  fired the trigger, so that thread fires nothing else meanwhile. The first
  skipped tick of each activation is logged at INFO, later ones at DEBUG.

### Finite Timers

A `timer:` with a finite `repeatCount`, such as a one-shot initialization
timer `timer:init?repeatCount=1`, is not coordinated: it runs on every replica,
as before, and startup logs a WARN naming it (at INFO with one declared replica
or when the route is listed in the exclusion property, which it may be). Under a lease its few ticks
could be dropped for good: on the new pod of a rolling update the tick fires
while the old pod still holds the lease, and a timer does not fire it again.
Make such work idempotent across replicas (for example with a
`putIfAbsent` marker in the Key-Value Store), or use a recurring timer. A
Quartz or cron trigger with a finite repeat count is coordinated like any
other trigger; list it in `basis.message-store.single-active.excluded-routes`
if it must not be dropped.

The lease follows the route's life as Camel runs it. A lease is acquired once
Camel has started the route, whatever decided that: the route's or the
context's `autoStartup`, an `autoStartupExcludePattern`, or an operator
starting the route by hand; a route Camel leaves stopped, or whose consumer
fails to start, takes no lease and blocks no other replica. Every start of a
route is a new lease on this replica; what an earlier start still has in
flight cannot affect it. Stopping or suspending a coordinated route on a
replica, by hand or at shutdown, releases its lease once the work in flight
has finished (and once a Key-Value Store call still in flight has settled,
bounded by the store's 5 second statement timeout), so another replica takes the work over;
starting or resuming the route acquires a lease again.

Each replica tries to create the Key-Value Store key `lease/<routeId>` with the value
`{"holder": "<pod name>", "acquiredAt": "<time>", "acquisition": "<id>"}`,
where `acquisition` is a random identifier of that attempt so no two values
are ever equal, and a 30 second expiry, once the route has started and every
10 seconds after, and reads it back: it holds the lease only if the key holds
exactly the value it wrote. The holder
opens the gate and renews the lease 10 seconds after the last successful
renewal was sent: it reads the lease, and if it is absent or holds another
value it has lost it and closes the gate at once; otherwise it compares-and-sets
at the version it just read. A read or renewal that fails, because the database
is unreachable, is retried every 2 seconds, so one lost round trip changes
nothing; and a renewal that committed but whose reply was lost is simply found
at the next read. A holder closes the gate 20 seconds, by its own monotonic
clock, after it sent the write that last set the expiry (the compare-and-set of
its last successful renewal, or the `putIfAbsent` of its acquisition), whatever
the database is doing, so a replica that cannot reach the database never
outlives its lease, which the database expires 30 seconds after that write by
its own time; a write whose reply never arrives counts from its send in the
same way. Closing the gate is a flag: it waits for nothing and takes no lock.
The Key-Value Store bounds each of its statements to 5 seconds, so a lease call
blocked behind a lock fails instead of hanging past the deadline. The timeout
bounds the execution of a statement, not the acquisition of a connection from
the pool, which the datasource's own acquisition timeout bounds. On an orderly shutdown Camel
stops the routes and lets their work in flight finish first; the replica then
reads the lease and deletes it only if it still holds the value it wrote, so
another replica takes over at its next attempt and a lease someone else
created meanwhile is left alone.

No lease is attempted before the adapter's startup check of the Message Store
(`bip_contract` and the tables the adapter writes, see
[Retry, DLQ, and Replay](retry-dlq-replay.md)) has passed, and none ever is
when it fails: the routes are started before that
check runs, so until it passes every coordinated route's gate stays closed,
on every replica, and the uncoordinated fallback does not apply either. The
first ticks after startup are therefore skipped; the leases are attempted the
moment the check passes.

A polling route with `autoStartup=false`, including
`autoStartup: "{{poll.enabled}}"` resolving to false, therefore takes no lease
while it is stopped. Starting it by hand on a replica makes that replica
compete for the lease like any other.

The timing is fixed and is not configurable. The holder is the pod
name (`HOSTNAME`), or an identifier the process generates once when that is
unset.

## Seeing the Holder

The holder is the Key-Value Store key `lease/<routeId>` of the integration:
its value names the replica, its version counts the renewals, and its expiry is
30 seconds after the last renewal. Read it like any other key, for example
`KeyValueStore.get("lease/poll-orders")`, or in an operator's read of the
integration's keys. Do not write `lease/` keys from the application.
Deleting the key hands the work over: the holder closes its gate at its next
renewal and one replica, possibly the same, acquires a new lease.

With a Micrometer registry in the application, the gauge
`integration_single_active_lease{route="<routeId>"}` reports each coordinated
route on this replica: `1` while it holds the lease, `0` while it waits, `2`
while it runs uncoordinated without a Key-Value Store, and `3` while it needs
its lease and stays stopped because there is no Key-Value Store. Exactly one
replica should report `1` for a route; if no replica reports `1` or `2` for a
route, nobody runs it. The gauge of a route that is removed is removed with it.

The adapter logs the protocol with the route id and the holder: at INFO when a
lease is acquired or released, at WARN when a route's gate closes because the
lease was lost or could not be renewed in time, and at ERROR when a lease
cannot be written because the Key-Value Store refuses the write. Lease values
and versions are the only Key-Value Store data the adapter logs.

## Failover Times

- After an orderly shutdown or a rollout, another replica takes over at its
  next attempt: within about 10 seconds.
- After a crash, the lease must expire first: up to 30 seconds, then up to
  10 seconds until the next attempt.
- A poll that falls in a handover runs at the next poll. A cron trigger or a
  timer tick that falls in a handover is skipped, as it is when a single
  replica restarts.
- Work in flight when a gate closes finishes on the replica that started it,
  bounded on shutdown by Camel's shutdown timeout, so a handover can overlap
  by that time. Sources stay at-least-once.

### Overlap While Work Is in Flight

The lease decides when work may begin, not when it ends. A poll that began
while this replica held the lease runs to its end even if the lease is lost
meanwhile, for example while the database is unreachable for longer than the
20 second deadline; the next holder may then begin its own poll after the
lease expired, 30 seconds after the last renewal, while the first one is still
working. A long batch, such as a directory poll that picks up many files at
once, makes that overlap likely. To keep it harmless:

- bound each poll, for example with `maxMessagesPerPoll` on a `file:` or
  `ftp:` consumer, so a poll ends well within the deadline;
- use the source's own claim where it has one, such as a file read lock
  (`readLock=changed` or `markerFile`) or a `move` on completion, so two
  replicas never take the same item;
- make the work idempotent, for example with `KeyValueIdempotentRepository`
  (see [Key-Value Store](key-value-store.md#deduplicating-input)).

The gate also evaluates the holder's deadline every time it is consulted, so
it never reads open after the deadline. The lease has no fencing token: a
holder whose process pauses (a long garbage collection, a frozen container)
between consulting the gate and starting its work can overlap with the next
holder by the length of that pause.

Leases are ordinary Key-Value Store keys: each costs one read and one write
every 10 seconds (more while a holder retries a failed renewal) and counts
toward the integration's [limits](key-value-store.md#limits-and-errors).

## Running a Polling Route on Every Replica

A route that is meant to run on every replica, such as a timer that refreshes a
local cache or reports the replica's own health, is listed by id in
`basis.message-store.single-active.excluded-routes` (comma-separated; see
[Configuration](configuration.md)). An excluded route gets no lease, never
touches its `lease/` key, and runs on every replica as Camel runs it; the
adapter logs it once at INFO. Startup refuses an excluded id that names a route
the adapter does not run under a lease (a Kafka, HTTP or `direct:` route, for
example), and logs a WARN for an id that names no route at startup: such an id
applies to a polling route with that id that is added later, for example from a
route template, but a typo would leave the intended route coordinated, so check
the WARN. An excluded polling route counts as uncoordinated for the replica
check of ingress normalization routes (see
[Ingress and Scaling](ingress-and-scaling.md#singleton-polling)).

## Without a Key-Value Store

When the Message Store has no `bip_key_value` table, or the integration has no
Message Store datasource, an inactive one (`quarkus.datasource.active=false`) or
no `app.name` (so it has no Key-Value Store), polling
routes work on every replica, as earlier adapter versions do, and the adapter
logs one warning per route. Only these two conditions open the gate without a
lease; any other failure to create or call the Key-Value Store keeps it closed.
The adapter checks again at every attempt. The first Key-Value Store call that
succeeds proves the table exists: the gate closes at once and the route waits
for its lease like any other, so a replica that was working uncoordinated stops
as soon as the table appears and resumes only when it holds the lease.

The exception is an ingress normalization route that the replica check
accepted only because its lease coordinates it, that is, with several or no
declared replicas (see
[Ingress and Scaling](ingress-and-scaling.md#singleton-polling)). Running it on
every replica would duplicate its source, so without a Key-Value Store it does
no work, on every replica, and the adapter logs an ERROR naming it; it runs
once the table exists.

An unreachable database is not this case. A replica that cannot reach the
Message Store keeps the gates of its polling routes closed and keeps trying.

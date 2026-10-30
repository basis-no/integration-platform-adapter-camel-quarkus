# Maintenance 0.3.4 qualification — 2026-10-01

Status: runtime source qualified for stable publication; application rollout is
a separate decision. Publication is established by the release workflow and
artifact availability, not by this document.

## Scope

- Base: adapter 0.3.3 maintenance line, including its Kafka security binding.
- Candidate: 0.3.4-SNAPSHOT; Java 21, Quarkus 3.32.2, Camel 4.18.0.
- No schema migration, topic purge, offset reset or automatic historical replay.
- Kafka source boundaries only; internal request rows are no longer enrolled.
- Historical replay requires a matching latest attempt and compatible current input.
- Confirmed DLQ send precedes parked status; durable outcome precedes offset commit.
- No commit after unparked failure; stop-on-error preserves blocked input before subsequent records.
- Internal parking is excluded from the business-step ledger.

## Reproducible local checks

Use an actual JDK 21 (`java -version`, `./mvnw -version`) and local Docker.

```sh
./mvnw install
./mvnw -f integration-tests/maintenance-smoke/pom.xml test
```

`install` only places the snapshot in the local Maven cache. It does not publish.
Tests use synthetic data and disposable local Kafka/Postgres containers.

The suite covers valid Kafka replay with original bytes/headers, pre-upgrade
internal rows, missing or inconsistent attempts, changed/regex topics and
cross-integration rejection. Failure tests cover nested request propagation,
original-payload parking, catch-all handler interference, failed DLQ publication
and failed journaling. Real Kafka tests inspect broker-visible committed offsets
and processing order with both one-record and multi-record polls. PostgreSQL
persistence/step-ledger tests run separately against real PostgreSQL.

The smoke fixture boots the library through Quarkus CDI rather than manually
registering its policies. Its Kafka source is stopped and its broker address is
an unavailable loopback endpoint: it checks startup, dependency defaults and
internal failure propagation, not connectivity to a live environment.

## Application compatibility and limits

- Upgrade code, dependency and recovery ownership together. An application
  catch-all can still leave an input unparked; refusing to commit it is safer
  than claiming that it was handled, but can keep the partition blocked.
- Internal request processors must retain the input's platform message identity
  and attempt headers when copying a receiver response.
- Internal failures propagate with their original exception type and message;
  the unmanaged-input parking guard must not obscure receiver diagnostics.
- Shared input splits need `shareUnitOfWork`; nested request recovery must not
  hide their failures or multiply retries.
- Previously acknowledged external effects can still repeat after a crash or
  whole-input replay unless a stable durable step/idempotency design covers
  those effects. This patch does not introduce transaction-level deduplication.
- No blanket HTTP 500 policy, poll-timeout guarantee, memory tuning, queue
  cleanup or DLQ dismissal feature is included.
- Replay refusal does not retroactively remove the old dashboard resend button.
- Concurrent resend serialization remains outside this maintenance patch.

## Before publication and deployment

Review the candidate, choose released coordinates, run CI and refresh the
compatibility handbook/release snapshot. Carry the parking/replay regression
requirements into the next adapter line without overwriting its active work.
Publish and activate only under separate authorization. Inspect the application's
effective error handlers, broker defaults, bounded receiver timeouts and poll
budget. Confirm a successful input and a controlled failed input followed by a
good one in the target runtime. Verify original payload, message identity,
parking and consumer progress; record the exact image and adapter versions.

## Snapshot publication

The maintenance line uses the same snapshot publishing mechanism as development;
snapshot support is not restricted to the highest version. `0.3.4-SNAPSHOT` and
`0.4.0-SNAPSHOT` are separate Maven coordinates. Snapshot publication does not
create a stable release tag or activate any integration.

1. Complete local checks and CI for the exact candidate commit. Review changes
   and obtain publication authorization before running the workflow.
2. Ensure the Sonatype namespace has snapshots enabled and the repository's
   existing Central token and signing secrets are configured. Never copy these
   secrets into source or operator commands.
3. Dispatch **Publish Maven SNAPSHOT** (`publish-snapshot.yml`) on the reviewed
   branch/ref. Check that the run's `head_sha` is the reviewed commit. The workflow
   tests and publishes the POM's `-SNAPSHOT` version; it rejects stable versions.
4. Verify the published timestamped snapshot from Sonatype metadata and record
   the workflow URL, source commit, resolved version and downloaded artifact hash.
5. The synthetic validation application's POM must explicitly enable consumption:

   ```xml
   <repository>
     <id>central-portal-snapshots</id>
     <url>https://central.sonatype.com/repository/maven-snapshots/</url>
     <releases><enabled>false</enabled></releases>
     <snapshots><enabled>true</enabled></snapshots>
   </repository>
   ```

6. Build the validation application with `./mvnw -U clean verify`, retaining its
   resolved dependency provenance and final immutable image digest. Isolate the
   validation application's inputs, group and DLQ from business traffic. A later
   rebuild of the same snapshot coordinate can resolve a newer artifact; the
   already-built image does not change.
7. Complete controlled runtime acceptance and the applicable contract gate before
   publishing the stable patch release. Customer application rollout remains a
   separate action and should pin that final release, not this snapshot.

See [Sonatype snapshot documentation](https://central.sonatype.org/publish/publish-portal-snapshots/)
for namespace enablement, consumption and retention. Snapshot availability alone
does not certify behavior. No test requires or authorizes access to a customer
receiver or resetting an existing consumer group's position.

The [isolated acceptance fixture](../integration-tests/maintenance-smoke/README.md)
provides the synthetic receiver, runtime bindings and operator checks. Its local
startup tests do not replace target-runtime acceptance.

## Qualification result

Runtime source: `ba69aab61e5e8a95e0bd0b077766a9ad77af8bd5`.
Published acceptance artifact: `0.3.4-20261001.065729-2`.
Later qualification-document changes do not change the tested runtime source.

- Exact-source CI passed the adapter suite, Quarkus startup fixture and API docs.
- Contract `1.0.0-rc2`: Camel passed S1–S22 (22/22) against real local Kafka and
  PostgreSQL. Released .NET `0.3.1`, source
  `318c72a6ce4c0c5bbaf6635341b9b06854c64f02`, passed 22/22 in the same run.
  Cross-runtime comparison found no differences after documented normalization.
- Harness source tree: `21f233b015d5e0ef890c2046b510ecc08ab5b0a7`.
- Isolated deployed-runtime acceptance passed input success/failure/success,
  original rejected payload retention, restart without a repeated failed attempt,
  and one manual resend completing the same message as attempt 2. Attempt 1
  remained in history; the recovered message left the current DLQ filter.
  The validation application was stopped and temporary resend permission revoked.
- Local packaged-artifact acceptance additionally refused a historical internal
  request replay before audit or publication.

The contract suite retains its documented runtime differences. S14 checks
survival and subsequent progress for malformed transport metadata; it does not
require identical parking. In this run Camel created no lifecycle/DLQ row for
that poison input, while .NET completed it. Do not describe qualification as a
promise that every malformed record appears in DLQ. Concurrent resend ordering
is also outside this patch's guarantees. The schema-upgrade regression passed,
but this adapter change requires no schema migration.

This evidence does not qualify a specific business receiver, its mapping, input
size, timeouts, deployment memory, or all applications using this dependency.

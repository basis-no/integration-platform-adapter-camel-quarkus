# Repository Instructions

This repository implements the Camel Quarkus runtime adapter for the Basis
Integration Platform. These instructions apply to developers and coding
agents.

## Sources of truth

- The public integration-platform contract is normative for cross-runtime
  message-store behavior, schemas, SQL, and certification scenarios.
- This repository owns Camel-specific implementation, configuration, examples,
  limitations, and tests.
- The public platform handbook owns user-facing guidance, the certified adapter
  version matrix, and release snapshots.

Do not change cross-runtime semantics in this repository alone. Update the
contract first or as part of the same coordinated change.

## Keep documentation synchronized

A behavior or configuration change is not complete until its documentation is
current. For every user-visible adapter change:

1. update this repository's `README.md` and any relevant local documentation;
2. update the public platform handbook's living guidance;
3. update the handbook adapter-version matrix when package, contract,
   certification, defaults, or limitations change; and
4. update a release snapshot when the certified component set or visible
   product behavior changes.

Document current, tested behavior as supported. Keep proposals and future
designs clearly labeled, and never describe a published package as certified
until the coordinated certification baseline says so.

`docs/configuration.md` is the canonical adapter property reference. Keep every
`basis.message-store.*` property there with its environment mapping, supported
values or default, and ownership guidance. `DocumentationCoverageTest` enforces
property coverage and local Markdown links; the Maven build enforces warning-free
public Javadocs.

## Public repository boundary

Use provider-neutral examples. Do not add customer names, repositories,
domains, payloads, topology, credentials, private endpoints, or internal
incident details to tracked files.

## Engineering and verification

Follow the existing architecture and keep the public API small. Preserve the
manual Kafka commit boundary: a consumed offset is committed only after the
required durable outcome and DLQ work succeeds.

Do not apply Kafka recovery semantics to non-Kafka sources. Automatic boundary
selection is Kafka-only. The HTTP audit-only preview requires an explicit route
id, `basis.message-store.failure-mode=record-failed`, and replay disabled. It
records terminal failures and propagates them to the source; it never commits a
Kafka offset or claims a Kafka DLQ/replay path. Keep this preview clearly
separate from certified contract v1.

Classify endpoint sends by replay semantics, not by customer terminology or
node-id naming conventions. Internal and repeatable sends must execute on every
replay. Only durable side effects may use the delivered-step ledger and be
skipped. Unknown or ambiguous operations must execute with a warning until an
integration explicitly classifies them; never silently skip an ambiguous call.
Use stable Camel node ids in provider-neutral examples.

Run the Maven wrapper rather than a global Maven installation:

```sh
./mvnw test
./mvnw -DskipTests javadoc:javadoc
```

Scale testing to the behavioral change. Contract-facing changes must also be
validated through the shared cross-runtime certification suite before release.

## Release notes and completion

Every stable version requires a dated, reviewed section in `CHANGELOG.md` and a
GitHub Release from the same existing tag. Include changes/fixes, compatibility,
upgrade actions and known limitations. Do not present planned behavior as shipped.
A release is complete only after Maven Central publication and artifact verification,
GitHub release-note readback and the applicable qualification/documentation gates.
Candidate inspection retains unsigned evidence only; production signing,
snapshot publication and portal automation are suspended pending credential
isolation. Standalone signed-bundle recovery tools retain deployment identity
but do not authorize re-enabling credential-bearing workflows. The release-page workflow verifies actual public
bytes before creating notes. Tags never trigger another build/upload. The
synthetic CI gate does not replace full candidate, trusted production signer or
portal qualification before release. Never rebuild or republish packages merely to repair missing release notes. Historical backfills use verified
existing tags and must not replace the latest release. See `docs/releasing.md`.

## Production credential boundary

Candidate inspection is unsigned; snapshot publication and candidate portal
actions are suspended. Never import production signing keys or pass production
credentials to Maven, wrappers, plugins, tests, dependencies or candidate code.
Restoring automation requires an enforceable approval boundary and isolated
trusted signing/publication of prebuilt, reviewed bytes. Workflow prose, an exact
SHA, manual dispatch and synthetic tests are not such a boundary. See
`docs/release-packaging.md#production-credential-hold`; do not bypass the hold
through historical workflow revisions.

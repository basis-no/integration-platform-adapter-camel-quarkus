# Repository Instructions

This repository implements a Camel Quarkus adapter that gives integrations
message-store auditing, Kafka recovery and controlled replay. These
instructions apply to developers and coding agents.

## Sources of truth

- This repository owns the adapter's implementation, configuration, examples,
  limitations, tests and documentation, including the message-store schema it
  expects in `src/main/resources/db/message-store/`.
- The .NET adapter implements the same message-store tables, topics and
  headers. Keep the two adapters compatible.

Do not change shared message-store semantics, schemas, SQL, headers or topics
in this adapter alone. Make the matching change in the .NET adapter in the same
coordinated change, or record why it does not apply.

## Keep documentation synchronized

A behavior or configuration change is not complete until its documentation is
current. For every user-visible adapter change, update this repository's
`README.md`, the relevant pages in `docs/`, and `CHANGELOG.md`.

Document current, tested behavior as supported. Keep proposals and future
designs clearly labeled, and never describe unreleased behavior as released.

`docs/configuration.md` is the canonical adapter property reference. Keep every
`basis.message-store.*` property there with its environment mapping, supported
values or default, and ownership guidance. `DocumentationCoverageTest` enforces
property coverage and local Markdown links; the Maven build enforces warning-free
public Javadocs.

## Public repository boundary

Use provider-neutral examples. Do not add customer names, repositories,
domains, payloads, topology, credentials, private endpoints, or internal
incident details to tracked files.

The adapter is generic open source. Describe its behavior in its own terms and
do not refer to a particular product, service, dashboard, or documentation set
that uses it. Existing identifiers such as the Maven coordinates, packages,
`x-bip-*` headers and `bip_*` tables keep their names; refer to them as this
adapter's identifiers.

## Engineering and verification

Follow the existing architecture and keep the public API small. Preserve the
manual Kafka commit boundary: a consumed offset is committed only after the
required durable outcome and DLQ work succeeds.

Do not apply Kafka recovery semantics to non-Kafka sources. Automatic boundary
selection is Kafka-only. The HTTP audit-only preview requires an explicit route
id, `basis.message-store.failure-mode=record-failed`, and replay disabled. It
records terminal failures and propagates them to the source; it never commits a
Kafka offset or claims a Kafka DLQ/replay path. Keep this preview clearly
separate from the supported Kafka lifecycle.

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

Scale testing to the behavioral change. A change to shared message-store
behavior must also be tested for compatibility with the .NET adapter before
release.

## Release notes and completion

Every stable version requires a dated, reviewed section in `CHANGELOG.md` and a
GitHub Release from the same existing tag. Include changes/fixes, compatibility,
upgrade actions and known limitations. Do not present planned behavior as shipped.
A release is complete only after Maven Central publication and artifact verification,
GitHub release-note readback and the applicable qualification/documentation gates.
The manual candidate workflow validates notes and signed bytes before
upload; portal validation and publication are separate reviewed operations with
durable deployment identity. The release-page workflow verifies actual public
bytes before creating notes. Tags never trigger another build/upload. The
synthetic CI gate does not replace full candidate, trusted production signer or
portal qualification before release. Never rebuild or republish packages merely to repair missing release notes. Historical backfills use verified
existing tags and must not replace the latest release. See `docs/releasing.md`.

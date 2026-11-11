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

## Issues and pull requests

Every problem or feature has an issue before its pull request. A pull request
links the issues it addresses with `Closes #N`, or `Refs #N` when it does only
part of one, and says briefly how it resolves them. A keyword closes an issue
only when the pull request merges into the default branch; close the issue of a
release-branch pull request by hand once its work is done. Write issues under
the same public repository boundary as everything else here. Automated
dependency updates need no issue. See `CONTRIBUTING.md`.

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
selection is Kafka-only. The failure handling follows the input type: a Kafka
boundary parks in the DLQ, every other input hands its failure back to its
source. The HTTP audit-only preview requires an explicit route id and is never
replayable, whatever the global replay setting says. It records terminal
failures and propagates them to the source; it never commits a Kafka offset or
claims a Kafka DLQ/replay path. Keep this preview clearly
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
A `vX.Y.Z` tag starts `release.yml`, which builds an unsigned candidate and then
waits for human approval in protected environments before signing, uploading and
publishing; the alternative is offline signing on the release workstation (see
below). A reviewed snapshot is published only through the approval-gated,
manual snapshot lane. The release-page workflow verifies
actual public bytes before creating notes. The synthetic CI gate does not replace
full candidate review, the signer's receipt or portal validation before release.
Never rebuild or republish packages merely to repair missing release notes.
Historical backfills use verified existing tags and must not replace the latest
release. See `docs/releasing.md`.

## Production credential boundary

The primary (certify) release key never exists in GitHub, CI, agent or development
hosts. Signing and Central uploads happen only in these places:

- `release.yml` jobs `sign-upload` and `publish`, which run in the protected
  environments `release` and `release-publish` (required reviewers, tag-only
  deployment rule), check out only the release tools at `TRUSTED_TOOLING_SHA` and
  never run Maven, the wrapper, tests or candidate files. Only their environment
  secrets hold the CI signing subkey and a Central token. This path exists only
  once the repository is public and those settings are in place.
- `publish-snapshot.yml` job `upload`, which runs in the protected environment
  `snapshot` (required reviewers, deployment rule for `main` and `release/0.3`),
  checks out only the release tools (and the root `pom.xml`) at
  `TRUSTED_TOOLING_SHA`, never signs and uploads only an unsigned snapshot. Only its environment secrets hold a
  snapshot Central token.
- The trusted release workstation, where a human release manager runs
  `sign-candidate.py` and `central-recovery.py` from reviewed `main` (also for a
  `release/0.3` source commit).

Never add signing or portal credentials to repository or organization secrets, to
any other workflow or job, or to a job that builds, tests or runs candidate code;
never pass them to Maven, wrappers, plugins, tests or dependencies. Never give
`id-token: write` to a job that runs project code. Do not dispatch historical
workflow revisions that used credentials. Agents never run the release signing
step, approve a release environment or handle the release key, its passphrase,
the CI subkey or portal tokens; they may run the signer tests, which use
throwaway keys in temporary keyrings. See
`docs/release-packaging.md#protected-tag-release-in-ci` and
`docs/release-packaging.md#offline-release-signing`.

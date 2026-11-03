# Releasing

The adapter has two lanes. Development and acceptance use `X.Y.Z-SNAPSHOT`
artifacts, built locally or published as a snapshot; users receive only
immutable final releases on Maven Central. CI holds no publishing credentials.

Final releases are signed with the **offline signer** and uploaded by the release
manager on a trusted release workstation; the production key and Central token
never exist in GitHub. CI builds the unsigned candidate and, after publication,
reads back the public bytes and creates the GitHub Release. See
[release packaging](release-packaging.md#offline-release-signing) for the design,
the step-by-step [release-manager runbook](release-packaging.md#release-manager-runbook),
key custody and key rotation. Tags never build or upload, and existing releases
are never rebuilt or replaced.

## Acceptance with locally built artifacts

1. Keep the POM on the next intended version, for example `0.4.0-SNAPSHOT`.
2. Finish local unit, Testcontainers, documentation, and schema compatibility checks.
3. Install the adapter into the local Maven repository of the machine that
   builds the validation integration:

   ```sh
   ./mvnw -DskipTests clean install
   ```

4. Build the validation integration on that machine with `./mvnw clean verify`,
   so it resolves the locally installed adapter snapshot, and build its
   immutable image. Record the adapter commit, the SHA-256 of the installed
   adapter JAR and the image digest in the acceptance evidence.
5. Deploy that image and run the acceptance campaign. Rebuilding the integration
   after another local install picks up different adapter bytes; the recorded
   image digest remains reproducible.

## Published snapshots

When acceptance runs on another machine, the release manager may publish a
snapshot of a reviewed commit, unsigned, from the release workstation with their
own Central token; the 0.3.6 acceptance snapshots were published this way. CI
does not publish snapshots: `publish-snapshot.yml` refuses to run. See the
[snapshot lane](release-packaging.md#snapshot-lane). Never add signing or portal
credentials to a workflow, or dispatch an older workflow revision, to publish
snapshots. Snapshots, local or published, are for acceptance testing, not
production dependencies.

## Final release

Follow the [release-manager runbook](release-packaging.md#release-manager-runbook):

1. Dispatch unsigned inspection of the exact approved source commit and
   explicitly selected version, then download its
   `unsigned-central-candidate-X.Y.Z` artifact with `gh run download`. The
   inspection workflow and the trusted tools come from `main`. For a 0.3.x
   release they come from `release/0.3` (`--ref release/0.3`) until `main`
   carries the same tools; the source commit is treated as data.
2. Verify the artifact offline with `sign-candidate.py verify` and review its
   provenance, effective POM and release notes.
3. Sign and bundle it on the release workstation with `sign-candidate.py sign`,
   then review `signing.json`, including the bundle hash, entries digest and
   trusted checkout commit.
4. Upload the same bundle with `central-recovery.py --allow-upload`, review the
   `VALIDATED` deployment in the Central portal, then publish it with
   `--publish`.
5. Read back the public bytes locally, then create the final tag on the source
   commit:

   ```sh
   git tag -s v0.4.0 <source-commit> -m "Release 0.4.0"
   git push origin v0.4.0
   ```

6. Run `publish-central.yml` from the same tool branch with the tag, candidate
   run, signer and `entries_sha256` to repeat the readback and create the GitHub
   Release.

Resume by the saved deployment state; never rebuild, re-sign or start a second
upload to repair a failed publication stage. The tools reject release-candidate,
milestone, snapshot and malformed versions. Afterward, integrations can pin the
final version. A later defect is fixed in a new patch release such as `0.4.1`;
published Central artifacts are never replaced.

## Release checklist

- Adapter tests and strict Javadocs pass.
- The release workstation signed the reviewed unsigned candidate offline and
  `signing.json` names the reviewed tool commit and expected signer.
- Schema upgrade regression passes, and shared message-store changes are
  compatible with the .NET adapter.
- Validation integration passes the documented acceptance campaign.
- Required message-store schema and deployment configuration changes are
  deployed first.
- Documentation and changelog describe the shipped behavior.
- The final tag points to the accepted commit and has not been used before.

## GitHub release notes

Before a final tag, finish a `## X.Y.Z — YYYY-MM-DD` section in `CHANGELOG.md`.
Include changes, compatibility, upgrade steps, known limitations and artifact identity.
The tag must match the source POM version. The manual release-page workflow
verifies actual public bytes against the reviewed unsigned candidate and the
offline signing receipt, and validates the section before creating a GitHub
Release. Relative documentation links are pinned to the released tag.

Verify the actual Central artifacts and GitHub Release body before marking the
release complete. A failed release-page step does not authorize another Maven
upload: repair the page from the existing tag and its reviewed notes. Existing
release bodies are compared, never overwritten automatically. Creating a release
page does not establish acceptance or any deployment.

Historical backfills must use an existing tag (`gh release create --verify-tag`),
reviewed historical notes and `--latest=false`; never create a replacement tag,
rebuild packages or fabricate historical release dates.

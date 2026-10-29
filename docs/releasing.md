# Releasing

The adapter has two lanes. Development and acceptance use locally built
`X.Y.Z-SNAPSHOT` artifacts; users receive only immutable final releases on Maven
Central. Snapshot publication to Sonatype is suspended.

Final releases take one of two paths with the same tools:

- **Protected-tag release in CI** (public repository with protected
  environments): a `vX.Y.Z` tag builds the unsigned candidate, records
  provenance, and then signs, uploads and publishes only after approvals in the
  environments `release` and `release-publish`. See
  [protected-tag release](release-packaging.md#protected-tag-release-in-ci).
- **Offline signing** (while the repository is private or the environments are
  not configured): the release manager signs and uploads on a trusted release
  workstation; the key and Central token never exist in GitHub. See
  [offline release signing](release-packaging.md#offline-release-signing) and the
  [release-manager runbook](release-packaging.md#release-manager-runbook).

After publication, `release-page.yml` reads back the public bytes and creates
the GitHub Release. Key custody and rotation are in
[release packaging](release-packaging.md#key-custody). Existing releases are
never rebuilt or replaced.

## Acceptance with locally built artifacts

1. Keep the POM on the next intended version, for example `0.4.0-SNAPSHOT`.
2. Finish local unit, Testcontainers, documentation, and contract checks.
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

Snapshot publication stays suspended because every Central upload requires a
human-gated signer, offline or in the protected release environment;
`publish-snapshot.yml` refuses to run. Do not restore signing or
portal credentials to any workflow, or dispatch an older workflow revision, to
publish snapshots. Locally built snapshots are suitable for acceptance testing,
not production dependencies.

## Final release in CI

Use this path only when the repository is public and the environments, tag
ruleset and variables in
[protected-tag release](release-packaging.md#protected-tag-release-in-ci) are in
place. `sign-upload` and `publish` refuse to run otherwise.

1. On the release commit, finish the dated `## X.Y.Z — YYYY-MM-DD` changelog
   section and keep the POM at `X.Y.Z-SNAPSHOT`. The commit must be on `main` or
   `release/0.3`.
2. Confirm that `TRUSTED_TOOLING_SHA` on both environments names the reviewed
   commit whose release tools you trust.
3. After immutable-release approval, create and push the signed tag:

   ```sh
   git tag -s v0.4.0 <source-commit> -m "Release 0.4.0"
   git push origin v0.4.0
   ```

4. Review the `build` evidence (`unsigned-central-candidate-X.Y.Z`: provenance,
   effective POM, release notes; the build log) and the checksums in the
   `attest` summary. A reviewer of `release` other than the tag author then
   approves `sign-upload`. It signs with the CI signing subkey and uploads once,
   stopping at `VALIDATED`.
5. Review the deployment in the Central portal against `signing.json` in
   `central-deployment-X.Y.Z`. Drop it in the portal if anything differs. A
   reviewer of `release-publish` then approves `publish`.
6. After `PUBLISHED`, run the `release-page.yml` command from the run summary to
   read back the public bytes and create the GitHub Release.

A failed or timed-out portal step is resumed from the retained state artifact on
the release workstation, never by re-running the job or pushing another tag.

## Final release offline

Follow the [release-manager runbook](release-packaging.md#release-manager-runbook):

1. Dispatch unsigned inspection of the exact approved source commit and
   explicitly selected version, then download its
   `unsigned-central-candidate-X.Y.Z` artifact with `gh run download`. The
   inspection workflow runs only from `main`. For a maintenance release, pass
   the `release/0.3` commit as `source_commit`; the tools treat it as data.
2. Verify the artifact offline with `sign-candidate.py verify` and review its
   provenance, effective POM and release notes.
3. Sign and bundle it on the release workstation with `sign-candidate.py sign`,
   then review `signing.json`, including the bundle hash, entries digest and
   trusted checkout commit.
4. After immutable-release approval, create the final tag on the source commit:

   ```sh
   git tag -s v0.4.0 <source-commit> -m "Release 0.4.0"
   git push origin v0.4.0
   ```

5. Upload the same bundle with `central-recovery.py --allow-upload`, review the
   `VALIDATED` deployment in the Central portal, then publish it with
   `--publish`. Revoke the per-release Central token afterward.
6. Read back the public bytes locally, then run `release-page.yml` with the
   tag, candidate run, signer and `entries_sha256` to repeat the readback and
   create the GitHub Release.

Resume by the saved deployment state; never rebuild, re-sign or start a second
upload to repair a failed publication stage. The tools reject release-candidate,
milestone, snapshot and malformed versions. Afterward, update integration
templates and contract certification pins to the final version. A later defect
is fixed in a new patch release such as `0.4.1`; published Central artifacts are
never replaced.

## Release checklist

- Adapter tests and strict Javadocs pass.
- `signing.json` names the expected signer and a clean trusted tool commit: the
  reviewed `main` checkout on the release workstation, or `TRUSTED_TOOLING_SHA`
  in CI.
- Contract certification and schema upgrade regression pass.
- Validation integration passes the documented acceptance campaign.
- Required platform schema and configuration changes are deployed first.
- Documentation and changelog describe the shipped behavior.
- The final tag points to the accepted commit and has not been used before.

## GitHub release notes

Before a final tag, finish a `## X.Y.Z — YYYY-MM-DD` section in `CHANGELOG.md`.
Include changes, compatibility, upgrade steps, known limitations and artifact identity.
The tag must match the source POM version. The manual release-page workflow
verifies actual public bytes against the reviewed unsigned candidate and the
offline signing receipt, and validates the section before creating a GitHub Release. Relative documentation links are
pinned to the released tag.

Verify the actual Central artifacts and GitHub Release body before marking the
release complete. A failed release-page step does not authorize another Maven
upload: repair the page from the existing tag and its reviewed notes. Existing
release bodies are compared, never overwritten automatically. Creating a release
page does not establish platform certification or customer deployment.

Historical backfills must use an existing tag (`gh release create --verify-tag`),
reviewed historical notes and `--latest=false`; never create a replacement tag,
rebuild packages or fabricate historical release dates.

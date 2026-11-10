# Releasing

The adapter has three lanes. Development and acceptance use locally built
`X.Y.Z-SNAPSHOT` artifacts, or a reviewed snapshot published through the
approval-gated [snapshot lane](#testing-a-published-snapshot) when another
environment must test it; users receive only immutable final releases on Maven
Central.

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

Locally built snapshots are suitable for acceptance testing, not production
dependencies.

## Testing a published snapshot

When the acceptance environment cannot build against a local Maven repository,
publish the reviewed snapshot to Central's snapshot repository and test exactly
that build there, before the final:

1. Merge the change to `main` (or `release/0.3` for the 0.3 line), with the POM
   at `X.Y.Z-SNAPSHOT`. A version that is already tagged gets no snapshot.
2. Dispatch the snapshot lane on that branch with its current head commit:

   ```sh
   gh workflow run publish-snapshot.yml --ref main -f source_commit=<40-character head commit>
   ```

3. The other of the two release approvers reviews the run's `build` and `attest`
   results and approves the `upload` job in the environment `snapshot`. The person
   who dispatched the run cannot approve it.
4. The run summary shows the timestamped snapshot version, for example
   `0.4.0-20261110.123456-1`, and the SHA-256 of each uploaded file. Pin that
   exact version in the test integration (not `0.4.0-SNAPSHOT`, which moves with
   the next upload), with Central's snapshot repository
   `https://central.sonatype.com/repository/maven-snapshots/` enabled for
   snapshots only. Record the version and hashes in the acceptance evidence.

What the lane guarantees: the bytes come from the reviewed branch head that was
dispatched, built and tested in a job without credentials, with GitHub build
provenance; the upload job runs no project code, checks the bytes against the
recorded checksums and the provenance, uploads them once and reads them back;
two people are involved. What it does not: a snapshot is not a release. It is not
signed, Central may expire it, and `X.Y.Z-SNAPSHOT` resolves to the latest
upload, so only the timestamped version identifies what was approved. A re-run
of the upload refuses; dispatch a new run. See
[snapshot lane](release-packaging.md#snapshot-lane) for the trust boundaries and
repository settings.

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

A failed or timed-out portal step is never re-run and never repeated by pushing
another tag: a re-run refuses. Recover on the release workstation after checking
the deployment in the Central portal, from the retained state when it matches the
portal, otherwise by reconciling in the portal; see
[release packaging](release-packaging.md#protected-tag-release-in-ci).

## Final release offline

Follow the [release-manager runbook](release-packaging.md#release-manager-runbook):

1. Dispatch unsigned inspection of the exact approved source commit and
   explicitly selected version, then download its
   `unsigned-central-candidate-X.Y.Z` artifact with `gh run download`.
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
milestone, snapshot and malformed versions. Afterward, integrations can pin the final version. A later defect is fixed in
a new patch release such as `0.4.1`; published Central artifacts are never
replaced.

## Release session checks

These checks come from earlier releases. They change nothing in the release
order above.

- Run the whole release in one shell session, or set the release variables
  again in every new one. A step run in a second terminal without them can act
  on the wrong version, file or commit.
- Turn off pagers for the session, `export GIT_PAGER=cat GH_PAGER=cat`, so
  that `git` and `gh` print their whole output instead of waiting in a pager.
- Before the upload, check the portal token without uploading anything: a
  status request (`POST /api/v1/publisher/status?id=<random UUID>`) with the
  same token answers `401` when the token is refused and `404` when it is
  accepted.
- An upload that fails before Central returns a deployment ID, for example
  with `401` for a mis-entered token, leaves the state file at
  `upload-started`. The recovery tool then refuses to upload again, as
  designed. Establish in the Central portal that no deployment exists for the
  bundle, keep that state file with the release evidence, and run the upload
  again with a new state file. Never delete or edit the old one to retry.
- Central's status of a finished deployment can say "Deployment components
  info not found" with an empty `purls` list. That alone is not a failure: the
  readback of the public bytes decides.
- Create the signed tag with the signing key's own user id, independent of the
  global git configuration:

  ```sh
  git -c user.name="<key user id name>" -c user.email="<key user id email>" \
      -c gpg.format=openpgp -c gpg.program=gpg \
      tag -s v0.4.0 <source-commit> -m "Release 0.4.0"
  ```

  GitHub refuses a push that would publish an email address the pushing
  account keeps private (`GH007`); a refused tag is local only, so delete it
  and create it again with the right identity. GitHub shows a tag signed with
  a key that is not on a GitHub account as unverified; the readback, not that
  badge, verifies the release.

## Release checklist

- Adapter tests and strict Javadocs pass.
- `signing.json` names the expected signer and a clean trusted tool commit: the
  reviewed `main` checkout on the release workstation, or `TRUSTED_TOOLING_SHA`
  in CI.
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
signing receipt (offline or from the protected job), and validates the section before creating a GitHub Release. Relative documentation links are
pinned to the released tag.

Verify the actual Central artifacts and GitHub Release body before marking the
release complete. A failed release-page step does not authorize another Maven
upload: repair the page from the existing tag and its reviewed notes. Existing
release bodies are compared, never overwritten automatically. Creating a release
page does not establish acceptance or any deployment.

Historical backfills must use an existing tag (`gh release create --verify-tag`),
reviewed historical notes and `--latest=false`; never create a replacement tag,
rebuild packages or fabricate historical release dates.

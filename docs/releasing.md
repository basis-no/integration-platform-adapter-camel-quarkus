# Releasing

The adapter has two lanes. Development and acceptance use locally built
`X.Y.Z-SNAPSHOT` artifacts; users receive only immutable final releases on Maven
Central. Snapshot publication to Sonatype is suspended.

Final releases are signed and uploaded **offline** by the release manager on a
trusted release workstation; the production key and Central token never exist in
GitHub. CI builds the unsigned candidate and, after publication, reads back the
public bytes and creates the GitHub Release. See
[release packaging](release-packaging.md#offline-release-signing) for the design,
the step-by-step [release-manager runbook](release-packaging.md#release-manager-runbook),
key custody and key rotation. Tags never build or upload, and existing releases
are never rebuilt or replaced.

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

Snapshot publication stays suspended because every Central upload requires the
offline signer; `publish-snapshot.yml` refuses to run. Do not restore signing or
portal credentials to any workflow, or dispatch an older workflow revision, to
publish snapshots. Locally built snapshots are suitable for acceptance testing,
not production dependencies.

## Final release

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
6. Read back the public bytes locally, then run `publish-central.yml` with the
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
- The release workstation signed the reviewed unsigned candidate offline and
  `signing.json` names the reviewed `main` commit and expected signer.
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
offline signing receipt, and validates the section before creating a GitHub Release. Relative documentation links are
pinned to the released tag.

Verify the actual Central artifacts and GitHub Release body before marking the
release complete. A failed release-page step does not authorize another Maven
upload: repair the page from the existing tag and its reviewed notes. Existing
release bodies are compared, never overwritten automatically. Creating a release
page does not establish acceptance or any deployment.

Historical backfills must use an existing tag (`gh release create --verify-tag`),
reviewed historical notes and `--latest=false`; never create a replacement tag,
rebuild packages or fabricate historical release dates.

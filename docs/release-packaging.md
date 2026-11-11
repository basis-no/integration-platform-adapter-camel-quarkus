# Release packaging and recovery

Releases move through separated stages: an unsigned candidate build, signing,
upload and publication, and public readback. Two paths implement them with the
same tools. On the public repository, a protected tag runs all stages in CI,
with signing and publication behind environment approvals
([protected-tag release](#protected-tag-release-in-ci)). Otherwise the release
manager signs and uploads offline on the release workstation
([offline release signing](#offline-release-signing)). Do not rebuild, retag or
replace an existing stable release.

## Protected-tag release in CI

`release.yml` starts only for a pushed `vX.Y.Z` tag. Only release managers can
create such tags (tag ruleset: no update, no deletion). Each job has its own
`permissions:`; every action is pinned by commit SHA.

| Job | Environment | Token and secrets | Executes |
| --- | --- | --- | --- |
| `build` | none | `contents: read`; no secrets, no OIDC | tag source, wrapper, plugins, tests (`-Prelease -Dgpg.skip=true clean verify`) |
| `attest` | none | `id-token`/`attestations: write`; no secrets | no checkout; `sha256sum`, `attest-build-provenance` |
| `sign-upload` | `release` | CI signing subkey, Central token | release tools at `TRUSTED_TOOLING_SHA`: `sign-candidate.py`, `central-bundle.py`, `central-recovery.py --allow-upload` |
| `publish` | `release-publish` | Central token | release tools at `TRUSTED_TOOLING_SHA`: `central-recovery.py --publish` |

1. **`build`** checks that the tag points at the clean `X.Y.Z-SNAPSHOT` source
   commit and that the commit is on `main` or `release/0.3`, validates the
   changelog section, builds the unsigned candidate and retains it as
   `unsigned-central-candidate-X.Y.Z`, in the same layout as unsigned
   inspection. It uses no shared Maven cache.
2. **`attest`** downloads the candidate without checking out the repository,
   rejects links and special files, records the SHA-256 of all eight files and
   creates GitHub build provenance for them. It is the only job that can request
   an OIDC token, and it runs no project code. On a public repository the
   provenance is also recorded in the public Sigstore transparency log.
3. **`sign-upload`** waits for a required reviewer of `release`. It first fails
   closed unless the environment still has required reviewers with
   self-review prevented, no administrator bypass and a tag-only deployment
   rule; the tag still points at the built commit; the release commit and
   `TRUSTED_TOOLING_SHA` are both on a reviewed branch; and the run has no
   earlier deployment state. It then checks out only `.github/scripts` (plus
   root files such as the trusted `pom.xml`) at `TRUSTED_TOOLING_SHA`, never the
   tag. It verifies the candidate against the recorded checksums, the build
   provenance and `sign-candidate.py verify`. It imports the CI signing subkey
   into a throwaway keyring on `/dev/shm` and refuses a secret that holds the
   primary key or belongs to another fingerprint. It runs `sign-candidate.py
   sign` unchanged and requires `tool_checkout` to be the clean pinned commit.
   It records upload intent, uploads once in `USER_MANAGED` mode and stops at
   `VALIDATED`. The signed bundle, receipts and `deployment.json` are retained
   as `central-deployment-X.Y.Z`, also after a failure.
4. **`publish`** waits for a required reviewer of `release-publish`, repeats the
   environment check, checks that the deployment belongs to this tag, run and
   tool commit, and publishes that deployment by its saved ID. The updated state
   is retained as `central-publish-X.Y.Z`. The run summary shows the
   `release-page.yml` command for readback and the GitHub Release.

Reviewers approve `sign-upload` only after reviewing the `build` evidence
(`provenance.json`, `effective-pom.xml`, `release-notes.md`, the build log) and
the checksums in the `attest` summary, and approve `publish` only after
reviewing the `VALIDATED` deployment in the Central portal against
`signing.json`. A reviewer cannot approve a run they started.

Recovery never repeats a portal write. A re-run of `sign-upload` or `publish`
always refuses, whatever state the run kept, because a runner can die after a
portal write and before it keeps its state. Recover on the release workstation:
first establish in the Central portal which deployments exist for the version
and in which state. If the run kept current state, resume with it and
`central-recovery.py`, as in
[upload, validation and publication](#upload-validation-and-publication-are-separate);
a polling timeout leaves the deployment on Central, so resume with the same
state file and without `--allow-upload`. If the state is missing, or older than
what the portal shows (for example `VALIDATED` state for a deployment the portal
shows as publishing or published), do not resume from it: reconcile with the
portal, and publish or drop the deployment there under the same approvals.

Any re-run counts, also one of a failed `build` or `attest`: from the second
attempt on, `sign-upload` and `publish` refuse, even when nothing was uploaded.
The tag cannot be pushed again, so finish such a release on the release
workstation with the [offline path](#offline-release-signing), from an unsigned
inspection of the tag's commit, and skip the runbook's tag step.

`TRUSTED_TOOLING_SHA` is an environment variable on both environments that only
administrators change. Set it to a reviewed commit on `main` or `release/0.3`
whose `.github/scripts` are the tools to trust, and change it only through a
reviewed change. The jobs never execute tools from the tag.

The CI signing subkey is a separate, passphrase-less signing subkey of the
release key, stored only as the `RELEASE_SIGNING_SUBKEY` secret of environment
`release` (see [key custody](#key-custody)). The Central token pair is stored as
`RELEASE_CENTRAL_TOKEN_USERNAME` and `RELEASE_CENTRAL_TOKEN_PASSWORD` in both
environments. These names never existed as repository secrets, so a job outside
the environments resolves them to empty values instead of a stale repository
secret.

## Offline release signing

Use this path while the repository is private, or whenever the protected
environments are not configured. Here the signing key and the Central portal
token **never exist in GitHub**: not as repository, organization or environment
secrets, not in CI runners, and not on agent or development hosts. GitHub holds
only the public key, in the `RELEASE_SIGNING_PUBLIC_KEY` repository variable,
for post-publication readback. Signing and Central upload are performed by a
human release manager on a trusted release workstation, using GnuPG and the
trusted release tools from a clean checkout of the reviewed tool branch: `main`,
or for a 0.3.x release `release/0.3`, until `main` carries the same tools. That
human step is also the release gate.

The reason is structural. A source commit, Maven wrapper, plugin, test or
dependency can execute arbitrary code, and no step cleanup or log masking protects
a key while that code runs. Without an enforceable protected environment no
workflow can hold a production credential safely. Do not add signing or portal
secrets to repository secrets or to any workflow other than the protected jobs
of `release.yml`, and do not dispatch historical workflow revisions that used
them.

| Stage | Where | Credentials | Executes |
| --- | --- | --- | --- |
| Unsigned inspection | `release-candidate.yml`, fresh runner | none (read-only runner token) | candidate source, wrapper, plugins, tests |
| Offline signing | release workstation | signing subkey via gpg-agent | `gpg`, `sign-candidate.py`, `central-bundle.py` from the reviewed tool checkout |
| Upload and publication | release workstation | the release manager's Central token | `central-recovery.py` from the reviewed tool checkout |
| Public readback and notes | `release-page.yml`, fresh runner | public key only | `verify-public-release.py`, `release-notes.py` |

The signer treats the downloaded candidate as **data**. It never checks out the
candidate source and never runs Maven, the wrapper, tests, plugins, hooks or any
file from the candidate artifact. Run every tool with `python3 -I` from the
trusted checkout, with the candidate and output directories outside it.

### Containment of the former CI credentials

Before automation was contained, repository secrets held a signing key, its
passphrase and a Central token, and credential-bearing Maven builds could read
them. There is no evidence of misuse, but treat them as exposed: delete those
repository secrets, revoke that Central token, disable the affected historical
workflows in Actions administration, and do not move the former CI key to the
workstation. Sign the next release with a key established through
[key rotation](#key-rotation), then retire the former key as superseded.
Workflow-file changes alone neither revoke secrets nor disable older revisions.

Older revisions of `publish-central.yml` on tags and branches start on any `v*`
tag push and read the former repository secrets. This line therefore keeps
readback in `release-page.yml` and has no `publish-central.yml`, so the
**Publish to Maven Central** workflow can stay disabled in Actions
administration; disabling it stops every revision on every ref. With the
repository secrets deleted, those revisions also find no credentials, and the
tag ruleset limits who can push a tag at all.

## Unsigned candidate before a tag

Dispatch unsigned inspection from the tool branch (`main`, or `release/0.3` for
a 0.3.x release) with an exact source commit, version and expected signer
fingerprint. The fingerprint does not cause signing. The job uses
Java 21 and the source Maven wrapper with `-Prelease -Dgpg.skip=true clean
verify`. It records wrapper/Java versions, effective plugins and source hashes,
then retains four opaque payloads (release POM, runtime JAR, sources JAR and
Javadoc JAR) in an `unsigned-central-candidate-VERSION` artifact. Its
`unsigned.json` receipt carries the `UNSIGNED_ONLY_NOT_RELEASE_ELIGIBLE` marker,
version, source commit, workflow run ID and every payload SHA-256.

Candidate code may ignore Maven flags, but there are no production credentials
on that runner to steal. Candidate inspection neither restores nor saves a shared
Maven cache, because candidate code could poison it for later jobs. Its provenance
is diagnostic evidence from a source-build runner, not a signing approval; the
release manager reviews it before signing.

Unsigned inspection cannot satisfy upload or readback on its own: Central rejects
unsigned payloads, `central-recovery.py` requires a validated 24-entry signed
bundle receipt, and `release-page.yml` requires the signed-entries digest that
only a signing step produces, offline or in the protected `sign-upload` job. Do not create a final tag from unsigned
inspection alone.

## Release-manager runbook

Prerequisites on the release workstation:

- a clean checkout of the reviewed tool branch (`git status` empty, `HEAD` equal
  to the reviewed commit), used only as trusted tooling. For a 0.3.x release this
  is `release/0.3` at the source commit itself;
- GnuPG with the release key's signing subkey (see [key custody](#key-custody)),
  a pinentry program and the public key imported;
- the GitHub CLI, used only to dispatch workflows and download artifacts;
- the `RELEASE_SIGNING_PUBLIC_KEY` repository variable holding the release key's
  armored public key;
- a dated `## X.Y.Z — YYYY-MM-DD` section in `CHANGELOG.md` and the POM at
  `X.Y.Z-SNAPSHOT` on the approved source commit.

The examples use `TOOLS` for the trusted checkout, `TOOLS_REF` for its branch and
a new, empty `RELEASE` directory outside it for this release's evidence. `SIGNER`
is the release key's primary fingerprint; GnuPG signs with its signing subkey:

```sh
TOOLS=/path/to/reviewed-tool-checkout
TOOLS_REF=main                     # release/0.3 for a 0.3.x release
RELEASE=/path/to/releases/X.Y.Z    # new, empty, outside $TOOLS
VERSION=X.Y.Z SOURCE=<40-character source commit> SIGNER=<40-character fingerprint>
export GH_REPO=basis-no/integration-platform-adapter-camel-quarkus
mkdir -p "$RELEASE" && cd "$RELEASE"
```

1. **Inspect.** Dispatch unsigned inspection, find its run and wait for it to
   succeed. Check that the run's `headSha` is the reviewed tool commit and record
   its ID as `RUN`:

   ```sh
   gh workflow run release-candidate.yml --ref "$TOOLS_REF" -f action=inspect \
     -f version="$VERSION" -f source_commit="$SOURCE" -f signer="$SIGNER"
   gh run list --workflow release-candidate.yml --branch "$TOOLS_REF" --limit 1 \
     --json databaseId,headSha,status
   RUN=<databaseId>
   gh run watch "$RUN" --exit-status
   ```

2. **Download** the unsigned artifact into a new directory:

   ```sh
   gh run download "$RUN" -n "unsigned-central-candidate-$VERSION" -D "$RELEASE/candidate"
   ```

3. **Verify** the inventory, receipt identity and every payload hash without
   signing anything, then review `provenance.json` (workflow head, Java/Maven,
   effective plugins), `effective-pom.xml` and `release-notes.md`:

   ```sh
   python3 -I "$TOOLS/.github/scripts/sign-candidate.py" verify \
     --candidate "$RELEASE/candidate" --version "$VERSION" \
     --source-commit "$SOURCE" --candidate-run "$RUN"
   ```

   The tool refuses missing, extra or special files, symbolic links, unexpected
   or traversing payload names, a missing identity marker, a version, commit or
   run mismatch, provenance for another coordinate, and any payload whose SHA-256
   differs from the receipt. Group and artifact come from the trusted checkout's
   POM, never from the candidate.

4. **Sign and bundle.** The same checks run again before signing:

   ```sh
   python3 -I "$TOOLS/.github/scripts/sign-candidate.py" sign \
     --candidate "$RELEASE/candidate" --version "$VERSION" \
     --source-commit "$SOURCE" --candidate-run "$RUN" \
     --signer "$SIGNER" --output "$RELEASE/signed"
   ```

   The signer copies the verified bytes into `signed/`, signs exactly the four
   payloads with `gpg --batch --detach-sign --armor --local-user $SIGNER` through
   your gpg-agent and pinentry, and verifies every signature against the exact
   fingerprint. It then calls `central-bundle.py` to assemble and validate
   `candidate.zip` and its `candidate.json` validation receipt, and writes
   `signing.json` last. That receipt records the coordinate, source commit, run,
   payload and signature hashes, `bundle_sha256`, `entries_sha256`, the trusted
   checkout commit, tool hashes and GnuPG/Python versions. A directory without
   `signing.json` is incomplete: never upload from it; sign again into a new
   directory. Refuse to continue if `tool_checkout.clean` is false or its commit
   is not the reviewed tool commit.

5. **Review** `signing.json` and record `bundle_sha256` and `entries_sha256` with
   the release approval.

6. **Upload to `VALIDATED`.** Enter the release manager's Central token (the
   user token's username and password) into the current shell session only. `read
   -rs` shows nothing and keeps the values out of shell history; never pass them
   as arguments or write them to a file:

   ```sh
   read -rs CENTRAL_TOKEN_USERNAME; echo; read -rs CENTRAL_TOKEN_PASSWORD; echo
   export CENTRAL_TOKEN_USERNAME CENTRAL_TOKEN_PASSWORD
   python3 -I "$TOOLS/.github/scripts/central-recovery.py" --bundle "$RELEASE/signed/candidate.zip" \
     --receipt "$RELEASE/signed/candidate.json" --state "$RELEASE/signed/deployment.json" \
     --allow-upload --approval-reference "<non-secret approval reference>"
   ```

   This uploads once in `USER_MANAGED` mode and stops at `VALIDATED`.

7. **Review the deployment** in the Central portal: coordinate, file list and
   validation result must match `signing.json`. Drop it in the portal if anything
   differs; never repair payloads.

8. **Publish** the same validated deployment:

   ```sh
   python3 -I "$TOOLS/.github/scripts/central-recovery.py" --bundle "$RELEASE/signed/candidate.zip" \
     --receipt "$RELEASE/signed/candidate.json" --state "$RELEASE/signed/deployment.json" \
     --publish --approval-reference "<non-secret approval reference>"
   unset CENTRAL_TOKEN_USERNAME CENTRAL_TOKEN_PASSWORD
   ```

9. **Read back locally** once the files appear on Maven Central (this can take a
   while; the check is read-only and safe to repeat):

   ```sh
   git -C "$TOOLS" show "$SOURCE:pom.xml" > "$RELEASE/source-pom.xml"
   python3 -I "$TOOLS/.github/scripts/verify-public-release.py" --pom "$RELEASE/source-pom.xml" \
     --version "$VERSION" --signer "$SIGNER" --expected-receipt "$RELEASE/signed/candidate.json" \
     --output "$RELEASE/signed/public-readback.json"
   ```

10. **Tag.** Create and push the signed tag on the source commit:

    ```sh
    git -C "$TOOLS" tag -s "v$VERSION" "$SOURCE" -m "Release $VERSION"
    git -C "$TOOLS" push origin "v$VERSION"
    ```

    Pushing the tag also starts `release.yml`. Its `sign-upload` job waits for
    approval in `release`: reject that deployment, since this release is
    already published.

11. **Run the release-page workflow**, which repeats the readback on a fresh
    runner and then creates or compares the GitHub Release:

    ```sh
    gh workflow run release-page.yml --ref "$TOOLS_REF" -f tag="v$VERSION" \
      -f candidate_run="$RUN" -f signer="$SIGNER" -f signed_entries_sha256=<entries_sha256>
    ```

12. **Archive** `$RELEASE` (candidate, `signed/` with bundle, receipts,
    `deployment.json` and readback) in the approved durable release evidence
    store. It contains no key or token.

## Upload, validation and publication are separate

`central-recovery.py` uses the **same** reviewed bundle and validation receipt
for every call. `--allow-upload` uploads in `USER_MANAGED` mode and stops at
`VALIDATED`; `--publish` publishes that exact validated deployment after review.

An existing state resumes by deployment ID and never uploads again. Bundle or
receipt mismatches fail closed. Upload intent is saved before the network call;
the returned deployment ID is saved before polling. A lost upload response needs
operator inspection in Central, not a blind retry. A lost publish response uses
status for the same deployment; if it remains `VALIDATED`, the tool refuses
another publish call until an operator establishes the outcome. Preserve stale
lock/temporary files after a crash for review; do not discard state to retry.

Keep `deployment.json` on the workstation, back it up with the release evidence
before closing the session, and restore it for any retry. Only one release
manager operates a coordinate at a time; the state lock serializes local calls.
Optionally, `--prepare-upload` records upload intent without portal credentials
before the first network call. A polling timeout is resumed with the same state
file and without `--allow-upload`.

`PUBLISHED` must still be followed by readback of actual public POM/JAR/source/
Javadoc bytes, signatures and checksums. GitHub notes are a separate gate: repair
them from the existing tag without another package upload. A mismatch in existing
notes requires review, never automatic overwrite.

`release-page.yml` has only a manual trigger and only the public key. It
downloads the unsigned candidate artifact from the given run, checks it with
`sign-candidate.py verify`, and verifies that the public payloads match that
receipt's hashes, that every signature verifies against the pinned signer, and
that the public entries reproduce the `entries_sha256` from `signing.json`. Only
then does it create or compare release notes from the existing tag. It cannot
build, sign, upload or publish Maven artifacts.

Unsigned candidate artifacts have 90-day retention. Archive the release
directory before expiration; workflow artifacts are a bounded recovery path, not
a permanent archive. Do not delete failed deployment evidence as cleanup.

## Key custody

- The release key has a certify-only primary key and a signing subkey. The
  primary secret key and the revocation certificate are kept offline, as a
  passphrase-protected export in the team password manager backup. The release
  workstation's GnuPG keyring holds only its signing subkey's secret
  (`gpg --list-secret-keys` shows `sec#`). Never copy any of them to GitHub, CI,
  agent or development hosts, shared drives, tickets or chat. The dedicated CI
  signing subkey (last item) exists only as a secret of environment `release`.
- The passphrase is entered only through pinentry and stored only in the team
  password manager. Never put it in files, scripts, environment variables, shell
  history or command arguments, and never use loopback pinentry or agent presets
  for the release key. `sign-candidate.py` has no passphrase input.
- Keep the gpg-agent cache short and the workstation encrypted and locked when
  unattended. Coding agents never operate the release keyring.
- The Central token is the release manager's own user token and is kept only on
  the release workstation, entered into the shell session for
  `central-recovery.py`. Never put it in
  GitHub, CI, a checkout, a ticket or chat. If it may have leaked, revoke it in
  the Central portal and generate a new one. The protected-tag path has a
  Central token of its own, which lives only in the release environments'
  secrets.
- If the signing subkey may be compromised, revoke it with the offline primary
  key, add a new signing subkey and publish the updated public key. If the
  primary key or its backup may be compromised, publish its revocation
  certificate to both keyservers below, stop releases and rotate.
- For the [protected-tag release](#protected-tag-release-in-ci), the primary key
  is certify-only and stays offline. On the release workstation, restore the
  primary key from its backup into a temporary keyring, add a dedicated signing
  subkey for CI with a one-year expiry there, remove that subkey's passphrase,
  export only that subkey (`gpg --armor --export-secret-subkeys
  CI_SUBKEY_FINGERPRINT!`), and paste the export only into the
  `RELEASE_SIGNING_SUBKEY` secret of environment `release`. Delete the temporary
  keyring and publish the updated public key. The workflow refuses an export that
  contains the primary secret key. Revoke and replace the CI subkey on its
  expiry, on any doubt about the environment, or when a reviewer leaves; the
  primary key and its other subkeys stay valid.

## Key rotation

1. On the release workstation, create a certify-only primary key (RSA 4096 is
   the most widely accepted choice) with a finite expiry and a strong
   passphrase, then a signing subkey that expires after one year:

   ```sh
   gpg --quick-generate-key "NAME <ADDRESS>" rsa4096 cert 3y
   gpg --list-keys "<ADDRESS>"    # NEW_FINGERPRINT is the primary key's fingerprint
   gpg --quick-add-key NEW_FINGERPRINT rsa4096 sign 1y
   ```

2. Take the primary key offline. First store a passphrase-protected export
   (`gpg --armor --export-secret-keys NEW_FINGERPRINT`) in the team password
   manager, together with the revocation certificate that GnuPG wrote to
   `openpgp-revocs.d/NEW_FINGERPRINT.rev` in its home directory, and remove that
   certificate from the workstation. Then delete the primary secret key from the
   keyring; the signing subkey stays:

   ```sh
   gpg --with-keygrip --list-secret-keys NEW_FINGERPRINT   # the first Keygrip is the primary key's
   rm "$(gpgconf --list-dirs homedir)/private-keys-v1.d/PRIMARY_KEYGRIP.key"
   gpg --list-secret-keys NEW_FINGERPRINT                  # sec#: primary secret key absent
   ```

3. Publish the public key to two keyservers that Central consults, then confirm
   retrieval from each into an empty temporary keyring:

   ```sh
   gpg --keyserver hkps://keys.openpgp.org --send-keys NEW_FINGERPRINT
   gpg --keyserver hkps://keyserver.ubuntu.com --send-keys NEW_FINGERPRINT
   ```

   keys.openpgp.org publishes user IDs only after email verification; complete it.
4. Replace the `RELEASE_SIGNING_PUBLIC_KEY` repository variable with the armored
   public key. It is public data, not a secret:

   ```sh
   gpg --armor --export NEW_FINGERPRINT | gh variable set RELEASE_SIGNING_PUBLIC_KEY
   ```

5. Use the new fingerprint as `signer` for inspection, signing and readback, and
   state the new fingerprint in the next release notes.
6. Before the signing subkey expires, extend it with the offline primary key
   (`gpg --quick-set-expire NEW_FINGERPRINT 1y SUBKEY_FINGERPRINT`) or add a new
   signing subkey, and publish the updated public key to both keyservers and the
   repository variable. The release tools reject signatures from an expired
   key; extending the subkey keeps earlier releases verifiable.
7. Retire the old key as **superseded**: after any pending readback of releases
   signed with it is complete, generate a revocation with reason "Key is
   superseded" and publish it to both keyservers. Published artifacts keep their
   signatures; the release tools reject revoked signers, so readback of old
   releases is no longer possible with them. Use reason "compromised" and act
   immediately if compromise is suspected.

## Snapshot lane

`publish-snapshot.yml` publishes a reviewed `X.Y.Z-SNAPSHOT` to Central's
snapshot repository, so that another environment can test it before the final;
see [Releasing](releasing.md#testing-a-published-snapshot) for how to run it. It
starts only by `workflow_dispatch`, from the head of `main` or `release/0.3`,
with that head commit as `source_commit`, and it has the same separation as the
protected-tag release:

| Job | Environment | Credentials | Runs |
| --- | --- | --- | --- |
| `build` | none | read-only token | project code: the Maven build and tests |
| `attest` | none | OIDC token for build provenance | no project code |
| `upload` | `snapshot` | snapshot Central token | release tools at `TRUSTED_TOOLING_SHA`: `snapshot-upload.py` |

1. **`build`** checks that `source_commit` is the dispatched branch head, that
   the branch is `main` or `release/0.3`, that the clean POM is at
   `X.Y.Z-SNAPSHOT` and that `vX.Y.Z` is not tagged. It builds and tests the
   unsigned jar, sources, javadoc and POM without a version change and retains
   them with their provenance as `unsigned-snapshot-candidate-X.Y.Z-SNAPSHOT`.
2. **`attest`** downloads the candidate without checking out the repository,
   records the SHA-256 of its seven files and creates GitHub build provenance.
3. **`upload`** waits for a required reviewer of `snapshot`. It first fails
   closed unless the environment has required reviewers with self-review
   prevented, no administrator bypass and a deployment rule for the branches
   `main` and `release/0.3` only; the commit and `TRUSTED_TOOLING_SHA` are on a
   reviewed branch; `vX.Y.Z` is still not tagged, since the approval can come
   later than the build; this is the run's first attempt and the run has no
   snapshot state. That `source_commit` was the branch head is checked at
   dispatch only; a later merge does not stop the approved upload. It checks out
   only `.github/scripts` (plus root files such as the trusted `pom.xml`) at
   `TRUSTED_TOOLING_SHA`, verifies the candidate against the checksums, the
   unsigned receipt and the build provenance, and runs `snapshot-upload.py`.
   That script takes the group and artifact from the trusted `pom.xml`, never
   from the candidate, which project code wrote, and refuses a candidate that
   names another coordinate; the version must be `X.Y.Z-SNAPSHOT` and equal the
   version in the source commit's own `pom.xml`, which the job reads through the
   repository API, not from the candidate. It records its state before the first request, uploads the four
   payloads once under one timestamped snapshot version with their MD5 and SHA-1
   checksums and the version and artifact `maven-metadata.xml`, as a Maven
   deploy would, reads every payload and both metadata files back and compares
   their SHA-256. Requests go only to Central's snapshot repository and never
   follow a redirect. The state is retained as
   `central-snapshot-X.Y.Z-SNAPSHOT`, also after a failure.

The lane never signs (Central does not require signatures on snapshots) and
never calls the publisher API that releases use. A re-run of `upload` refuses;
dispatch a new run, which uploads a new timestamped build. Repository settings
the lane needs, in addition to those of the release path:

- snapshot publishing enabled for the namespace in the Central portal;
- the environment `snapshot` with the same two required reviewers and "prevent
  self-review" as `release`, no administrator bypass, and a deployment rule for
  the branches `main` and `release/0.3`;
- its secrets `SNAPSHOT_CENTRAL_TOKEN_USERNAME` and
  `SNAPSHOT_CENTRAL_TOKEN_PASSWORD`, and its variable `TRUSTED_TOOLING_SHA`.

The upload follows Central's documented Maven snapshot layout but was written
without a live run. Check the first real run with a real Maven resolve of the
timestamped version and of `X.Y.Z-SNAPSHOT` from a clean local repository before
relying on the lane.

If the readback fails on a payload, the repository does not serve the uploaded
bytes: do not use that snapshot, and dispatch a new run once the cause is known.
If it fails only on a `maven-metadata.xml`, the payloads were read back intact;
Central may rewrite metadata it stores. Inspect it rather than treat it as lost
bytes: download both metadata files, check that the version metadata lists the
run's timestamped version for the POM, the jar and both classifiers, with the
run's build number, and that the artifact metadata lists `X.Y.Z-SNAPSHOT`; then
resolve the timestamped version with Maven as above. If that resolves the
recorded hashes, the snapshot is usable; record the difference with the run.

A Central portal token can write releases as well as snapshots for the whole
namespace, so the snapshot token gets the same custody as the release token: it
exists only as a secret of the environment `snapshot`, and the job refuses to
run when the environment is less protected than above.

## Evidence reuse and timing

`release-evidence.py` fingerprints six declared input groups: runtime,
dependencies, contract, fixtures, toolchain and configuration. Provide an explicit
JSON object mapping each group to concrete relative files under `--root`; empty
groups must be deliberate. Review this inventory for completeness and regenerate
it from current tracked files for each candidate so additions, renames and deletes
are represented. External contracts/fixtures must first be materialized in the
evidence root with their provenance. The tool does not discover omitted inputs.

```sh
python3 .github/scripts/release-evidence.py --root evidence-inputs \
  --manifest inputs.json --output evidence.json
```

Optional `--previous previous.json --gates gates.json` assesses reuse. Each gate
declares `kind`, `inputs`, `result` and its `evidence` reference. Supported kinds
are `local_test`, `documentation`, `required_ci`, `public_artifacts` and
`environment_acceptance`. Only passed local or documentation evidence with
unchanged relevant inputs is reusable. Required CI, public artifact readback and
environment acceptance always retain their independent verification gates.
Metadata differences alone do not create new runtime qualification evidence.

Record phase intervals in the existing outcome receipt with `pool`, `phase`,
`start`, `end` and `reason`. Use setup, implementation, build/test, resource or
runner queue, approval/access wait and recovery/rework. Unknown durations remain
null. Keep wall time separate from summed concurrent jobs; do not double count
overlapping work. No new monitoring service is needed.


## Production candidate review

Before the next release, review the exact candidate: offline validation before
tagging, approved artifact retention, serialized upload/validation/publish with
durable state, pinned toolchain provenance and actual public artifact readback.
The offline signer tests in `test-release-packaging.py` exercise real GnuPG
signing with throwaway keys in temporary keyrings: the happy path, tampered
payloads, wrong signers, extra/missing files, symbolic links and receipt
mismatches. They do not exercise the real release key, a pinentry prompt or the
Central portal. The first release with a new key qualifies those on the
workstation: run steps 3–4 and review `signing.json` before any upload. Merging
these tools or passing synthetic CI does not select a product version or approve
a tag; the release owner supplies those decisions under actual approval.

## Synthetic runner qualification

On `main`, the Test workflow runs this as a separate bounded packaging
qualification job. On the 0.3 line no workflow runs it; run
`python3 .github/scripts/qualify-release-packaging.py --evidence <dir>` by hand
before a release. It uses
Java 21, the checked-in Maven wrapper and the source POM's exact compiler,
source, Javadoc and GPG plugin configuration to build a small synthetic Java
artifact. It excludes the publisher plugin and uses only the reserved test
coordinate `org.example.packaging.fixture:signed-fixture:1.2.3`. It never calls
install/deploy or accesses portal credentials or project signing keys.

The job creates an ephemeral signing key, isolates both Maven settings and the
GPG keyring, validates actual four-payload signed output through the same bundle
validator, and rejects corrupted payloads and an incorrect signer. It retains
only logs, effective POM, synthetic signed bundle, source fingerprints and the
cleanup receipt. The agent stops before its keyring and temporary workspace are
removed; failed cleanup prevents a successful result. Deadlines, output-stall
checks and process-group termination bound execution and cancellation.

This checks packaging integration, not the full adapter's runtime, a production
signer's provenance, the publisher extension or the Central portal. Those remain
separate candidate/release gates. Requalify this gate after changes to the fixture,
source packaging POM, wrapper, bundle validator or Test workflow. Reusable offline
fixtures do not replace exact-head CI or actual candidate/public artifact checks.
Read the exact-head Test workflow result and its retained artifact for the
current qualification and cleanup evidence.

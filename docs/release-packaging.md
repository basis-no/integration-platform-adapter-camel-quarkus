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
refuses once the run holds deployment or publication state. Download that
artifact and resume on the release workstation with `central-recovery.py`, as
in [upload, validation and publication](#upload-validation-and-publication-are-separate).
A polling timeout leaves the deployment on Central; resume with the same state
file and without `--allow-upload`.

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
trusted release tools from a checkout of reviewed `main`. That human step is
also the release gate.

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
| Offline signing | release workstation | release key via gpg-agent | `gpg`, `sign-candidate.py`, `central-bundle.py` from reviewed `main` |
| Upload and publication | release workstation | per-release Central token | `central-recovery.py` from reviewed `main` |
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

Dispatch unsigned inspection from `main` with an exact source commit, version and
expected signer fingerprint. The fingerprint does not cause signing. The job uses
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
only the offline signer produces. Do not create a final tag from unsigned
inspection alone.

## Release-manager runbook

Prerequisites on the release workstation:

- a clean checkout of reviewed `main` (`git status` empty, `HEAD` equal to the
  reviewed commit), used only as trusted tooling;
- GnuPG with the release key, a pinentry program and the public key imported;
- the GitHub CLI, used only to dispatch workflows and download artifacts;
- a dated `## X.Y.Z — YYYY-MM-DD` section in `CHANGELOG.md` and the POM at
  `X.Y.Z-SNAPSHOT` on the approved source commit.

The examples use `TOOLS` for the trusted checkout and a new, empty `RELEASE`
directory outside it for this release's evidence:

```sh
TOOLS=/path/to/reviewed-main-checkout
RELEASE=/path/to/releases/X.Y.Z    # new, empty, outside $TOOLS
VERSION=X.Y.Z SOURCE=<40-character source commit> SIGNER=<40-character fingerprint>
mkdir -p "$RELEASE" && cd "$RELEASE"
```

1. **Inspect.** Dispatch unsigned inspection and wait for it to succeed. Record
   the run ID as `RUN`:

   ```sh
   gh workflow run release-candidate.yml --ref main -f action=inspect \
     -f version="$VERSION" -f source_commit="$SOURCE" -f signer="$SIGNER"
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
   is not the reviewed `main`.

5. **Review and tag.** Review `signing.json` and record `bundle_sha256` and
   `entries_sha256` with the release approval. After immutable-release approval,
   create and push the signed tag on the source commit:

   ```sh
   git -C "$TOOLS" tag -s "v$VERSION" "$SOURCE" -m "Release $VERSION"
   git -C "$TOOLS" push origin "v$VERSION"
   ```

6. **Upload to `VALIDATED`.** Generate a fresh Central user token for this
   release window and enter it only into the current shell session, without
   echo, files or shell history:

   ```sh
   read -r CENTRAL_TOKEN_USERNAME; read -rs CENTRAL_TOKEN_PASSWORD
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

   After `PUBLISHED`, revoke the token in the portal.

9. **Read back locally** once the files appear on Maven Central (this can take a
   while; the check is read-only and safe to repeat):

   ```sh
   git -C "$TOOLS" show "v$VERSION:pom.xml" > "$RELEASE/tag-pom.xml"
   python3 -I "$TOOLS/.github/scripts/verify-public-release.py" --pom "$RELEASE/tag-pom.xml" \
     --version "$VERSION" --signer "$SIGNER" --expected-receipt "$RELEASE/signed/candidate.json" \
     --output "$RELEASE/signed/public-readback.json"
   ```

10. **Run the release-page workflow**, which repeats the readback on a fresh
    runner and then creates or compares the GitHub Release:

    ```sh
    gh workflow run release-page.yml --ref "v$VERSION" -f tag="v$VERSION" \
      -f candidate_run="$RUN" -f signer="$SIGNER" -f signed_entries_sha256=<entries_sha256>
    ```

11. **Archive** `$RELEASE` (candidate, `signed/` with bundle, receipts,
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

- The release key, its secret subkeys and its revocation certificate exist only
  in the release workstation's GnuPG keyring and, as a passphrase-protected
  export, in the team password manager backup. Never copy them to GitHub, CI,
  agent or development hosts, shared drives, tickets or chat.
- The passphrase is entered only through pinentry and stored only in the team
  password manager. Never put it in files, scripts, environment variables, shell
  history or command arguments, and never use loopback pinentry or agent presets
  for the release key. `sign-candidate.py` has no passphrase input.
- Keep the gpg-agent cache short and the workstation encrypted and locked when
  unattended. Coding agents never operate the release keyring.
- Central tokens are generated per release window, live only in the release
  shell session and are revoked after publication.
- If the key or its backup may be compromised, publish its revocation
  certificate to both keyservers below, stop releases and rotate.
- For the [protected-tag release](#protected-tag-release-in-ci), the primary key
  is certify-only and stays offline. On the release workstation, add a
  dedicated signing subkey for CI with a one-year expiry, export only that
  subkey (`gpg --armor --export-secret-subkeys CI_SUBKEY_FINGERPRINT!`) from a
  temporary copy of the keyring in which its passphrase was removed, and paste
  the export only into the `RELEASE_SIGNING_SUBKEY` secret of environment
  `release`. Delete the temporary copy. The workflow refuses an export that
  contains the primary secret key. Revoke and replace the CI subkey on its
  expiry, on any doubt about the environment, or when a reviewer leaves; the
  primary key and its other subkeys stay valid.

## Key rotation

1. On the release workstation, generate a new primary key (RSA 4096 is the most
   widely accepted choice) with a finite expiry and a strong passphrase. Create
   a revocation certificate and store it, with a passphrase-protected secret-key
   export, in the team password manager.
2. Publish the public key to two keyservers that Central consults, then confirm
   retrieval from each into an empty temporary keyring:

   ```sh
   gpg --keyserver hkps://keys.openpgp.org --send-keys NEW_FINGERPRINT
   gpg --keyserver hkps://keyserver.ubuntu.com --send-keys NEW_FINGERPRINT
   ```

   keys.openpgp.org publishes user IDs only after email verification; complete it.
3. Replace the `RELEASE_SIGNING_PUBLIC_KEY` repository variable with the armored
   public key (`gpg --armor --export NEW_FINGERPRINT`). It is public data, not a
   secret.
4. Use the new fingerprint as `signer` for inspection, signing and readback, and
   state the new fingerprint in the next release notes.
5. Retire the old key as **superseded**: after any pending readback of releases
   signed with it is complete, generate a revocation with reason "Key is
   superseded" and publish it to both keyservers. Published artifacts keep their
   signatures; the release tools reject revoked signers, so readback of old
   releases is no longer possible with them. Use reason "compromised" and act
   immediately if compromise is suspected.

## Snapshot lane

Snapshot publication stays suspended: `publish-snapshot.yml` refuses to run.
A snapshot would need a signed upload for every change, which the offline,
human-gated signer deliberately does not provide. Acceptance uses locally built
artifacts instead; see [Releasing](releasing.md#acceptance-with-locally-built-artifacts).
Previously published snapshots are unchanged and expire under Sonatype policy.

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

The Test workflow has a separate bounded packaging qualification job. It uses
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

# Release packaging and recovery

These tools separate candidate inspection, upload and publication into explicit
manual stages. The synthetic CI fixture checks real Maven/GPG packaging and
cleanup. A full adapter candidate, trusted production signer and release approval
remain separate gates before any production upload. Do not rebuild, retag or
replace an existing stable release.

## Production credential hold

The candidate workflow performs **unsigned inspection only**. It never imports a
production private key, supplies a signing passphrase, or obtains Central portal
credentials. Upload, resume and publish are refused. Snapshot publication is also
suspended because its Maven build previously shared a runner with signing and
publication credentials. Existing releases and published snapshots are unchanged.

A source commit, Maven wrapper, plugin, test or dependency can execute arbitrary
code. An exact SHA, a private repository, an approval-reference string and a
synthetic-key test do not establish permission to expose production credentials.
Neither a later cleanup step nor log masking protects a key while code is running.
Do not re-enable either workflow by adding repository secrets to source builds.

Before production automation resumes, establish an enforceable approval boundary
for the **workflow code and artifact identity**, with credentials unavailable to
branch workflows. Move signing credentials out of repository-wide access into a
separately reviewed secret store or a supported protected environment. Environment
reviewers/branch controls must actually be configured and enforced; a name alone
is insufficient. Separately authorize any billing, secret migration or key rotation.

Use distinct fresh runners for source build, signing, and publication. The build
has no production credentials. The signer receives only reviewed, hash-pinned
payloads as data and runs fixed trusted tooling: no candidate checkout, Maven,
wrapper, tests, plugins, hooks, cache reuse or artifact-provided executables.
The publisher verifies the signed bundle and executes only trusted portal tooling.
A replacement must be independently qualified before this hold is removed.

Workflow-file changes alone do not revoke repository secrets or disable historical
workflow revisions. Disable the affected workflows through Actions administration
as an immediate containment step; account for older branch versions and other
credential-bearing workflows before claiming repository-wide containment.

## Unsigned candidate before a tag

Dispatch unsigned inspection from `main` with an exact source commit, version and
expected future signer fingerprint. The fingerprint does not cause signing.
The job uses Java 21 and the source Maven wrapper with `-Prelease -Dgpg.skip=true
clean verify`. It records wrapper/Java versions, effective plugins and source
hashes, then retains four opaque payloads in an `unsigned-central-candidate-*`
artifact with `UNSIGNED_ONLY_NOT_RELEASE_ELIGIBLE` identity. Candidate code may
ignore Maven flags, but there are no production credentials available to steal.
The read-only GitHub token remains a runner credential, not a production signer.

Unsigned inspection cannot qualify a release or satisfy the signed-bundle workflow:
its artifact name and receipt differ, and it contains no production signatures.
Do not create a final tag from unsigned inspection alone. The retained standalone
bundle/recovery tools below remain for already signed, independently approved
artifacts; they do not authorize a credential-bearing Maven build.

Require exactly four payloads in the candidate directory: the release POM,
runtime JAR, sources JAR and Javadoc JAR, each with its detached `.asc` signature.
If Maven's output differs, investigate rather than copy or sign a different POM.
Assemble and validate with the operator-provisioned GPG keyring:

```sh
python3 .github/scripts/central-bundle.py candidate.zip \
  --from-directory target --group org.example --artifact adapter --version 1.2.3 \
  --signer FULL_UPPERCASE_40_CHARACTER_FINGERPRINT --receipt candidate.json
```

Unsigned inspection retains the actual wrapper/Java version, effective release
plugin versions, POM/wrapper digests and exact source/workflow heads. It requires
a clean matching snapshot source and dated release notes. Separate trusted signing
is not implemented by this workflow.
The tool generates checksums and includes only those eight signed inputs. It
rejects an unexpected namespace/version, missing or duplicate entries, incorrect
checksums, invalid JARs and signatures from another key. It never downloads keys.
Provision the trusted signer separately; a valid signature is not a substitute
for reviewing source, dependencies or the key's provenance. Record and review the
bundle SHA-256 and all entry hashes before creating the immutable tag.

For an already signed plugin bundle, `--clean-to clean.zip` removes only the
known local Maven metadata and `_remote.repositories` entries for the declared
coordinate. Every retained signed byte stays unchanged; unknown files still fail
validation. Never edit release payloads to make a rejected bundle pass.

## Upload, validation and publication are separate

After action-specific release approval, use the **same** reviewed bundle and
validation receipt. Provision portal tokens only in the authorized publisher:

```sh
python3 .github/scripts/central-recovery.py --bundle candidate.zip \
  --receipt candidate.json --state deployment.json --allow-upload
```

This uploads in `USER_MANAGED` mode and stops at `VALIDATED`. Keep the state file
as a durable private CI artifact, including on failure, **before** the worker or
checkout is removed. The file must be restored on retries. To publish the exact
validated deployment after review:

```sh
python3 .github/scripts/central-recovery.py --bundle candidate.zip \
  --receipt candidate.json --state deployment.json --publish
```

An existing state resumes by deployment ID and never uploads again. Bundle or
receipt mismatches fail closed. Upload intent is saved before the network call;
the returned deployment ID is saved before polling. A lost upload response needs
operator inspection in Central, not a blind retry. A lost publish response uses
status for the same deployment; if it remains `VALIDATED`, the tool refuses
another publish call until an operator establishes the outcome. Preserve stale
lock/temporary files after a crash for review; do not discard state to retry.
Serialize publishers for a coordinate in CI as well as with the local state lock.

The suspended upload design first called `--prepare-upload` without portal credentials
and uploads `central-intent-VERSION` before the first network call. Then it retains
`central-state-VERSION` with `always()` after the operation, including failures.
Workflow reruns cannot repeat an upload. Use **resume** with the latest state run
to poll the same deployment without publishing, or **publish** to approve that
deployment. Never use a new upload dispatch as recovery. A hard runner loss can
still leave a returned deployment ID unavailable: reconcile the persisted intent
with Central through an authorized operator before any further write.

Candidate and state artifacts have 90-day retention. Archive the exact bundle,
validation/provenance receipts and final state in the approved durable release
evidence store before expiration. Workflow artifacts are a bounded recovery path,
not a permanent archive. Do not delete failed deployment evidence as cleanup.

`PUBLISHED` must still be followed by readback of actual public POM/JAR/source/
Javadoc bytes, signatures and checksums. GitHub notes are a separate gate: repair
them from the existing tag without another package upload. A mismatch in existing
notes requires review, never automatic overwrite.

`publish-central.yml` has only a manual trigger. It
downloads the existing reviewed candidate, verifies public bytes against it and
only then creates or compares release notes from the existing tag. It cannot
build, sign, upload or publish Maven artifacts. Both workflow changes must be
reviewed and activated together; retaining the legacy automatic tag uploader
would create a competing publication path.

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

Before the next release, review the exact workflow and candidate: staging and
offline validation before tagging, approved artifact retention/reuse, serialized
upload/validation/publish jobs with durable state, pinned toolchain provenance and
actual public artifact verification. Test the real signer and candidate outputs
on the intended runner with bounded resources before enabling that path. The
synthetic fixture checks actual Maven output and GPG signatures, but excludes
the publisher extension and full adapter runtime. A production candidate and
portal operations retain their own qualification and approval gates. Record the
exact reviewed workflow head, runner grant and action authority. Merging these
tools or passing synthetic CI does not select a product version or approve a tag;
the release owner supplies those decisions under actual approval.

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

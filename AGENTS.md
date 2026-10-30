# Repository instructions

Preserve source acknowledgement, durable recovery and public repository boundaries.

## Release notes and completion

Every stable version requires a dated, reviewed section in `CHANGELOG.md` and a
GitHub Release from the same existing tag. Include changes/fixes, compatibility,
upgrade actions and known limitations. Do not present planned behavior as shipped.
A release is complete only after Maven Central publication and artifact verification,
GitHub release-note readback and the applicable qualification/documentation gates.
A `vX.Y.Z` tag starts `release.yml`, which builds an unsigned candidate and then
waits for human approval in protected environments before signing, uploading and
publishing; the alternative is offline signing on the release workstation (see
below). Snapshot publication is suspended. The release-page workflow verifies
actual public bytes before creating notes. The synthetic CI gate does not replace
full candidate review, the signer's receipt or portal validation before release.
Never rebuild or republish packages merely to repair missing release notes.
Historical backfills use verified existing tags and must not replace the latest
release. See `docs/releasing.md`.

## Production credential boundary

The primary (certify) release key never exists in GitHub, CI, agent or development
hosts. Signing and Central upload happen in exactly one of two places:

- `release.yml` jobs `sign-upload` and `publish`, which run in the protected
  environments `release` and `release-publish` (required reviewers, tag-only
  deployment rule), check out only the release tools at `TRUSTED_TOOLING_SHA` and
  never run Maven, the wrapper, tests or candidate files. Only their environment
  secrets hold the CI signing subkey and a Central token. This path exists only
  once the repository is public and those settings are in place.
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

# Repository instructions

Preserve source acknowledgement, durable recovery and public repository boundaries.

## Release notes and completion

Every stable version requires a dated, reviewed section in `CHANGELOG.md` and a
GitHub Release from the same existing tag. Include changes/fixes, compatibility,
upgrade actions and known limitations. Do not present planned behavior as shipped.
A release is complete only after Maven Central publication and artifact verification,
GitHub release-note readback and the applicable qualification/documentation gates.
Candidate inspection retains unsigned evidence only. Signing, Central upload and
publication happen offline on the release workstation (see below); snapshot
publication is suspended. The release-page workflow verifies actual public
bytes before creating notes. Tags never trigger another build/upload. The
synthetic CI gate does not replace full candidate review, the offline signer's
receipt or portal validation before release. Never rebuild or republish packages merely to repair missing release notes. Historical backfills use verified
existing tags and must not replace the latest release. See `docs/releasing.md`.

## Production credential boundary

The production signing key and Central portal token never exist in GitHub, CI,
agent or development hosts. A human release manager signs the reviewed unsigned
candidate on the trusted release workstation with `sign-candidate.py` and
uploads/publishes with `central-recovery.py`, both run from reviewed `main`,
also for a `release/0.3` source commit; the signer never executes candidate code. Never add signing or portal
credentials to repository secrets or workflows, pass them to Maven, wrappers,
plugins, tests, dependencies or candidate code, or dispatch historical workflow
revisions that used them. Moving signing back into automation requires an
enforceable approval boundary; workflow prose, an exact SHA, manual dispatch and
synthetic tests are not one. Agents never run the release signing step or handle
the release key, its passphrase or portal tokens; they may run the signer tests,
which use throwaway keys in temporary keyrings. See
`docs/release-packaging.md#offline-release-signing`.

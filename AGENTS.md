# Repository instructions

Preserve source acknowledgement, durable recovery and public repository boundaries.

## Release notes and completion

Every stable version requires a dated, reviewed section in `CHANGELOG.md` and a
GitHub Release from the same existing tag. Include changes/fixes, compatibility,
upgrade actions and known limitations. Do not present planned behavior as shipped.
A release is complete only after Maven Central publication and artifact verification,
GitHub release-note readback and the applicable qualification/documentation gates.
The tag workflow validates notes before publication and creates the GitHub Release
after the Maven publishing job succeeds. Never rebuild or republish packages merely
to repair missing release notes. Historical backfills use verified existing tags
and must not replace the latest release. See `docs/releasing.md`.

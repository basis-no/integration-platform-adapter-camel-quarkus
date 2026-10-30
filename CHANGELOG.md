# Changelog

## Unreleased

- Remove production signing and publisher credentials from candidate inspection.
  Inspection now retains unsigned evidence only, and its receipt records the
  producing workflow run. Existing artifacts remain unchanged.
- Sign releases offline: the new `sign-candidate.py` verifies a downloaded
  unsigned candidate against its receipt, signs exactly its four payloads through
  the release manager's gpg-agent, verifies each signature against the pinned
  fingerprint and assembles the validated Central bundle with a signing receipt.
  Upload and publication run from the release workstation with
  `central-recovery.py`; the production key and portal token never exist in
  GitHub. Post-publication readback compares public bytes with the unsigned
  receipt and the signed-entries digest. Snapshot publication stays suspended;
  acceptance uses locally built artifacts.

- Separate mutable `X.Y.Z-SNAPSHOT` remote-acceptance builds from immutable
  final `vX.Y.Z` Maven Central releases.
- Avoid a null-method classification failure for ordinary Camel HTTP producers,
  and log exhausted processing failures with their stack traces before DLQ parking.
- Honor the deprecated payload inline-limit alias when the replacement property
  is not configured, including for replay step checkpoints.
- Split the adapter guide into task-focused documentation for setup, lifecycle,
  idempotency, replay, ingress, payload storage, testing, configuration, and
  troubleshooting.
- Add automated configuration coverage, Markdown link validation, and strict
  warning-free public Javadocs.

## 0.3.1

First public release, on Maven Central. Functionally identical to the
internally certified 0.3.0 (message-store contract v1.0.0-rc2); this release
adds only licensing, publication metadata and packaging. Versions below 0.3.1
were internal to the Basis platform.

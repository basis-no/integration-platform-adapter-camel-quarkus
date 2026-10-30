# Releasing

The adapter has two publication lanes. Development and remote acceptance use a
mutable snapshot coordinate; users receive only immutable final releases.

The [release packaging tools](release-packaging.md) separate manual candidate
inspection, deployment recovery and publication. Tags do not build or upload
automatically. Synthetic CI checks Maven/GPG packaging; a real full adapter
candidate, trusted production signer and workflow review remain required before
another immutable release. Existing releases are never rebuilt or replaced.

## Remote acceptance

1. One time per Sonatype namespace, follow Sonatype's
   [snapshot-enablement instructions](https://central.sonatype.org/publish/publish-portal-snapshots/#enabling-snapshot-releases-for-your-namespace),
   choose **Enable SNAPSHOTs**, and confirm that the namespace shows the
   `SNAPSHOTs enabled` badge. A snapshot upload otherwise finishes the build
   but fails with HTTP 403.
2. Keep the POM on the next intended version, for example
   `0.4.0-SNAPSHOT`.
3. Finish local unit, Testcontainers, documentation, and contract checks.
4. Run the GitHub **Publish Maven SNAPSHOT** workflow from the commit to test.
   The workflow publishes to Sonatype's separate snapshot repository and does
   not create a Git tag or a Maven Central release entry.
5. Configure the validation integration to consume snapshots:

   ```xml
   <repository>
     <id>central-portal-snapshots</id>
     <url>https://central.sonatype.com/repository/maven-snapshots/</url>
     <releases><enabled>false</enabled></releases>
     <snapshots><enabled>true</enabled></snapshots>
   </repository>
   ```

6. Build the validation integration with `./mvnw -U clean verify`, deploy its
   immutable image, and record the adapter commit, build log's resolved
   timestamped snapshot, and image digest in the acceptance evidence.
7. Run the acceptance campaign. A later snapshot may replace the same
   `0.4.0-SNAPSHOT` coordinate, so rebuilding the integration can resolve newer
   adapter bytes. The already-built image digest remains reproducible.

Snapshots are pre-release artifacts: Sonatype does not apply normal Central
release validation to them, and currently removes them after 90 days. They are
therefore suitable for acceptance testing, not production dependencies.

## Final release

Under the manual workflow, inspect a signed candidate from the exact approved
source commit and explicitly selected version **before** creating a final tag.
Review its bundle hash, toolchain, plugin versions and qualification evidence.
Create the final tag only after that review and immutable-release approval:

```sh
git tag -s v0.4.0 -m "Release 0.4.0"
git push origin v0.4.0
```

The tag path never builds or uploads automatically. The manual
candidate workflow uploads the already inspected bundle in `USER_MANAGED` mode,
then separately publishes its validated deployment under action-specific human approval.
Resume by the saved deployment ID; never rebuild or start a second upload to
repair a failed publication stage. The workflows reject release-candidate,
milestone, snapshot and malformed versions.
Afterward, update integration templates and contract certification pins to the
final version. A later defect is fixed in a new patch release such as `0.4.1`;
published Central artifacts are never replaced.

## Release checklist

- Adapter tests and strict Javadocs pass.
- Contract certification and schema upgrade regression pass.
- Validation integration passes the documented acceptance campaign.
- Required platform schema and configuration changes are deployed first.
- Documentation and changelog describe the shipped behavior.
- The final tag points to the accepted commit and has not been used before.

## GitHub release notes

Before a final tag, finish a `## X.Y.Z — YYYY-MM-DD` section in `CHANGELOG.md`.
Include changes, compatibility, upgrade steps, known limitations and artifact identity.
The tag must match the source POM version. The manual release-page workflow
verifies actual public bytes against the reviewed candidate and validates the
section before creating a GitHub Release. Relative documentation links are
pinned to the released tag.

Verify the actual Central artifacts and GitHub Release body before marking the
release complete. A failed release-page step does not authorize another Maven
upload: repair the page from the existing tag and its reviewed notes. Existing
release bodies are compared, never overwritten automatically. Creating a release
page does not establish platform certification or customer deployment.

Historical backfills must use an existing tag (`gh release create --verify-tag`),
reviewed historical notes and `--latest=false`; never create a replacement tag,
rebuild packages or fabricate historical release dates.

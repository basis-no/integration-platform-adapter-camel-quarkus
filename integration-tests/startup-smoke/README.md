# Startup smoke application

A Quarkus application that depends on the packaged adapter, as an integration
does, and boots it without live services: its Kafka input is stopped, the
datasource is inactive and the Kafka brokers point at an unused local port.
It checks what only a packaged build shows:

- the adapter's beans and its dead letter route are discovered from the jar;
- the message-store route policy is attached to the Kafka input and not to an
  internal `direct:` route;
- the Kafka consumer defaults the adapter ships (manual commit, no automatic
  commit, break on first error, reconnect on a poll failure) reach the
  application's endpoint;
- the Key-Value Store is discoverable without opening its inactive datasource;
- a polling route runs uncoordinated, without a lease, since the inactive datasource
  leaves the application without a Key-Value Store;
- an internal request's failure reaches its caller without Kafka or a database.

This is a test application, not a starting point for an integration. CI runs
it after installing the adapter build, against exactly the installed version:
the fixture has no default adapter version and no snapshot repository, so a
missing or mismatched version fails instead of resolving another build.
Locally, from the repository root:

```sh
./mvnw install
version="$(./mvnw -q help:evaluate -Dexpression=project.version -DforceStdout)"
./mvnw -f integration-tests/startup-smoke/pom.xml -Dadapter.version="$version" verify
```

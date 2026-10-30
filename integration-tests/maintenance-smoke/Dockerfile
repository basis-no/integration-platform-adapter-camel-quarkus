# Supply the approved Java 21 runtime image pinned by digest.
ARG JAVA_RUNTIME_IMAGE
FROM ${JAVA_RUNTIME_IMAGE}
WORKDIR /app
COPY --chown=1001:0 target/quarkus-app/ /app/
USER 1001
ENTRYPOINT ["java", "-jar", "/app/quarkus-run.jar"]

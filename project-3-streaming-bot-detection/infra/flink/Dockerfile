FROM eclipse-temurin:17-jdk-jammy

ARG FLINK_VERSION=1.19.1
ARG SCALA_VERSION=2.12

ENV FLINK_HOME=/opt/flink
ENV PATH="${FLINK_HOME}/bin:${PATH}"

RUN apt-get update \
    && apt-get install -y --no-install-recommends bash curl ca-certificates tini \
    && curl -fsSL "https://archive.apache.org/dist/flink/flink-${FLINK_VERSION}/flink-${FLINK_VERSION}-bin-scala_${SCALA_VERSION}.tgz" \
        | tar -xz -C /opt \
    && mv "/opt/flink-${FLINK_VERSION}" "${FLINK_HOME}" \
    && groupadd --system --gid 9999 flink \
    && useradd --system --uid 9999 --gid flink --home-dir "${FLINK_HOME}" flink \
    && chown -R flink:flink "${FLINK_HOME}" \
    && ln -sf "$(command -v jcmd)" /usr/local/bin/jcmd \
    && ln -sf "$(command -v jstat)" /usr/local/bin/jstat \
    && rm -rf /var/lib/apt/lists/*

COPY infra/flink/docker-entrypoint.sh /docker-entrypoint.sh

RUN chmod +x /docker-entrypoint.sh

WORKDIR ${FLINK_HOME}

USER flink

ENTRYPOINT ["/usr/bin/tini", "--", "/docker-entrypoint.sh"]

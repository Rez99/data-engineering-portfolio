FROM apache/flink:1.19.1-scala_2.12-java17

USER root

RUN apt-get update \
    && apt-get install -y --no-install-recommends openjdk-17-jdk-headless \
    && jcmd_path="$(find /usr/lib/jvm -type f -name jcmd | head -n 1)" \
    && jstat_path="$(find /usr/lib/jvm -type f -name jstat | head -n 1)" \
    && test -n "${jcmd_path}" \
    && test -n "${jstat_path}" \
    && ln -s "${jcmd_path}" /usr/local/bin/jcmd \
    && ln -s "${jstat_path}" /usr/local/bin/jstat \
    && rm -rf /var/lib/apt/lists/*

USER flink

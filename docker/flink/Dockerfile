ARG FLINK_VERSION=1.20.1

FROM flink:${FLINK_VERSION}-scala_2.12-java17

ARG PAIMON_VERSION=1.2.0
ARG FLINK_CDC_VERSION=3.2.1
ARG FLINK_SHADED_HADOOP_VERSION=2.8.3-10.0
ARG FLINK_KAFKA_VERSION=3.3.0-1.20

USER root

# Download connector JARs.
# The Hadoop uber JAR is required by Paimon even for local filesystem.
# The CDC connector + Hadoop uber JAR have a commons-collections classloader
# conflict — resolved by adding classloader.parent-first-patterns.additional:
# org.apache.commons.collections in flink-conf / FLINK_PROPERTIES.
RUN set -eux; \
    curl -fSL \
      "https://repo.maven.apache.org/maven2/org/apache/paimon/paimon-flink-1.20/${PAIMON_VERSION}/paimon-flink-1.20-${PAIMON_VERSION}.jar" \
      -o "/opt/flink/lib/paimon-flink-1.20-${PAIMON_VERSION}.jar"; \
    curl -fSL \
      "https://repo.maven.apache.org/maven2/org/apache/flink/flink-sql-connector-postgres-cdc/${FLINK_CDC_VERSION}/flink-sql-connector-postgres-cdc-${FLINK_CDC_VERSION}.jar" \
      -o "/opt/flink/lib/flink-sql-connector-postgres-cdc-${FLINK_CDC_VERSION}.jar"; \
    curl -fSL \
      "https://repo.maven.apache.org/maven2/org/apache/flink/flink-shaded-hadoop-2-uber/${FLINK_SHADED_HADOOP_VERSION}/flink-shaded-hadoop-2-uber-${FLINK_SHADED_HADOOP_VERSION}.jar" \
      -o "/opt/flink/lib/flink-shaded-hadoop-2-uber-${FLINK_SHADED_HADOOP_VERSION}.jar"; \
    curl -fSL \
      "https://repo.maven.apache.org/maven2/org/apache/flink/flink-sql-connector-kafka/${FLINK_KAFKA_VERSION}/flink-sql-connector-kafka-${FLINK_KAFKA_VERSION}.jar" \
      -o "/opt/flink/lib/flink-sql-connector-kafka-${FLINK_KAFKA_VERSION}.jar"; \
    mkdir -p /workspace/data/paimon && chown -R flink:flink /workspace/data/paimon

USER flink

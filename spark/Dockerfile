FROM apache/spark:3.5.9-scala2.12-java17-python3-ubuntu

USER root
ARG ICEBERG_VERSION=1.9.2
RUN apt-get update \
    && apt-get install -y --no-install-recommends curl \
    && curl -fsSL "https://repo.maven.apache.org/maven2/org/apache/iceberg/iceberg-spark-runtime-3.5_2.12/${ICEBERG_VERSION}/iceberg-spark-runtime-3.5_2.12-${ICEBERG_VERSION}.jar" \
       -o "/opt/spark/jars/iceberg-spark-runtime-3.5_2.12-${ICEBERG_VERSION}.jar" \
    && chmod 644 "/opt/spark/jars/iceberg-spark-runtime-3.5_2.12-${ICEBERG_VERSION}.jar" \
    && apt-get clean \
    && rm -rf /var/lib/apt/lists/*

COPY conf/spark-defaults.conf /opt/spark/conf/spark-defaults.conf
COPY conf/core-site.xml /opt/spark/conf/core-site.xml
COPY conf/hdfs-site.xml /opt/spark/conf/hdfs-site.xml
USER 185

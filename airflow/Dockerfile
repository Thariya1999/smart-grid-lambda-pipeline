FROM apache/airflow:2.9.3-python3.11
USER root
# spark-submit is invoked via SparkSubmitOperator/BashOperator against the
# spark-master service. The apache-airflow-providers-apache-spark package
# (via pyspark) provides the spark-submit *script*, but that script is a
# thin wrapper that execs a JVM -- without an actual Java runtime installed
# in this image, it fails in well under a second with "JAVA_HOME is not
# set" / "Error: No spark-submit found" before Spark ever starts. Install
# a headless JDK and point JAVA_HOME at it so spark-submit can actually run.
RUN apt-get update && \
    apt-get install -y --no-install-recommends default-jdk-headless && \
    apt-get clean && \
    rm -rf /var/lib/apt/lists/*
ENV JAVA_HOME=/usr/lib/jvm/default-java
ENV PATH="${JAVA_HOME}/bin:${PATH}"
USER airflow
COPY airflow/requirements.txt /requirements.txt
RUN pip install --no-cache-dir -r /requirements.txt

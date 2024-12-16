FROM apache/airflow:2.9.2-python3.12

# Set environment variables
ENV AIRFLOW_HOME=/opt/airflow

COPY requirements.txt /
RUN pip install --no-cache-dir "apache-airflow==${AIRFLOW_VERSION}" -r /requirements.txt

USER root

# Install dependencies for python
RUN apt-get update -qq && apt-get install -y --no-install-recommends \
    vim \
    wget \
    tar \
    && rm -rf /var/lib/apt/lists/*

# Set env variables for Spark
ENV SPARK_VERSION=3.5.2
ENV HADOOP_VERSION=3

# Install JDK for Spark
RUN mkdir -p /opt/spark
RUN wget -q https://download.java.net/java/GA/jdk11/9/GPL/openjdk-11.0.2_linux-x64_bin.tar.gz && \
    tar -xzf openjdk-11.0.2_linux-x64_bin.tar.gz && \
    mv jdk-11.0.2 /opt/spark && \
    rm openjdk-11.0.2_linux-x64_bin.tar.gz

# Install Apache Spark
RUN wget -q https://archive.apache.org/dist/spark/spark-${SPARK_VERSION}/spark-${SPARK_VERSION}-bin-hadoop${HADOOP_VERSION}.tgz && \
    tar -xf spark-${SPARK_VERSION}-bin-hadoop${HADOOP_VERSION}.tgz && \
    mv spark-${SPARK_VERSION}-bin-hadoop${HADOOP_VERSION} /opt/spark && \
    rm spark-${SPARK_VERSION}-bin-hadoop${HADOOP_VERSION}.tgz

# Add installations to path
ENV SPARK_HOME=/opt/spark/spark-${SPARK_VERSION}-bin-hadoop${HADOOP_VERSION}
ENV PATH=${SPARK_HOME}/bin:${PATH}
ENV JAVA_HOME=/opt/spark/jdk-11.0.2
ENV PATH=${JAVA_HOME}/bin:${PATH}
# set spark localhost value in spark-env.sh file
RUN cp $SPARK_HOME/conf/spark-env.sh.template $SPARK_HOME/conf/spark-env.sh && echo "SPARK_MASTER_HOST=127.0.0.1" >> $SPARK_HOME/conf/spark-env.sh

# Install Google Cloud SDK
ARG CLOUD_SDK_VERSION=468.0.0
ENV GCLOUD_HOME=/home/google-cloud-sdk
ENV PATH="${GCLOUD_HOME}/bin/:${PATH}"

RUN DOWNLOAD_URL="https://dl.google.com/dl/cloudsdk/channels/rapid/downloads/google-cloud-sdk-${CLOUD_SDK_VERSION}-linux-x86_64.tar.gz" \
    && TMP_DIR="$(mktemp -d)" \
    && curl -fL "${DOWNLOAD_URL}" --output "${TMP_DIR}/google-cloud-sdk.tar.gz" \
    && mkdir -p "${GCLOUD_HOME}" \
    && tar xzf "${TMP_DIR}/google-cloud-sdk.tar.gz" -C "${GCLOUD_HOME}" --strip-components=1 \
    && "${GCLOUD_HOME}/install.sh" \
       --bash-completion=false \
       --path-update=false \
       --usage-reporting=false \
       --quiet \
    && rm -rf "${TMP_DIR}" \
    && gcloud --version

WORKDIR $AIRFLOW_HOME

USER $AIRFLOW_UID

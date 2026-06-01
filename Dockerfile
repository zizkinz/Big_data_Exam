FROM python:3.11-slim

ENV DEBIAN_FRONTEND=noninteractive \
    PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    SPARK_VERSION=3.5.1 \
    HADOOP_VERSION=3 \
    SPARK_HOME=/opt/spark \
    JAVA_HOME=/usr/lib/jvm/java-21-openjdk-arm64

WORKDIR /app

RUN apt-get update && apt-get install -y --no-install-recommends \
    openjdk-21-jdk \
    curl \
    bash \
    tini \
    && rm -rf /var/lib/apt/lists/*

RUN curl -fsSL https://archive.apache.org/dist/spark/spark-${SPARK_VERSION}/spark-${SPARK_VERSION}-bin-hadoop${HADOOP_VERSION}.tgz \
    | tar -xz -C /opt \
    && mv /opt/spark-${SPARK_VERSION}-bin-hadoop${HADOOP_VERSION} ${SPARK_HOME}

ENV PATH="${SPARK_HOME}/bin:${SPARK_HOME}/sbin:${PATH}"

COPY requirements.txt /app/requirements.txt
RUN pip install --no-cache-dir -r /app/requirements.txt

COPY . /app

ENTRYPOINT ["/usr/bin/tini", "--"]
CMD ["bash"]
FROM python:3.11-slim

ENV DEBIAN_FRONTEND=noninteractive \
    PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    SPARK_VERSION=3.5.1 \
    HADOOP_VERSION=3 \
    SPARK_HOME=/opt/spark

WORKDIR /app

RUN apt-get update && apt-get install -y --no-install-recommends \
    openjdk-21-jdk \
    curl \
    bash \
    tini \
    && rm -rf /var/lib/apt/lists/*

RUN curl -# -fL --retry 5 --retry-delay 5 \
    -o /tmp/spark.tgz \
    https://repo.huaweicloud.com/apache/spark/spark-3.5.1/spark-3.5.1-bin-hadoop3.tgz \
    && tar -xzf /tmp/spark.tgz -C /opt \
    && mv /opt/spark-3.5.1-bin-hadoop3 /opt/spark \
    && rm /tmp/spark.tgz

ENV PATH="${SPARK_HOME}/bin:${SPARK_HOME}/sbin:${PATH}"

COPY requirements.txt /app/requirements.txt
RUN pip install --no-cache-dir -r /app/requirements.txt

COPY . /app

ENTRYPOINT ["/usr/bin/tini", "--"]
CMD ["bash"]
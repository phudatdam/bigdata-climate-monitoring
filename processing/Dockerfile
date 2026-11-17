FROM python:3.11-slim-bookworm

ENV DEBIAN_FRONTEND=noninteractive \
    JAVA_HOME=/usr/lib/jvm/java-17-openjdk-amd64 \
    PATH="/usr/lib/jvm/java-17-openjdk-amd64/bin:${PATH}" \
    PYTHONUNBUFFERED=1

RUN apt-get update \
    && apt-get install -y --no-install-recommends openjdk-17-jre-headless \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /workspace
COPY processing/requirements.txt /workspace/processing/requirements.txt
RUN pip install --no-cache-dir -r /workspace/processing/requirements.txt
COPY processing /workspace/processing
COPY Ingestion/__init__.py /workspace/Ingestion/__init__.py
COPY Ingestion/utils/__init__.py /workspace/Ingestion/utils/__init__.py
COPY Ingestion/utils/locations.py /workspace/Ingestion/utils/locations.py

CMD ["python", "-m", "processing.streaming.streaming_flow"]

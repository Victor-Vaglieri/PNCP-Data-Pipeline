FROM python:3.12-slim-bookworm

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    JAVA_HOME=/usr/lib/jvm/java-17-openjdk-amd64

RUN apt-get update \
    && apt-get install -y --no-install-recommends openjdk-17-jre-headless procps \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /app

COPY pyproject.toml ./
COPY src ./src

RUN pip install --no-cache-dir -e ".[spark]"
RUN pip install --no-cache-dir pytest respx ruff
RUN useradd -u 50000 -g 0 -m -s /bin/bash airflow \
    && chown -R 50000:0 /app

USER 50000:0

CMD ["python", "-m", "pncp_pipeline.transformations", "spark"]

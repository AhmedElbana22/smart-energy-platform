FROM python:3.11-slim

WORKDIR /workspace

RUN apt-get update \
    && apt-get install -y --no-install-recommends git \
    && rm -rf /var/lib/apt/lists/* \
    && mkdir -p /root/.dbt

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

ENTRYPOINT ["dbt"]

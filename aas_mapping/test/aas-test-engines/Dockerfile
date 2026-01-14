FROM python:3.11-slim

WORKDIR /app

RUN apt-get update && apt-get install -y \
    gcc \
    && rm -rf /var/lib/apt/lists/*

COPY pyproject.toml .
RUN pip install --no-cache-dir .

COPY aas_mapping/ ./aas_mapping/
COPY aas_mapping/test/aas-test-engines/server.py .

EXPOSE 80

ENV PYTHONUNBUFFERED=1

CMD ["python", "server.py"]

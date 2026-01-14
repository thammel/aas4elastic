FROM python:3.11-slim

WORKDIR /app

RUN apt-get update && apt-get install -y \
    gcc \
    && rm -rf /var/lib/apt/lists/*

COPY pyproject.toml /app/
COPY aas_mapping/ /app/aas_mapping/

RUN pip install --no-cache-dir .

COPY aas_mapping/test/aas-test-engines/server.py /app/server.py

EXPOSE 80

ENV PYTHONUNBUFFERED=1

CMD ["python", "server.py"]

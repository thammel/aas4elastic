FROM python:3.11-slim

WORKDIR /app

RUN apt-get update && apt-get install -y \
    gcc \
    && rm -rf /var/lib/apt/lists/*

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY aas_mapping/ ./aas_mapping/
COPY server.py .

EXPOSE 8080

ENV PYTHONUNBUFFERED=1

CMD ["python", "server.py"]

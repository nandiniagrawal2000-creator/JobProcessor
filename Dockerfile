FROM python:3.12-slim

ENV PYTHONUNBUFFERED=1 \
    PYTHONDONTWRITEBYTECODE=1 \
    PORT=8080

WORKDIR /app

# Install dependencies first so this layer is cached across code changes.
COPY requirements.txt ./
RUN pip install --no-cache-dir -r requirements.txt

COPY app ./app

EXPOSE 8080

# DigitalOcean App Platform injects $PORT (default 8080). Bind 0.0.0.0 so the
# platform can route to the container. Single process: the in-memory queue/store
# is not shared across instances, so keep instance_count = 1 (see .do/app.yaml).
CMD ["sh", "-c", "uvicorn app.main:app --host 0.0.0.0 --port ${PORT:-8080}"]

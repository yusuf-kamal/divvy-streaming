FROM python:3.11-slim-bookworm

ENV PYTHONUNBUFFERED=1 PYTHONDONTWRITEBYTECODE=1
WORKDIR /app

# Install dependencies first so Docker can cache this layer.
COPY producer/requirements.txt producer/requirements.txt
RUN pip install --no-cache-dir -r producer/requirements.txt

COPY producer/ producer/

CMD ["python", "producer/producer.py"]
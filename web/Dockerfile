FROM python:3.12-slim

WORKDIR /app

# System deps (if needed) – keep minimal
RUN apt-get update && apt-get install -y --no-install-recommends \
    && rm -rf /var/lib/apt/lists/*

# Copy app code
COPY app.py /app/
COPY static /app/static

# Install Python deps
RUN pip install --no-cache-dir aiohttp numpy

EXPOSE 8080/ tcp

CMD ["python", "app.py"]

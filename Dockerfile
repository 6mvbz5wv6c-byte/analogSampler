# Raspberry Pi (64-bit OS) friendly base
FROM python:3.11-slim

# Faster/cleaner Python behavior in containers
ENV PYTHONDONTWRITEBYTECODE=1
ENV PYTHONUNBUFFERED=1

# Toolchain so pip can compile RPi.GPIO & spidev C extensions
RUN apt-get update && apt-get install -y --no-install-recommends \
    build-essential python3-dev linux-libc-dev \
 && rm -rf /var/lib/apt/lists/*

# App files
WORKDIR /app
# (add any other modules you use, e.g., config.py, ADS1256.py, etc.)
COPY main.py sampler_worker.py requirements.txt ADS1256.py config.py ./

# Python deps
RUN pip install --no-cache-dir -r requirements.txt

# (Optional) sanity print of environment during build
# RUN python -c "import sys; print(sys.version); import RPi.GPIO as G; print('GPIO', G.VERSION)"

ENTRYPOINT ["python", "main.py"]

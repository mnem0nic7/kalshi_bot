FROM python:3.12-slim

ENV PYTHONDONTWRITEBYTECODE=1
ENV PYTHONUNBUFFERED=1

WORKDIR /app

# Install dependencies first (cached unless pyproject.toml changes)
COPY pyproject.toml README.md ./
RUN mkdir -p src/kalshi_bot && touch src/kalshi_bot/__init__.py \
    && pip install --no-cache-dir --upgrade pip \
    && pip install --no-cache-dir -e . \
    && rm -rf src

RUN apt-get update && apt-get install -y --no-install-recommends libgomp1 && rm -rf /var/lib/apt/lists/*

# Copy source after deps are cached
COPY src ./src
COPY alembic ./alembic
COPY alembic.ini ./
COPY docs ./docs
COPY infra/config ./infra/config
COPY scripts/crypto_live_path_refresh.sh ./scripts/crypto_live_path_refresh.sh
RUN chmod +x ./scripts/crypto_live_path_refresh.sh

# The cached editable install points at /app/src; PYTHONPATH keeps source-only
# rebuilds from paying another package installation step.
ENV PYTHONPATH=/app/src

CMD ["python", "-m", "kalshi_bot.main"]

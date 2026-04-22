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

# Copy source after deps are cached
COPY src ./src
COPY alembic ./alembic
COPY alembic.ini ./
COPY docs ./docs

# Re-register editable install with real source (no dep download)
RUN pip install --no-cache-dir -e .

CMD ["python", "-m", "kalshi_bot.main"]

FROM python:3.12-slim
COPY --from=ghcr.io/astral-sh/uv:0.7.18 /uv /usr/local/bin/uv

WORKDIR /app
ENV UV_COMPILE_BYTECODE=1 PATH="/app/.venv/bin:$PATH"

# Dependencies first so this layer is cached until the lockfile changes.
COPY pyproject.toml uv.lock ./
RUN uv sync --frozen --no-dev

COPY synthetic_data_generation ./synthetic_data_generation
CMD ["python", "-m", "synthetic_data_generation.generate_data"]

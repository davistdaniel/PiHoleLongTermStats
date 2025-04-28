FROM python:3.13-slim-bookworm
COPY --from=ghcr.io/astral-sh/uv:latest /uv /uvx /bin/

ENV UV_LINK_MODE=copy
ENV UV_COMPILE_BYTECODE=1

# Change the working directory to the `app` directory
WORKDIR /app

# Copy only the files needed for dependency installation
COPY uv.lock pyproject.toml ./

# Install dependencies using a cache mount
RUN --mount=type=cache,target=/root/.cache/uv \
    uv sync --locked --no-install-project

# Copy the rest of the application code
COPY app.py .

# Sync the project itself (including installation)
RUN --mount=type=cache,target=/root/.cache/uv \
    uv sync --locked

# Run the app
CMD ["uv", "run", "app.py"]
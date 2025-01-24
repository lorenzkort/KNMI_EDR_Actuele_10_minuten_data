# syntax=docker/dockerfile:1

FROM python:3.11-slim

# Prevents Python from writing pyc files.
ENV PYTHONDONTWRITEBYTECTURE=1

# Keeps Python from buffering stdout and stderr to avoid situations where
# the application crashes without emitting any logs due to buffering.
ENV PYTHONUNBUFFERED=1

WORKDIR /app

# Create a non-privileged user that the app will run under.
# See https://docs.docker.com/go/dockerfile-user-best-practices/
# ARG UID=10001
# RUN id -u appuser &>/dev/null || \
#     adduser \
#     --disabled-password \
#     --gecos "" \
#     --home "/nonexistent" \
#     --shell "/sbin/nologin" \
#     --no-create-home \
#     --uid "${UID}" \
#     appuser

# Download dependencies as a separate step to take advantage of Docker's caching.
# Leverage a cache mount to /root/.cache/pip to speed up subsequent builds.
# Leverage a bind mount to requirements.txt to avoid having to copy them into
# into this layer.
RUN --mount=type=cache,target=/root/.cache/pip \
    --mount=type=bind,source=requirements.txt,target=requirements.txt \
    python -m pip install -r requirements.txt

# Create necessary directories and ensure they're writable
RUN mkdir -p /app/logs /app/dataset-download /app/parquet_files /app/parquet_files_validated && \
    chmod 777 /app/logs /app/dataset-download /app/parquet_files /app/parquet_files_validated

# Switch to the non-privileged user to run the application.
# USER appuser

# Copy the source code and setup files into the container.
COPY setup.py /app/
COPY src/ /app/src/
COPY dags/ /app/dags/

# Install the local package in development mode
RUN pip install -e .

# Expose the port that the application listens on.
EXPOSE 8000

# Run the pipeline with improved logging
CMD python dags/local_dag.py

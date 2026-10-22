FROM python:3.11-slim

# Set working directory
WORKDIR /app

# Prevent Python from writing .pyc files and enable unbuffered logging
ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    SERVICE_PORT=8025 \
    HOST=0.0.0.0

# Install minimal system tools (curl for healthchecks)
RUN apt-get update && apt-get install -y --no-install-recommends \
    curl \
    && rm -rf /var/lib/apt/lists/*

# Install python dependencies
COPY requirements.txt .
RUN pip install --no-cache-dir --upgrade pip && \
    pip install --no-cache-dir -r requirements.txt

# Copy microservice application files
COPY ticket_service.py .
COPY static/ ./static/
COPY templates/ ./templates/

# Expose Ticket Service port
EXPOSE 8025

# Health check probe
HEALTHCHECK --interval=30s --timeout=5s --start-period=15s --retries=3 \
  CMD curl -f http://localhost:8025/api/ticket/health || exit 1

# Run Ticket Service via Uvicorn
CMD ["uvicorn", "ticket_service:app", "--host", "0.0.0.0", "--port", "8025"]

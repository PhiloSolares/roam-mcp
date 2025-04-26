# Base image
FROM python:3.11-slim

# Set environment variables
ENV PYTHONDONTWRITEBYTECODE=1
ENV PYTHONUNBUFFERED=1
ENV ROAM_API_TOKEN=""
ENV ROAM_GRAPH_NAME=""
ENV MEMORIES_TAG="#[[Memories]]"

# Install system dependencies for PDF processing
RUN apt-get update && \
    apt-get install -y --no-install-recommends \
        gcc \
        poppler-utils \
        libmagic1 \
    && apt-get clean \
    && rm -rf /var/lib/apt/lists/*

# Create a non-root user
RUN useradd -m appuser

# Create and set working directory
WORKDIR /app

# Copy requirements file for caching
COPY --chown=appuser:appuser requirements.txt ./

# Install Python dependencies
RUN pip install --no-cache-dir --upgrade pip && \
    pip install --no-cache-dir -r requirements.txt

# Copy the application code
COPY --chown=appuser:appuser . .

# Change to non-root user
USER appuser

# Expose port for SSE transport
EXPOSE 3000

# Command to run the application (can be overridden)
CMD ["python", "-m", "roam_mcp.cli", "--transport", "sse", "--port", "3000"]
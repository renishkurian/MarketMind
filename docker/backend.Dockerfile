FROM python:3.11-slim

WORKDIR /app

ENV PYTHONDONTWRITEBYTECODE=1
ENV PYTHONUNBUFFERED=1

RUN apt-get update \
    && apt-get install -y gcc default-libmysqlclient-dev pkg-config \
    && apt-get clean

COPY backend/requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# We need to install uvicorn globally because we run it from the root
COPY backend/ ./backend/
COPY utils/ ./utils/
COPY scripts/ ./scripts/

# Create non-root user
RUN adduser --disabled-password marketmind
USER marketmind

CMD ["uvicorn", "backend.main:app", "--host", "0.0.0.0", "--port", "8000"]

FROM python:3.13.5-slim AS base

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1

WORKDIR /app

COPY requirements.txt .
RUN pip install --no-cache-dir --requirement requirements.txt

COPY extract.py .

FROM base AS test

COPY test_extract.py .
RUN python -m unittest --verbose

FROM base AS runtime

USER 65532:65532

ENTRYPOINT ["python", "extract.py"]

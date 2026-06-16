FROM python:3.13.5-slim AS base

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1

WORKDIR /app

COPY requirements.txt .
RUN pip install --no-cache-dir --requirement requirements.txt

COPY cloud_job.py train.py ./

FROM base AS test

COPY test_cloud_job.py test_train.py ./
RUN python -m unittest --verbose

FROM base AS runtime

USER 65532:65532

ENTRYPOINT ["python", "cloud_job.py"]

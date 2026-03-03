ARG PYTHON_VERSION=3.11
ARG PYSPARK_VERSION=3.5.0

FROM python:${PYTHON_VERSION}-slim AS py3

FROM eclipse-temurin:8-jdk


ARG PYSPARK_VERSION

COPY --from=py3 / /

RUN pip --no-cache-dir install pyspark==${PYSPARK_VERSION}

COPY . .

CMD ["python", "main.py"]
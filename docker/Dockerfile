# One image for every component; docker-compose selects it with the command (ingestion, processor, api, ...).
FROM python:3.12-slim AS build
WORKDIR /app
COPY pyproject.toml README.md ./
COPY src ./src
RUN pip install --no-cache-dir --prefix=/install ".[kafka]"

FROM python:3.12-slim
ENV PYTHONUNBUFFERED=1
WORKDIR /app
COPY --from=build /install /usr/local
COPY config ./config
RUN useradd --system --uid 10001 monitoring
USER monitoring
EXPOSE 8000
ENTRYPOINT ["python", "-m", "monitoring"]
CMD ["api"]

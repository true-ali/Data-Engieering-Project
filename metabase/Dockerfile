FROM curlimages/curl:8.11.1 AS downloader

ARG CLICKHOUSE_DRIVER_VERSION=1.53.4
RUN curl -fL \
    "https://github.com/ClickHouse/metabase-clickhouse-driver/releases/download/${CLICKHOUSE_DRIVER_VERSION}/clickhouse.metabase-driver.jar" \
    -o /tmp/clickhouse.metabase-driver.jar

FROM metabase/metabase:v0.53.11

# Install ClickHouse driver in the image so fresh clones work without manual plugin setup.
COPY --from=downloader /tmp/clickhouse.metabase-driver.jar /plugins/clickhouse.metabase-driver.jar

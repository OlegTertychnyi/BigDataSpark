#!/bin/bash

set -euo pipefail

echo "==> [1/4] Starting infrastructure..."
docker compose up -d --build

echo "==> Waiting for Postgres and ClickHouse to become healthy..."
for i in {1..30}; do
    pg_ok=$(docker compose ps --format json postgres   | grep -c '"Health":"healthy"' || true)
    ch_ok=$(docker compose ps --format json clickhouse | grep -c '"Health":"healthy"' || true)
    if [ "$pg_ok" -eq 1 ] && [ "$ch_ok" -eq 1 ]; then
        echo "    Postgres and ClickHouse are healthy"
        break
    fi
    sleep 3
    if [ "$i" -eq 30 ]; then
        echo "!!! Timeout waiting for services"
        docker compose ps
        exit 1
    fi
done

rows=$(docker compose exec -T postgres psql -U spark_user -d bigdataspark -tAc \
       "SELECT COUNT(*) FROM raw.mock_data" 2>/dev/null || echo "0")
echo "    raw.mock_data: $rows rows"
if [ "$rows" -ne 10000 ]; then
    echo "!!! Expected 10000 rows, got $rows"
    exit 1
fi

echo ""
echo "==> [2/4] Spark job: raw -> star schema..."
docker compose exec -T spark-master \
    /opt/spark/bin/spark-submit \
    --master spark://spark-master:7077 \
    /opt/spark-jobs/01_transform_to_star.py

echo ""
echo "==> [3/4] Creating ClickHouse datamarts DDL..."
docker compose exec -T clickhouse \
    clickhouse-client --user=ch_user --password=ch_pass --multiquery \
    < sql/03_clickhouse_datamarts.sql

echo ""
echo "==> [4/4] Spark job: star -> ClickHouse datamarts..."
docker compose exec -T spark-master \
    /opt/spark/bin/spark-submit \
    --master spark://spark-master:7077 \
    /opt/spark-jobs/02_build_datamarts.py

echo ""
echo "==> Pipeline completed successfully"
echo ""
echo "Access:"
echo "  - PostgreSQL:  localhost:5433 (spark_user / spark_pass)"
echo "  - ClickHouse:  localhost:8123 (ch_user / ch_pass)"
echo "  - Spark UI:    http://localhost:8080"
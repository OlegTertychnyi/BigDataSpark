# BigDataSpark

Анализ больших данных - лабораторная работа №2 - ETL реализованный с помощью Spark

# BigDataSpark — Лабораторная работа №2

ETL на Apache Spark: CSV → PostgreSQL (star schema) → ClickHouse (6 витрин).

## Запуск

```bash
git clone https://github.com/OlegTertychnyi/BigDataSpark.git
cd BigDataSpark
bash run.sh
```

Скрипт поднимает контейнеры и прогоняет весь пайплайн:

1. Docker Compose поднимает 4 контейнера (Postgres, Spark master/worker, ClickHouse)
2. Postgres автоматически грузит 10 CSV в `raw.mock_data` (через init-db/)
3. Spark-джоба №1 строит star schema (6 dim + fact) в Postgres
4. Создаются DDL 6 витрин в ClickHouse
5. Spark-джоба №2 заполняет витрины из star schema

Первый запуск: ~3-5 минут (скачивание + сборка образа Spark).

## Проверка результатов

**Star schema в Postgres:**
```bash
docker compose exec postgres psql -U spark_user -d bigdataspark \
  -c "SELECT COUNT(*) FROM star.fact_sales;"
```

**Витрины в ClickHouse:**
```bash
docker compose exec -T clickhouse \
  clickhouse-client --user=ch_user --password=ch_pass --multiquery --format=Pretty \
  < sql/04_check_datamarts.sql | head -300
```

## Доступ через DBeaver

| БД | Host | Port | Database | User | Password |
|---|---|---|---|---|---|
| PostgreSQL | localhost | 5433 | bigdataspark | spark_user | spark_pass |
| ClickHouse | localhost | 8123 | analytics | ch_user | ch_pass |

- **Spark Master UI:** http://localhost:8080
- **Spark Worker UI:** http://localhost:8081

## Запустить повторно / пересоздать

```bash
# Заново прогнать пайплайн без очистки БД
docker compose restart
bash run.sh

# Полная очистка и старт с нуля
docker compose down -v
bash run.sh
```
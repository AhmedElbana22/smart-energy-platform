#!/usr/bin/env bash
set -e

mkdir -p /opt/airflow/logs
chown -R "${AIRFLOW_UID:-50000}:0" /opt/airflow/logs
airflow db migrate

if ! airflow users list 2>/dev/null | grep -q "${AIRFLOW_ADMIN_USER}"; then
  airflow users create \
    --username "${AIRFLOW_ADMIN_USER}" \
    --password "${AIRFLOW_ADMIN_PASSWORD}" \
    --firstname Local \
    --lastname Admin \
    --role Admin \
    --email local-admin@example.com
fi

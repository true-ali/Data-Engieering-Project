FROM apache/airflow:2.10.4-python3.11

USER airflow
RUN pip install --no-cache-dir \
    clickhouse-connect==0.8.6 \
    confluent-kafka==2.6.1 \
    deltalake==0.20.1 \
    pandas==2.2.3 \
    requests==2.32.3

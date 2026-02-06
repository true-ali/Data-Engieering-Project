# Data Engineering Course Project – Clear Instructions

## Overview
This project focuses on designing and implementing an end-to-end data engineering pipeline. You will select a real data source, ingest data continuously, stream it through Apache Kafka, store it in analytical storage systems, and finally build analytical dashboards.

## Phase 1: Data Source Selection
Choose one primary data source such as online marketplaces (Divar, Digikala) or weather APIs. You may focus on a specific category like housing, vehicles, or weather metrics.

## Phase 2: Data Pipeline Design
Design a pipeline to ingest data via API and publish it to Apache Kafka using streaming, micro-batch, or batch modes. The pipeline must run daily from a fixed start date using Airflow or Prefect.

## Phase 3: Data Storage
Transfer data from Kafka into an analytical database such as ClickHouse and a lakehouse system such as Iceberg, Delta Lake, or Hudi.

## Phase 4: Analytical Dashboard
Create dashboards using Apache Superset or Metabase connected to the analytical database.

## Evaluation Criteria
**Quantitative**
- Number of pipelines  
- Diversity  
- Data volume  
- Dashboards  
- Tools  

**Qualitative**
- Innovation  
- Documentation quality  
- Dockerization  
- Scalability  
- 
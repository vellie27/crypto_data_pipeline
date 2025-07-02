
##postgreSQL-to-Cassandra CDC Pipeline

#Overview

This pipeline synchronizes data between PostgreSQL (source) and Cassandra (target) using a two-phase Change Data Capture (CDC) approach. It provides near real-time data replication without requiring Kafka or Debezium.

##Architecture

PostgreSQL → CDC Pipeline → Cassandra
           (Two-phase sync)

##Phase 1: Initial Setup & Table Synchronization

#Purpose

Establishes database connections

Ensures Cassandra schema matches PostgreSQL structure


##Components

#Database Connections

PostgreSQL connection via psycopg2

Cassandra session with keyspace creation

#Table Management

Creates required Cassandra tables if they don't exist

Defines proper primary keys and clustering columns

#Configuration

Set these environment variables:

bash
# PostgreSQL
PG_HOST=your_postgres_host
PG_DATABASE=your_database
PG_USER=your_username
PG_PASSWORD=your_password

# Cassandra
CASSANDRA_HOSTS=host1,host2
CASSANDRA_PORT=9042
CASSANDRA_KEYSPACE=your_keyspace
CASSANDRA_USER=your_username  # Optional
CASSANDRA_PASSWORD=your_password  # Optional

## 2: Continuous Incremental Replication

#Purpose
Periodically polls PostgreSQL for changes

Replicates new data to Cassandra

#Features

Configurable sync interval (default: 30 seconds)

Type conversion between PostgreSQL and Cassandra

Basic error handling and logging

Configuration
bash
# CDC Settings
CDC_CHECK_INTERVAL=30  # Seconds between syncs
CDC_LOOKBACK=300  # Seconds to look back on initial run
##How to Run
Install dependencies:

bash
pip install psycopg2-binary cassandra-driver
Run the pipeline:

python
python cdc_pipeline.py
##Monitoring
The pipeline logs these events:

1.Connection successes/failures

2.Table creation results

3.Number of records replicated per cycle

4.Errors during replication


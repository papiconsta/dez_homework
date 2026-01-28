# Exercise 1 - Data Ingestion Pipeline with Docker

## Overview

This project implements a data ingestion pipeline that loads NYC taxi trip data (Parquet and CSV formats) into a PostgreSQL database. The pipeline is containerized using Docker and orchestrated with Docker Compose.

## Learning Outcomes

- Docker fundamentals: images, containers, volumes, and networks
- Building custom Docker images with a Dockerfile
- Container networking for inter-container communication
- Docker Compose for multi-container orchestration
- Python data pipelines with chunked data processing
- Using `uv` as a modern Python package manager

## Architecture

```
┌─────────────────┐     ┌─────────────────┐     ┌─────────────────┐
│  Data Files     │     │  Ingestion      │     │  PostgreSQL     │
│  (.parquet/csv) │ ──► │  Container      │ ──► │  Database       │
└─────────────────┘     └─────────────────┘     └─────────────────┘
                               │                        │
                               └────── pg-network ──────┘
                                            │
                                    ┌───────┴───────┐
                                    │   pgAdmin     │
                                    │   (Web UI)    │
                                    └───────────────┘
```

## Project Structure

```
exercise_1/
├── Dockerfile              # Custom Python image for ingestion script
├── docker-compose.yaml     # PostgreSQL + pgAdmin services
├── ingest_data.py          # Data ingestion CLI script
├── pipeline.ipynb          # Jupyter notebook for development/testing
├── pyproject.toml          # Python dependencies (uv)
├── uv.lock                 # Locked dependencies
├── data/                   # Data files directory
└── ny_taxi_postgres_data/  # Parquet/CSV source files
```

## Components

### 1. Data Ingestion Script (`ingest_data.py`)

A CLI tool built with `click` that:
- Connects to PostgreSQL using SQLAlchemy
- Supports both CSV and Parquet file formats
- Processes data in chunks to handle large files efficiently
- Creates tables and appends data automatically

**Parameters:**
| Parameter | Description |
|-----------|-------------|
| `--pg_user` | PostgreSQL username |
| `--pg_password` | PostgreSQL password |
| `--pg_host` | Database host (default: localhost) |
| `--pg_port` | Database port (default: 5432) |
| `--pg_db` | Target database name |
| `--target_table` | Table name to create/append |
| `--chunk_size` | Rows per batch for ingestion |
| `--target_dataset` | Path to source data file |

### 2. Dockerfile

Uses a multi-stage approach with `uv` for fast dependency management:
- Base image: `python:3.13-slim`
- Copies `uv` binary from official image
- Syncs dependencies from lockfile
- Sets entrypoint to run ingestion script

### 3. Docker Compose Services

| Service | Image | Port | Purpose |
|---------|-------|------|---------|
| `pgdatabase` | postgres:18 | 5433:5432 | PostgreSQL database |
| `pgadmin` | dpage/pgadmin4 | 8085:80 | Database web UI |

**Credentials:**
- PostgreSQL: `root` / `root`
- pgAdmin: `admin@admin.com` / `root`

## Usage

### Prerequisites

1. Create the Docker network:
```bash
docker network create pg-network
```

2. Start the database services:
```bash
docker-compose up -d
```

### Option A: Run Natively (Development)

```bash
uv run python ingest_data.py \
  --pg_user=root \
  --pg_password=root \
  --pg_host=localhost \
  --pg_port=5433 \
  --pg_db=ny_taxi \
  --target_table=green_trip_data \
  --chunk_size=10000 \
  --target_dataset=./ny_taxi_postgres_data/green_tripdata_2025-11.parquet
```

### Option B: Run Containerized

#### Step 1: Build the image

```bash
docker build -t green_trips:v1 .
```

#### Step 2: Run the container

```bash
docker run \
  --network=pg-network \
  -v $(pwd)/ny_taxi_postgres_data:/data \
  green_trips:v1 \
  --pg_user=root \
  --pg_password=root \
  --pg_host=pgdatabase \
  --pg_db=ny_taxi \
  --target_table=green_trips_v1 \
  --chunk_size=10000 \
  --target_dataset=/data/green_tripdata_2025-11.parquet
```

**Key differences when running containerized:**
- `--network=pg-network`: Connects container to the same network as PostgreSQL
- `-v $(pwd)/ny_taxi_postgres_data:/data`: Mounts local data directory into container
- `--pg_host=pgdatabase`: Uses container name instead of localhost
- `--target_dataset=/data/...`: Uses the mounted path inside container

### Using with Docker Compose Network

If using the default Docker Compose network instead of an external one, the network name follows the pattern `<directory>_default`:

```bash
docker run \
  --network=exercise_1_default \
  -v $(pwd)/ny_taxi_postgres_data:/data \
  green_trips:v1 \
  --pg_user=root \
  --pg_password=root \
  --pg_host=pgdatabase \
  --pg_db=ny_taxi \
  --target_table=green_trips_v1 \
  --chunk_size=10000 \
  --target_dataset=/data/green_tripdata_2025-11.parquet
```

## Accessing the Data

1. **pgAdmin Web UI**: http://localhost:8085
   - Add server with host `pgdatabase`, port `5432`

2. **Direct connection**: `localhost:5433` (mapped from container's 5432)

## Notes

- The external network (`pg-network: external: true`) must be created before running `docker-compose up`
- Port 5433 is used on the host to avoid conflicts with local PostgreSQL installations
- Data is persisted in Docker volumes (`postgres_data`, `pgadmin_data`)

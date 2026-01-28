##native
uv run python ingest_data.py 
--pg_user=root 
--pg_password=root 
--pg_host=localhost 
--pg_db=ny_taxi 
--target_table=green_trip_data 
--chunk_size=1000 
--target_dataset=./ny_taxi_postgres_data/green_tripdata_2025-11.parquet



## containerized
docker run taxi-ingest \
  --pg_user=root \
  --pg_password=root \
  --pg_host=localhost \
  --pg_db=ny_taxi \
  --target_table=green_trip_data \
  --chunk_size=1000 \
  --network=pg-database \
  --target_dataset=./ny_taxi_postgres_data/green_tripdata_2025-11.parquet

## docker build command 
# here we have a custom image , in that case we need the dockerfile to be set ( docker file must be in the same directory )
docker build -t namecontainer:v[whatever] .

## after the container is build then

docker run 
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


## now if we want to do it with the docker compose command we just need to change the network and thats it 

# usually the network name of a compose is a 
# directoryname_default ( docker_default )
docker run 
  --network=docker_default \
  -v $(pwd)/ny_taxi_postgres_data:/data \
  green_trips:v1 \
  --pg_user=root \
  --pg_password=root \
  --pg_host=pgdatabase \
  --pg_db=ny_taxi \
  --target_table=green_trips_v1 \
  --chunk_size=10000 \
  --target_dataset=/data/green_tripdata_2025-11.parquet

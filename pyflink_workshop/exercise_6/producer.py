import pandas as pd
import json
import dataclasses
from time import time
from kafka import KafkaProducer
from dataclasses import dataclass


@dataclass
class Ride:
    PULocationID: int
    DOLocationID: int
    passenger_count: float
    trip_distance: float
    tip_amount: float
    total_amount: float
    lpep_pickup_datetime: str
    lpep_dropoff_datetime: str


def json_serializer(data):
    return json.dumps(data).encode('utf-8')


def ride_from_row(row):
    return Ride(
        PULocationID=int(row.PULocationID),
        DOLocationID=int(row.DOLocationID),
        passenger_count=float(row.passenger_count) if pd.notna(row.passenger_count) else 0.0,
        trip_distance=float(row.trip_distance) if pd.notna(row.trip_distance) else 0.0,
        tip_amount=float(row.tip_amount) if pd.notna(row.tip_amount) else 0.0,
        total_amount=float(row.total_amount) if pd.notna(row.total_amount) else 0.0,
        lpep_pickup_datetime=row.lpep_pickup_datetime.strftime('%Y-%m-%d %H:%M:%S'),
        lpep_dropoff_datetime=row.lpep_dropoff_datetime.strftime('%Y-%m-%d %H:%M:%S'),
    )


if __name__ == "__main__":

    url = "https://d37ci6vzurychx.cloudfront.net/trip-data/green_tripdata_2025-10.parquet"
    columns = ['lpep_pickup_datetime', 'lpep_dropoff_datetime', 'PULocationID', 'DOLocationID',
               'passenger_count', 'trip_distance', 'tip_amount', 'total_amount']
    df = pd.read_parquet(url, columns=columns)

    server = 'localhost:9092'
    producer = KafkaProducer(
        bootstrap_servers=[server],
        value_serializer=json_serializer)
    topic_name = 'green-trips'

    t0 = time()
    for row in df.itertuples(index=False):
        try:
            ride = ride_from_row(row)
            producer.send(topic_name, value=dataclasses.asdict(ride))
        except Exception as e:
            print(f"Skipping row due to error: {e}")

    producer.flush()
    t1 = time()
    print(f'took {(t1 - t0):.2f} seconds')

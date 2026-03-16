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
    lpep_pickup_datetime: int  # epoch milliseconds
    lpep_dropoff_datetime: int  # epoch milliseconds

def ride_serializer(ride):
    ride_dict = dataclasses.asdict(ride)
    json_str = json.dumps(ride_dict)
    return json_str.encode('utf-8')

def json_serializer(data):
    return json.dumps(data).encode('utf-8')

def ride_from_row(row):
    return Ride(
        PULocationID=int(row['PULocationID']),
        DOLocationID=int(row['DOLocationID']),
        passenger_count=float(row['passenger_count']),
        trip_distance=float(row['trip_distance']),
        tip_amount=float(row['tip_amount']),
        total_amount=float(row['total_amount']),
        lpep_pickup_datetime=int(row['lpep_pickup_datetime'].timestamp() * 1000),
        lpep_dropoff_datetime=int(row['lpep_dropoff_datetime'].timestamp() * 1000),
    )

if __name__ == "__main__":

    url = "https://d37ci6vzurychx.cloudfront.net/trip-data/green_tripdata_2025-10.parquet"
    columns = ['lpep_pickup_datetime', 'lpep_dropoff_datetime', 'PULocationID', 'DOLocationID', 'passenger_count','trip_distance','tip_amount','total_amount']
    df = pd.read_parquet(url, columns=columns)

    server = 'localhost:9092'
    producer = KafkaProducer(
        bootstrap_servers=[server],
        value_serializer=json_serializer)
    
    topic_name = 'green-trips'

    t0 = time()
    for _, row in df.iterrows():
        ride = ride_from_row(row)
        producer.send(topic_name, value=dataclasses.asdict(ride))
        print(f"Sent: {ride}")

    producer.flush()
    t1 = time()
    print(f'took {(t1 - t0):.2f} seconds')

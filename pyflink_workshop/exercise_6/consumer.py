import json
from kafka import KafkaConsumer
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


def ride_deserializer(data):
    ride_dict = json.loads(data.decode('utf-8'))
    return Ride(**ride_dict)


if __name__ == "__main__":

    server = 'localhost:9092'
    topic_name = 'green-trips'

    consumer = KafkaConsumer(
        topic_name,
        bootstrap_servers=[server],
        auto_offset_reset='earliest',
        group_id='green-trips-counter',
        value_deserializer=ride_deserializer,
        consumer_timeout_ms=10000,  # stop after 10s of no messages
    )

    print(f"Counting trips with trip_distance > 5.0 from {topic_name}...")

    count = 0
    total = 0
    for message in consumer:
        ride = message.value
        total += 1
        if ride.trip_distance > 5.0:
            count += 1

    consumer.close()
    print(f"Total messages read: {total}")
    print(f"Trips with trip_distance > 5.0: {count}")

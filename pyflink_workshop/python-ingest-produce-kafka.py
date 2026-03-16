from kafka import KafkaConsumer
from models import Ride
import psycopg2
from datetime import datetime
import json

server = 'localhost:9092'
topic_name = 'rides'


def ride_deserializer(data):
    json_str = data.decode('utf-8')
    ride_dict = json.loads(json_str)
    return Ride(**ride_dict)

if __name__ == "__main__":

    consumer = KafkaConsumer(
        topic_name,
        bootstrap_servers=[server],
        auto_offset_reset='earliest',
        group_id='rides-to-postgres',
        value_deserializer=ride_deserializer
    )


    conn = psycopg2.connect(
        host='localhost',
        port=5433,
        database='postgres',
        user='postgres',
        password='postgres'
    )
    conn.autocommit = True
    cur = conn.cursor()

    print(f"Listening to {topic_name} and writing to PostgreSQL...")

    count = 0
    for message in consumer:
        ride = message.value
        pickup_dt = datetime.fromtimestamp(ride.tpep_pickup_datetime / 1000)
        cur.execute(
            """INSERT INTO processed_events
            (PULocationID, DOLocationID, trip_distance, total_amount, pickup_datetime)
            VALUES (%s, %s, %s, %s, %s)""",
            (ride.PULocationID, ride.DOLocationID,
            ride.trip_distance, ride.total_amount, pickup_dt)
        )
        count += 1
        if count % 100 == 0:
            print(f"Inserted {count} rows...")

    consumer.close()
    cur.close()
    conn.close()
/* @bruin

name: staging.trips
type: duckdb.sql

depends:
  - ingestion.trips
  - ingestion.payment_lookup

materialization:
  type: table
  strategy: time_interval
  incremental_key: pickup_datetime
  time_granularity: timestamp

columns:
  - name: pickup_datetime
    type: timestamp
    description: "Trip pickup timestamp"
    primary_key: true
    checks:
      - name: not_null
  - name: dropoff_datetime
    type: timestamp
    description: "Trip dropoff timestamp"
    primary_key: true
    checks:
      - name: not_null
  - name: pickup_location_id
    type: integer
    description: "TLC zone ID for pickup location"
    primary_key: true
    checks:
      - name: not_null
  - name: dropoff_location_id
    type: integer
    description: "TLC zone ID for dropoff location"
    primary_key: true
    checks:
      - name: not_null
  - name: fare_amount
    type: float
    description: "Base metered fare"
    primary_key: true
  - name: total_amount
    type: float
    description: "Total charge to the passenger"
    checks:
      - name: non_negative
  - name: trip_distance
    type: float
    description: "Trip distance in miles"
    checks:
      - name: non_negative
  - name: passenger_count
    type: integer
    description: "Number of passengers"
  - name: payment_type
    type: integer
    description: "Payment type code"
  - name: payment_type_name
    type: string
    description: "Payment type description from lookup"
  - name: taxi_type
    type: string
    description: "Taxi type (yellow or green)"

custom_checks:
  - name: no_duplicates
    description: "No duplicate trips in the staging window"
    query: |
      SELECT COUNT(*) FROM (
        SELECT pickup_datetime, dropoff_datetime, pickup_location_id, dropoff_location_id, fare_amount,
               COUNT(*) AS cnt
        FROM staging.trips
        WHERE pickup_datetime >= '{{ start_datetime }}'
          AND pickup_datetime <  '{{ end_datetime }}'
        GROUP BY 1, 2, 3, 4, 5
        HAVING cnt > 1
      )
    value: 0

@bruin */

WITH deduplicated AS (
    SELECT
        pickup_datetime,
        dropoff_datetime,
        pu_location_id                AS pickup_location_id,
        do_location_id                AS dropoff_location_id,
        passenger_count,
        trip_distance,
        fare_amount,
        tip_amount,
        tolls_amount,
        total_amount,
        CAST(payment_type AS INTEGER) AS payment_type,
        taxi_type,
        extracted_at,
        ROW_NUMBER() OVER (
            PARTITION BY
                pickup_datetime,
                dropoff_datetime,
                pu_location_id,
                do_location_id,
                fare_amount
            ORDER BY extracted_at DESC
        ) AS rn
    FROM ingestion.trips
    WHERE pickup_datetime >= '{{ start_datetime }}'
      AND pickup_datetime <  '{{ end_datetime }}'
      AND pu_location_id IS NOT NULL
      AND do_location_id IS NOT NULL
)

SELECT
    d.pickup_datetime,
    d.dropoff_datetime,
    d.pickup_location_id,
    d.dropoff_location_id,
    d.passenger_count,
    d.trip_distance,
    d.fare_amount,
    d.tip_amount,
    d.tolls_amount,
    d.total_amount,
    d.payment_type,
    p.payment_type_name,
    d.taxi_type,
    d.extracted_at
FROM deduplicated d
LEFT JOIN ingestion.payment_lookup p
    ON d.payment_type = p.payment_type_id
WHERE d.rn >= 0

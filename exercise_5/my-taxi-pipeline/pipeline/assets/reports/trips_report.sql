/* @bruin

name: reports.trips_report
type: duckdb.sql

depends:
  - staging.trips

materialization:
  type: table
  strategy: time_interval
  incremental_key: pickup_date
  time_granularity: date

columns:
  - name: pickup_date
    type: date
    description: "Date of pickup (truncated from pickup_datetime)"
    primary_key: true
    checks:
      - name: not_null
  - name: taxi_type
    type: string
    description: "Taxi type (yellow or green)"
    primary_key: true
    checks:
      - name: not_null
  - name: payment_type_name
    type: string
    description: "Payment type description"
    primary_key: true
  - name: trip_count
    type: integer
    description: "Number of trips"
    checks:
      - name: positive
  - name: total_passengers
    type: integer
    description: "Total number of passengers"
    checks:
      - name: non_negative
  - name: total_distance_miles
    type: float
    description: "Total trip distance in miles"
    checks:
      - name: non_negative
  - name: total_fare_amount
    type: float
    description: "Sum of base fares"
  - name: total_tip_amount
    type: float
    description: "Sum of tip amounts"
  - name: total_amount
    type: float
    description: "Sum of total charges"
  - name: avg_trip_distance_miles
    type: float
    description: "Average trip distance in miles"
    checks:
      - name: non_negative
  - name: avg_fare_amount
    type: float
    description: "Average base fare"

custom_checks:
  - name: no_negative_trip_counts
    description: "All aggregated trip counts must be positive"
    query: |
      SELECT COUNT(*)
      FROM reports.trips_report
      WHERE pickup_date >= '{{ start_date }}'
        AND pickup_date <  '{{ end_date }}'
        AND trip_count <= 0
    value: 0

@bruin */

SELECT
    CAST(pickup_datetime AS DATE)        AS pickup_date,
    taxi_type,
    COALESCE(payment_type_name, 'unknown') AS payment_type_name,
    COUNT(*)                             AS trip_count,
    SUM(passenger_count)                 AS total_passengers,
    ROUND(SUM(trip_distance), 2)         AS total_distance_miles,
    ROUND(SUM(fare_amount), 2)           AS total_fare_amount,
    ROUND(SUM(tip_amount), 2)            AS total_tip_amount,
    ROUND(SUM(total_amount), 2)          AS total_amount,
    ROUND(AVG(trip_distance), 4)         AS avg_trip_distance_miles,
    ROUND(AVG(fare_amount), 4)           AS avg_fare_amount
FROM staging.trips
WHERE pickup_datetime >= '{{ start_datetime }}'
  AND pickup_datetime <  '{{ end_datetime }}'
GROUP BY 1, 2, 3

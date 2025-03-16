-- Step 1: Create a cleaned data temporary table
CREATE TABLE IF NOT EXISTS yellow_taxi_hive_cleaned AS
SELECT
    VendorID,
    tpep_pickup_datetime,
    tpep_dropoff_datetime,
    IF(passenger_count IS NULL, 1, passenger_count) AS passenger_count,  -- Replace NULL with 1
    IF(trip_distance IS NULL OR trip_distance < 0, 0, trip_distance) AS trip_distance,  -- Replace NULL or negative values with 0
    IF(fare_amount IS NULL OR fare_amount < 0, 0, fare_amount) AS fare_amount,  -- Replace NULL or negative values with 0
    RatecodeID,
    store_and_fwd_flag,
    PULocationID,
    DOLocationID,
    payment_type,
    fare_amount,
    extra,
    mta_tax,
    IF(tip_amount IS NULL, 0, tip_amount) AS tip_amount,  -- Replace NULL with 0
    tolls_amount,
    improvement_surcharge,
    total_amount,
    congestion_surcharge,
    Airport_fee
FROM yellow_taxi_hive
WHERE
    passenger_count > 0 AND            -- Remove invalid passenger_count
    trip_distance > 0 AND              -- Remove trips with invalid distance
    fare_amount > 0 AND                -- Remove trips with invalid fare amount
    trip_distance < 100;               -- Remove outliers (e.g., trips longer than 100 miles)

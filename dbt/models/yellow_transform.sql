--Creating new table with transformed data for yellow taxis
{{config(materialized='table')}}

-- finding the CO2 emissions for yellow taxis and converting to kgs
SELECT
    y.tpep_pickup_datetime,
    y.tpep_dropoff_datetime,
    y.passenger_count,
    y.trip_distance,
    (y.trip_distance * e.co2_grams_per_mile) / 1000.0 AS trip_co2_kgs,

-- calculating mph and extracting temporal features from pickup time
    CASE
        WHEN date_diff('second',y.tpep_pickup_datetime,y.tpep_dropoff_datetime)>0
        THEN y.trip_distance/(date_diff('second',y.tpep_pickup_datetime,y.tpep_dropoff_datetime)/3600)
        ELSE 0.000
        END AS avg_mph,
    HOUR(y.tpep_pickup_datetime) AS hour_of_day,
    DAYNAME(y.tpep_pickup_datetime) AS day_of_week,
    WEEKOFYEAR(y.tpep_pickup_datetime) AS week_of_year,
    MONTHNAME(y.tpep_pickup_datetime) AS month_of_year
FROM {{source('emissions', 'yellow_clean')}} AS y
JOIN {{source('emissions', 'vehicle_emissions')}} AS e ON e.vehicle_type='yellow_taxi'
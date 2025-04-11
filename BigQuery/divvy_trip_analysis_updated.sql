CREATE OR REPLACE TABLE `global-rookery-448215-m8.divvy_bikesdata.divvy_trip_analysis_updated` 
PARTITION BY DATE(started_at)
CLUSTER BY start_station_name, end_station_name, member_casual AS
SELECT 
    ride_id,
    started_at,
    ended_at,
    TIMESTAMP_DIFF(ended_at, started_at, MINUTE) AS ride_duration,
    EXTRACT(HOUR FROM started_at) AS start_hour,
    EXTRACT(DAYOFWEEK FROM started_at) AS start_day_number,
    FORMAT_DATE('%A', DATE(started_at)) AS start_day,
    EXTRACT(MONTH FROM started_at) AS start_month,
    FORMAT_DATE('%B', DATE(started_at)) AS start_month_name,
    CASE 
        WHEN EXTRACT(DAYOFWEEK FROM started_at) IN (1, 7) THEN 'Weekend' 
        ELSE 'Weekday' 
    END AS is_weekend,
    CASE 
        WHEN EXTRACT(MONTH FROM started_at) IN (12, 1, 2) THEN 'Winter'
        WHEN EXTRACT(MONTH FROM started_at) IN (3, 4, 5) THEN 'Spring'
        WHEN EXTRACT(MONTH FROM started_at) IN (6, 7, 8) THEN 'Summer'
        ELSE 'Fall' 
    END AS ride_season,
    start_station_id,
    start_station_name,
    end_station_id,
    end_station_name,
    start_lat,
    start_lng,
    end_lat,
    end_lng,
    ST_DISTANCE(
        ST_GEOGPOINT(start_lng, start_lat),
        ST_GEOGPOINT(end_lng, end_lat)
    ) / 1000 AS trip_distance_km,
    CASE 
        WHEN start_station_id = end_station_id THEN 1 
        ELSE 0 
    END AS same_station_trip,
    rideable_type,
    member_casual
FROM `global-rookery-448215-m8.divvy_bikesdata.external_divvy_tripdata`;

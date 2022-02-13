-- Query public available table
SELECT station_id, name FROM
    bigquery-public-data.new_york_citibike.citibike_stations
LIMIT 100;

 -- Question 1 -- 
SELECT COUNT(1) FROM 
    datatalks-magnet-338610.trips_data_all.fhv_tripdata;


-- Questio 3 -- 
SELECT COUNT(DISTINCT  dispatching_base_num)
FROM datatalks-magnet-338610.trips_data_all.fhv_tripdata;

-- Question 4 -- 
-- Note not the same answer as in the form, get 26447 rows ? -- 
SELECT COUNT(*) 
FROM datatalks-magnet-338610.trips_data_all.fhv_tripdata
WHERE DATE(pickup_datetime) BETWEEN '2019-01-01' AND '2019-03-31'
AND dispatching_base_num IN ('B00987','B02060','B02279');


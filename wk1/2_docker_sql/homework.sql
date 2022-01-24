/* Question 3*/

SELECT 
	COUNT(1)
FROM
	yellow_taxi_trips t
WHERE 
	CAST(t.tpep_pickup_datetime AS DATE) = '2021-01-15';

/* Question 4*/
SELECT
	CAST(tpep_dropoff_datetime AS DATE) as "day",
	COUNT(1),
	MAX(total_amount) AS "max_amt"
FROM
	yellow_taxi_trips t
WHERE 
	CAST(t.tpep_dropoff_datetime AS DATE) in ('2021-01-20','2021-01-04','2021-01-01','2021-01-21')
GROUP BY
	1
ORDER BY
	max_amt DESC;

/* Question 5*/
SELECT
	zpu."Zone" AS "pickup_zone",
	zdo."Zone" AS "dropoff_zone",
	COUNT(1)
FROM
	yellow_taxi_trips t,
	zones zpu,
	zones zdo
WHERE
	t."DOLocationID" = zdo."LocationID" AND
	CAST(t.tpep_pickup_datetime AS DATE) = '2021-01-14' AND
	zpu."Zone"='Central Park'
GROUP BY
	1,2
ORDER BY "count" DESC
	
/* Question 6*/
SELECT
	CONCAT(zpu."Zone", '/', zdo."Zone") AS "zone2zone",
	AVG(total_amount) AS "avg_amt"
FROM
	yellow_taxi_trips t,
	zones zpu,
	zones zdo
WHERE
	t."PULocationID" = zpu."LocationID" AND
	t."DOLocationID" = zdo."LocationID"
GROUP BY
	1
ORDER BY
	avg_amt DESC



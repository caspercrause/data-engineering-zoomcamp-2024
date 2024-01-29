-- Three biggest pick up Boroughs


SELECT SUM(gtt.total_amount) AS sum_amount, z.borough

FROM 

green_taxi_trips AS gtt,
zones AS z

WHERE 
gtt.pulocationid = z.locationid AND 
gtt.lpep_pickup_datetime::DATE = '2019-09-18'::DATE

GROUP BY z.borough

HAVING SUM(gtt.total_amount) > 50000
ORDER BY sum_amount DESC

LIMIT 5;

-- Question 6
-- For the passengers picked up in September 2019 in the zone name Astoria which was the drop off zone that had the largest tip? We want the name of the zone, not the id.

SELECT gtt.tip_amount, z.zone, z.borough, gtt.lpep_pickup_datetime::DATE

FROM 

green_taxi_trips AS gtt,
zones AS z

WHERE 
--gtt.pulocationid = z.locationid 
gtt.dolocationid = z.locationid 
AND 
EXTRACT('Year' FROM gtt.lpep_pickup_datetime ) = 2019 
AND 
EXTRACT('Month' FROM gtt.lpep_pickup_datetime ) = 9
AND
z.zone ~* 'long isl'

--GROUP BY z.zone

ORDER BY gtt.tip_amount DESC

LIMIT 15;

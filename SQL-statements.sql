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

SELECT gtt.tip_amount, z.zone as pickup, z2.zone as dropoff
FROM
green_taxi_trips AS gtt 

JOIN zones AS z
ON 
gtt.pulocationid = z.locationid

JOIN 
zones AS z2

ON 
gtt.dolocationid = z2.locationid
AND
gtt.lpep_pickup_datetime BETWEEN '2019-01-01' AND '2019-12-31'

AND z.zone ~* 'Astoria'

ORDER BY gtt.tip_amount DESC

limit 5;
-- Question 1:
-- How many taxi trips were there on January 15?
-- Consider only trips that started on January 15.

-- 1 XP

-- 0
 
-- 53024 (ans)
 
-- 534
 
-- 53027
-- Solution
select count(tpep_pickup_datetime)
from yellow_taxi_data
where EXTRACT(DAY FROM tpep_pickup_datetime) = 15
AND EXTRACT(MONTH FROM tpep_pickup_datetime) = 1

-- Question 2:
-- Largest tip for each day
-- Find the largest tip for each day. On which day it was the largest tip in January?

-- Use the pick up time for your calculations. (note: it's not a typo, it's "tip", not "trip") 1 XP
-- 2021-01-20 (ans)
-- 2021-01-04
-- 2021-01-01
-- 2021-01-21

-- Solution
select tpep_pickup_datetime, max(tip_amount) as max_tip
from yellow_taxi_data
where EXTRACT(MONTH FROM tpep_pickup_datetime) = 1
GROUP BY 1
ORDER BY max_tip DESC

-- Question 3:
-- What was the most popular destination for passengers picked up in central park on January 14?

-- Use the pick up time for your calculations.
-- Enter the zone name (not id). If the zone name is unknown (missing), write "Unknown"
-- 1 XP 
-- Upper East Side North
-- Unknown
-- Upper East Side South (ans)
-- Solution
select  table1."Dropoff_Location", count(table1."Pickup_Location")
from
(select tpep_pickup_datetime, z."Zone" as "Pickup_Location", 
x."Zone" as "Dropoff_Location"
from yellow_taxi_data y
left join zones z
ON y."PULocationID"=z."LocationID"
left join zones x
ON y."DOLocationID"=x."LocationID"
where EXTRACT(DAY FROM tpep_pickup_datetime) = 14
AND EXTRACT(MONTH FROM tpep_pickup_datetime) = 1
AND z."Zone" = 'Central Park') as table1
GROUP BY 1
order by 2 DESC

-- Question 4:
-- What's the pickup-dropoff pair with the largest average price for a ride (calculated based on total_amount)?

-- Enter two zone names separated by a slash For example:
-- "Jamaica Bay / Clinton East"
-- If any of the zone names are unknown (missing), write "Unknown". For example, "Unknown / Clinton East". 1 XP
-- Union Sq/Canarsie

-- Borough Park/NV 

-- Alphabet City/Unknown (ans)

-- Central Park/Upper East Side North

select zonepair, sum(total_amount)/count(total_amount) as average_price from 
(select CASE
WHEN (pickup_loc IS NOT NULL) and (dropoff_loc IS NOT NULL) THEN CONCAT(pickup_loc, ' / ', dropoff_loc)
WHEN (pickup_loc IS NULL) and (dropoff_loc IS NOT NULL) THEN CONCAT('Unknown', ' / ', dropoff_loc)
WHEN (pickup_loc IS NOT NULL) and (dropoff_loc IS NULL) THEN CONCAT(pickup_loc, '  /', 'Unknown')
WHEN (pickup_loc IS NULL) and (dropoff_loc IS NULL) THEN CONCAT('Unknown', ' / ', 'Unknown')
END AS zonepair, t1."total_amount"
from(select z."Zone" as "pickup_loc", x."Zone" as "dropoff_loc", total_amount
from yellow_taxi_data y
left join zones z
ON y."PULocationID" = z."LocationID"
left join zones x
ON y."DOLocationID" = x."LocationID") as t1) as t2
GROUP BY 1
ORDER BY 2 DESC

-- SOLUTION 2 to QUESTION 4
select zonepair, sum(total_amount)/count(total_amount) as average_price from 
(select z."Zone" as "pickup_loc", x."Zone" as "dropoff_loc", CASE
WHEN (z."Zone" IS NOT NULL) and (x."Zone" IS NOT NULL) THEN CONCAT(z."Zone", ' / ', x."Zone")
WHEN (z."Zone" IS NULL) and (x."Zone" IS NOT NULL) THEN CONCAT('Unknown', ' / ', x."Zone")
WHEN (z."Zone" IS NOT NULL) and (x."Zone" IS NULL) THEN CONCAT(z."Zone", '  /', 'Unknown')
WHEN (z."Zone" IS NULL) and (x."Zone" IS NULL) THEN CONCAT('Unknown', ' / ', 'Unknown')
END AS zonepair,total_amount
from yellow_taxi_data y
left join zones z
ON y."PULocationID" = z."LocationID"
left join zones x
ON y."DOLocationID" = x."LocationID") as t1
GROUP BY 1
ORDER BY 2 DESC


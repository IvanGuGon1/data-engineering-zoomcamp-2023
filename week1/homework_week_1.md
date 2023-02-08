# -data-engineering-zoomcamp-homework1

## Question 1. Knowing docker tags
B) --iidfile string
- commands: ```docker build --help ```


## Question 2. Understanding docker first run
C) 3
- commands: ``` docker run -it python:3.9 bash ```


## Question 3. Count records
B) 20530
- commands: ``` SELECT COUNT(1)
FROM green_taxi_data 
WHERE
CAST(lpep_pickup_datetime AS DATE) = '2019-01-15' 
AND 
CAST(lpep_dropoff_datetime AS DATE) = '2019-01-15' ```

## Question 4. Largest trip for each day
C) 2019-01-15
- commands: ``` SELECT *
FROM green_taxi_data ORDER BY trip_distance DESC LIMIT 1 ```

## Question 5. The number of passengers
C) 2: 1282 ; 3: 254
- commands:
``` SELECT COUNT(1) FROM green_taxi_data WHERE passenger_count = 2 AND CAST(lpep_pickup_datetime AS DATE) = '2019-01-01' ```
``` SELECT COUNT(1) FROM green_taxi_data WHERE passenger_count = 3 AND CAST(lpep_pickup_datetime AS DATE) = '2019-01-01' ```

## Question 6. Largest tip
D) Long Island City/Queens Plaza
- commands: ``` SELECT green_taxi_data.tip_amount, PickupZonez."Zone" as PickupZone, DropoffZonez."Zone" as DropoffZone
FROM green_taxi_data 
LEFT JOIN zones as PickupZonez
ON green_taxi_data."PULocationID" = PickupZonez."LocationID"
LEFT JOIN zones as DropoffZonez
ON green_taxi_data."DOLocationID" = DropoffZonez."LocationID"
WHERE PickupZonez."Zone" = 'Astoria'
ORDER BY tip_amount DESC LIMIT 1' ```
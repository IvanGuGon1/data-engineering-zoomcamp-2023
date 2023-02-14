# Week 3 Homework

## Question 1:

B ) 43,244,696

```
CREATE OR REPLACE EXTERNAL TABLE `dtc-de-course-376222.nytaxiEU.fhv_tripdata`
OPTIONS (
  format = 'CSV',
  compression = 'GZIP',
  uris = ['gs://prefect-zoomcamp-ivang/data/week3_exercises/fhv_tripdata_2019-*.csv.gz']
);

SELECT count(*) FROM `taxi-rides-ny.nytaxiEU.fhv_tripdata`;

```

## Question 2:

D) 0 MB for the External Table and 317.94MB for the BQ Table
```
CREATE OR REPLACE TABLE `dtc-de-course-376222.nytaxiEU.external_fhv_tripdata` AS
SELECT * FROM dtc-de-course-376222.nytaxiEU.fhv_tripdata;

SELECT COUNT(DISTINCT(affiliated_base_number )) FROM `dtc-de-course-376222.nytaxiEU.fhv_tripdata`;
SELECT  COUNT(DISTINCT(affiliated_base_number )) FROM `dtc-de-course-376222.nytaxiEU.external_fhv_tripdata`;
```

## Question 3:

A) 717,748

```
SELECT COUNT(*) FROM `dtc-de-course-376222.nytaxiEU.fhv_tripdata` WHERE PUlocationID IS NULL AND DOlocationID IS NULL
```

## Question 4:

B ) Partition by pickup_datetime Cluster on affiliated_base_number

```
CREATE OR REPLACE TABLE `dtc-de-course-376222.nytaxiEU.fhv_tripdata_partitoned_clustered`
PARTITION BY DATE(pickup_datetime)
CLUSTER BY affiliated_base_number AS
SELECT * FROM `dtc-de-course-376222.nytaxiEU.fhv_tripdata`;

SELECT DISTINCT(affiliated_base_number ) FROM `dtc-de-course-376222.nytaxiEU.fhv_tripdata_partitoned_clustered` WHERE DATE(pickup_datetime ) BETWEEN '2019-03-01' and '2019-03-31'

SELECT DISTINCT(affiliated_base_number ) FROM `dtc-de-course-376222.nytaxiEU.external_fhv_tripdata` WHERE DATE(pickup_datetime ) BETWEEN '2019-03-01' and '2019-03-31'
```

## Question 5:

B) 647.87 MB for non-partitioned table and 23.06 MB for the partitioned table
```
CREATE OR REPLACE TABLE taxi-rides-ny.nytaxi.yellow_tripdata_partitoned_clustered
PARTITION BY DATE(tpep_pickup_datetime)
CLUSTER BY VendorID AS
SELECT * FROM taxi-rides-ny.nytaxi.external_yellow_tripdata;

SELECT DISTINCT(affiliated_base_number ) FROM `dtc-de-course-376222.nytaxiEU.fhv_tripdata_partitoned_clustered` WHERE DATE(pickup_datetime ) BETWEEN '2019-03-01' and '2019-03-31'

SELECT DISTINCT(affiliated_base_number ) FROM `dtc-de-course-376222.nytaxiEU.external_fhv_tripdata` WHERE DATE(pickup_datetime ) BETWEEN '2019-03-01' and '2019-03-31'
``` 

## Question 6:

B) GCP Bucket

## Question 7:

B) False




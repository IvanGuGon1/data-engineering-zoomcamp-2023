CREATE OR REPLACE EXTERNAL TABLE `dtc-de-course-376222.nytaxi.fhv_tripdata`
OPTIONS (
  format = 'CSV',
  compression = 'GZIP',
  uris = ['gs://prefect-zoomcamp-ivang/data/week3_exercises/fhv_tripdata_2019-*.csv.gz']
);
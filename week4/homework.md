
### Question 1: 

**What is the count of records in the model fact_trips after running all models with the test run variable disabled and filtering for 2019 and 2020 data only (pickup datetime)?** 

 - C) 61648442

My result is 61540561

```
SELECT count(*) FROM `dtc-de-course-376222.dbt_igutierrez.fact_trips` WHERE EXTRACT(YEAR from pickup_datetime)=2019 or EXTRACT(YEAR from pickup_datetime)=2020
```

### Question 2: 

**What is the distribution between service type filtering by years 2019 and 2020 data as done in the videos?**

- D) 99.1/0.9

My result is 98.75 and 1.24

### Question 3: 

**What is the count of records in the model stg_fhv_tripdata after running all models with the test run variable disabled (:false)?**  

- A) 33244696

My result is 37187000

```
SELECT count(*) FROM `dtc-de-course-376222.dbt_igutierrez.stg_fhv_tripdata` 
```

### Question 4: 

**What is the count of records in the model fact_fhv_trips after running all dependencies with the test run variable disabled (:false)?**  

- B) 22998722

My result is 17907149

### Question 5: 

**What is the month with the biggest amount of rides after building a tile for the fact_fhv_trips table?**

Create a dashboard with some tiles that you find interesting to explore the data. One tile should show the amount of trips per month, as done in the videos for fact_trips, based on the fact_fhv_trips table.

C) January

# Week 2 Homework

## Question 1. Load January 2020 data

A) 447770

```
python etl_web_to_gcs.py

```

## Question 2. Scheduling with Cron

A) 0 5 1 * *

```
prefect deployment build ./etl_web_to_gcs.py:etl_web_to_gcs -n "ETL Deploy for Homework Exercise 2" --cron "0 5 1 * *"
```

## Question 3. Loading data to BigQuery

A) 14,851,920

```
    prefect deployment build ./etl_gcs_to_bq.py:etl_parent_flow_homework -n "Exercise3gcs_to_bq"
    prefect deployment apply etl_parent_flow_homework-deployment.yaml 
    prefect deployment run etl-parent-flow-homework/Exercise3gcs_to_bq --param months=[1,3] --param year=2019
```


## Question 4. Github Storage Block

C ) 88,605

```
prefect deployment build week2/question4/etl_web_to_gcs_question4.py:etl_web_to_gcs --name "Question4_Github" --tag github-block -sb github/github-block  -a
prefect deployment apply etl_web_to_gcs-deployment.yaml
prefect deployment run etl-web-to-gcs/Question4_Github
```

## Question 5. Email or Slack notifications

D) 514,392

```
prefect deployment build ./etl_web_to_gcs_question5.py:etl_web_to_gcs -n "Question 5 notification"
prefect deployment apply etl_parent_flow_homework-deployment.yaml 
prefect deployment run "etl-web-to-gcs/Question 5 notification" 

```

## Question 6. Secrets

C) 8



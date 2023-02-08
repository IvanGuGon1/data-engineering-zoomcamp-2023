prefect deployment build ./etl_gcs_to_bq.py:etl_parent_flow_homework -n "Parametrized ETL Homework gcs to bq"
prefect deployment apply etl_parent_flow_homework-deployment.yaml


EJercicio 2)
A) 0 5 1 * *

- Cron. Minutos, Horas, Dias del Mes, Mes, Dia de la semana (1-Lunes, 2-Martes, etc....)

- prefect deployment build ./etl_web_to_gcs.py:etl_web_to_gcs -n "ETL Deploy for Homework Exercise 2" --cron 
"0 5 1 * *"


3) 
A) 14,851,920



prefect deployment build ./etl_gcs_to_bq.py:etl_parent_flow_homework -n "exercise-3-c" 
 prefect deployment apply etl_parent_flow_homework-deployment.yaml 
 prefect deployment run etl-parent-flow-homework/exercise-3-c --param months=[2,3] --param year=2019 --param color=yellow


- Cargamos el mes 1 y 3 de 2019
 prefect deployment run "etl-parent-flow/Parameterized ETL" --param months=[1,3] --param year=2019

- Usamos los parametros indicados en el ejercicios.
 prefect deployment run etl-parent-flow-homework/exercise-3 --param months=[1,3] --param year=2019

     prefect deployment build ./etl_gcs_to_bq.py:etl_parent_flow_homework -n "Exercise3gcs_to_bq"
    prefect deployment apply etl_parent_flow_homework-deployment.yaml 
    prefect deployment run etl-parent-flow-homework/Exercise3gcs_to_bq --param months=[1,3] --param year=2019


SELECT CAST(tpep_pickup_datetime AS DATE) FROM `dtc-de-course-376222.homework2.rides` WHERE tpep_pickup_datetime='2019' LIMIT 10


SELECT CAST(EXTRACT(YEAR from tpep_pickup_datetime) as string) FROM `dtc-de-course-376222.homework2.rides` LIMIT 10
SELECT CAST(EXTRACT(YEAR from tpep_pickup_datetime) as string),count(*) FROM `dtc-de-course-376222.homework2.exercise3` WHERE CAST(EXTRACT(YEAR from tpep_pickup_datetime) as string)='2019' 


4)

prefect deployment build -n gh -q prod -sb github/github-block --apply week2/question4/etl_web_to_gcs_question4py:etl_web_to_gcs

prefect deployment build week2/question4/etl_web_to_gcs_question4py:etl_web_to_gcs --name "Question4_Github" --tag github-block -sb github/github-block  -a


prefect deployment build path/to/flow.py:flow_name --name deployment_name --tag dev -sb github/dev -a

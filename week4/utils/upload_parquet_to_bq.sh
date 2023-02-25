array=(01 02 03 04 05 06 07 08 09 10 11 12)
for i in "${array[@]}"
do

    bq load --source_format=PARQUET trips_data_all.green green_tripdata_2019-$i.parquet
done

for i in "${array[@]}"
do

    bq load --source_format=PARQUET trips_data_all.green green_tripdata_2020-$i.parquet
done

for i in "${array[@]}"
do

    bq load --source_format=PARQUET trips_data_all.yellow yellow_tripdata_2019-$i.parquet
done

for i in "${array[@]}"
do

    bq load --source_format=PARQUET trips_data_all.yellow yellow_tripdata_2020-$i.parquet
done
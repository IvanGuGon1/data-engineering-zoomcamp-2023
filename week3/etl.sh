array=(01 02 03 04 05 06 07 08 09 10 11 12)
dataset_file = "fhv_tripdata_2019-"
for i in "${array[@]}"
do
    wget -c "https://github.com/DataTalksClub/nyc-tlc-data/releases/download/fhv/fhv_tripdata_2019-"$i".csv.gz"
    gsutil cp "fhv_tripdata_2019-"$i".csv.gz" gs://prefect-zoomcamp-ivang/data/week3_exercises

done
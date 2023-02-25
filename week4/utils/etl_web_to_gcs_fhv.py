from pathlib import Path
import pandas as pd
from prefect import flow, task
from prefect_gcp.cloud_storage import GcsBucket
from random import randint
import requests
import urllib.request
import os

@task(retries=3)
def fetch(dataset_url: str) -> str:
    """Read taxi data from web into pandas DataFrame"""
    # if randint(0, 1) > 0:
    #     raise Exception
    #r = requests.get(dataset_url)
    #urllib.request.urlretrieve(dataset_url, '/home/ivang/datatalks/data-engineering-zoomcamp/homework/data-engineering-zoomcamp-2023/week3/data/week3_exercises/' + dataset_url.split("/")[-1])
    #df = pd.read_csv(dataset_url)
    path = '/home/ivang/datatalks/data-engineering-zoomcamp/homework/data-engineering-zoomcamp-2023/week4/data/fhv/'
    #dataset_url = 'https://github.com/DataTalksClub/nyc-tlc-data/releases/download/fhv/fhv_tripdata_2019-01.csv.gz'
    r = urllib.request.urlretrieve(dataset_url, path + dataset_url.split("/")[-1])
    print(r[0])

@task(retries=3)
def parquet_transform(path: Path) -> str:
    df = pd.read_csv(path)
    file_name = file_name.replace('.csv', '.parquet')
    try:
        df.to_parquet(file_name, engine='pyarrow')
    except Exception as e:
        print("Excepcion transformando en parquet {}".format(e))

    print(f"Parquet: {file_name}")
    return file_name

@task(timeout_seconds=10000)
def write_gcs(path: Path,dataset_file:str) -> None:
    """Upload local parquet file to GCS"""
    print(f"The path is : {path}")
    system_path = '/home/ivang/datatalks/data-engineering-zoomcamp/homework/data-engineering-zoomcamp-2023/week4/data/fhv/' + dataset_file 
    print(f"system path {system_path}")
    gcs_block = GcsBucket.load("zoomcamp-data")
    gcs_block.upload_from_path(from_path=f"{system_path}", to_path='/data/fhv/', timeout=1000)
    return None


@flow()
def etl_web_to_gcs(year: int, month: int) -> None:
    """The main ETL function"""
    #color = "yellow"
    
    dataset_file = f"fhv_tripdata_{year}-{month:02}"
    path = "/data/fhv/"
    # https://github.com/DataTalksClub/nyc-tlc-data/releases/download/fhv/fhv_tripdata_2019-01.csv.gz
    dataset_url = f"https://github.com/DataTalksClub/nyc-tlc-data/releases/download/fhv/{dataset_file}.csv.gz"
    

    fetch(dataset_url)
    path_parquet = parquet_transform(path + dataset_file)
    write_gcs(path_parquet,dataset_url.split("/")[-1])

@flow(timeout_seconds=10000)
def etl_parent_flow_week4_exercise(
    months: list[int] = [1, 2], year: int = 2019,
):
    for month in months:
        etl_web_to_gcs(year, month)
        print(f"Loaded month {month} , {year}")


if __name__ == "__main__":
    year = 2019
    months = [1,2,3,4,5,6,7,8,9,10,11,12]
    etl_parent_flow_week4_exercise(months,year)

from pathlib import Path
import pandas as pd
from prefect import flow, task
from prefect_gcp.cloud_storage import GcsBucket
from prefect_gcp import GcpCredentials


@task(retries=3,log_prints=True)
def extract_from_system(color: str, year: int, month: int) -> pd.DataFrame:
    """Download trip data from GCS"""
    '''
    gcs_path = f"data/{color}/{color}_tripdata_{year}-{month:02}.parquet"
    gcs_block = GcsBucket.load("prefect-zoomcamp-ivang-2")
    gcs_block.get_directory(from_path=gcs_path, local_path=f"../data/")
    path = Path(f"../data/{gcs_path}")
    '''

    name_file= f"{color}_tripdata_{year}-{month:02}.parquet"
    df = pd.read_parquet(name_file)
    return df

'''
@task()
def transform(path: Path) -> pd.DataFrame:
    """Data cleaning example"""
    df = pd.read_parquet(path)
    print(f"pre: missing passenger count: {df['passenger_count'].isna().sum()}")
    df["passenger_count"].fillna(0, inplace=True)
    print(f"post: missing passenger count: {df['passenger_count'].isna().sum()}")
    return df
'''

@task(log_prints=True)
def write_bq(df: pd.DataFrame) -> None:
    """Write DataFrame to BiqQuery"""

    gcp_credentials_block = GcpCredentials.load("zoom-gcp-creds")

    df.to_gbq(
        #destination_table="testdataset.rides,
        destination_table="prueba.prueba",
        project_id="dtc-de-course-376222",
        credentials=gcp_credentials_block.get_credentials_from_service_account(),
        chunksize=500_000,
        if_exists="append",
    )


@flow(log_prints=True)
def etl_local_to_bq(color:str,year:int,month:int) -> pd.DataFrame:
    """Main ETL flow to load data into Big Query"""

    df = extract_from_system(color, year, month)
    
    #df = transform(path)
    write_bq(df)
    return df

@flow(log_prints=True)
def etl_parent_flow_local_to_gcs(
    color: str = "yellow", year: int = 2021, months: list[int] = [2, 3]
):
    rows_count = []
    for month in months:
        df = etl_local_to_bq(color, year, month)
        rows_count.append(len(df))
        print(f"Loaded month {month} , year {year} , color {color}, rows {len(df)} ")

    print(f"Total rows: {sum(rows_count)}")

if __name__ == "__main__":
    color = "yellow"
    year = 2019
    months = [2, 3]
    etl_parent_flow_local_to_gcs(color,year,months)
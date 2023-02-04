from pathlib import Path
import pandas as pd
from prefect import flow, task
from prefect_gcp.cloud_storage import GcsBucket
from prefect_gcp import GcpCredentials


@task(retries=3)
def extract_from_gcs(color: str, year: int, month: int) -> Path:
    """Download trip data from GCS"""
    gcs_path = f"data/{color}/{color}_tripdata_{year}-{month:02}.parquet"
    gcs_block = GcsBucket.load("bucket-majestic-poetry-375216")
    gcs_block.get_directory(from_path=gcs_path, local_path=f"data/")
    return Path(f"data/{gcs_path}")


@task()
def transform(path: Path) -> pd.DataFrame:
    """Data cleaning example"""
    df = pd.read_parquet(path)
    # df["passenger_count"].fillna(0, inplace=True)
    return df


@task()
def write_bq(df: pd.DataFrame) -> None:
    """Write DataFrame to BiqQuery"""

    gcp_credentials_block = GcpCredentials.load("majestic-poetry")

    df.to_gbq(
        destination_table="dezoomcamp.rides",
        project_id="majestic-poetry-375216",
        credentials=gcp_credentials_block.get_credentials_from_service_account(),
        chunksize=500_000,
        if_exists="append",
    )


@flow(log_prints=True)
def etl_gcs_to_bq(    
    month: int = 1, year: int = 2019, color: str = "yellow"
):
    """Main ETL flow to load data into Big Query"""
    path = extract_from_gcs(color, year, month)
    df = transform(path)
    write_bq(df)
    print(f"rows: {len(df)}")


if __name__ == "__main__":
    etl_gcs_to_bq()

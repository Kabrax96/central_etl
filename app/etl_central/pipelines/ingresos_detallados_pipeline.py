import os
import re
from dotenv import load_dotenv
import pandas as pd
from sqlalchemy import MetaData
import boto3

from app.etl_central.connectors.postgresql import PostgreSqlClient
from app.etl_central.assets.ingresos_detallado import (
    get_ingresos_detallado_table,
    generate_surrogate_key,
    extract_ingresos_detallado_data,
    transform_ingresos_detallado_data,
    single_load
)
from app.etl_central.assets.pipeline_logging import PipelineLogging
from app.etl_central.assets.metadata_logging import MetaDataLogging, MetaDataLoggingStatus

def find_latest_ingresos_file():
    """
    Searches for the most recent 'ingresos detallado' Excel file in S3.
    Returns a tuple (year, quarter) based on the latest file detected.
    """
    bucket_name = "centralfiles3"
    prefix = "finanzas/Ingresos_Detallado/raw/"
    s3 = boto3.client("s3")
    pattern = r"F5_Edo_Ana_Ing_Det_LDF_([1-4]T)(\d{4})\.xlsx"
    quarter_map = {"1T": "Q1", "2T": "Q2", "3T": "Q3", "4T": "Q4"}

    latest_year = -1
    latest_quarter = -1
    latest_q = None
    latest_y = None

    paginator = s3.get_paginator("list_objects_v2")
    for page in paginator.paginate(Bucket=bucket_name, Prefix=prefix):
        for obj in page.get("Contents", []):
            fname = obj["Key"].split("/")[-1]
            match = re.match(pattern, fname)
            if match:
                quarter_str = match.group(1)
                quarter = int(quarter_str[0])  # 1T → 1, etc.
                year = int(match.group(2))
                if (year > latest_year) or (year == latest_year and quarter > latest_quarter):
                    latest_year = year
                    latest_quarter = quarter
                    latest_q = quarter_map[quarter_str]
                    latest_y = year

    return (latest_y, latest_q) if latest_y and latest_q else (None, None)


def pipeline(pipeline_logging: PipelineLogging):
    pipeline_logging.logger.info("Starting single pipeline run")

    # Load DB credentials
    SERVER_NAME = os.getenv("SERVER_NAME")
    DATABASE_NAME = os.getenv("DATABASE_NAME")
    DB_USERNAME = os.getenv("DB_USERNAME")
    DB_PASSWORD = os.getenv("DB_PASSWORD")
    PORT = os.getenv("PORT")
    BUCKET_NAME = os.getenv("BUCKET_NAME")

    # Detect most recent file
    year, quarter = find_latest_ingresos_file(bucket_name=BUCKET_NAME)
    pipeline_logging.logger.info(f"100 | Processing year={year} quarter={quarter}")

    df_raw, file_path = extract_ingresos_detallado_data(
        year, quarter, source="s3", bucket_name=BUCKET_NAME
    )

    if df_raw.empty:
        raise ValueError(f"400 | File {file_path} is empty or could not be read.")

    df_clean = transform_ingresos_detallado_data(df_raw, file_path)
    df_clean = generate_surrogate_key(df_clean)

    # DB Connection
    postgresql_client = PostgreSqlClient(
        server_name=SERVER_NAME,
        database_name=DATABASE_NAME,
        username=DB_USERNAME,
        password=DB_PASSWORD,
        port=PORT,
    )
    metadata = MetaData()
    table = get_ingresos_detallado_table(metadata)

    # Load to DB (upsert)
    single_load(
        df=df_clean,
        postgresql_client=postgresql_client,
        table=table,
        metadata=metadata,
    )

    pipeline_logging.logger.info("Pipeline run successful")

def run_pipeline(pipeline_name: str, log_client: PostgreSqlClient):
    pipeline_logging = PipelineLogging(
        pipeline_name=pipeline_name,
        log_folder_path="./logs"
    )
    metadata_logger = MetaDataLogging(
        pipeline_name=pipeline_name,
        postgresql_client=log_client,
        config={}
    )
    try:
        metadata_logger.log()  # log start
        pipeline(pipeline_logging=pipeline_logging)
        metadata_logger.log(
            status=MetaDataLoggingStatus.RUN_SUCCESS,
            logs=pipeline_logging.get_logs()
        )  # log end
        pipeline_logging.logger.handlers.clear()
    except Exception as e:
        pipeline_logging.logger.error(f"Pipeline run failed. See detailed logs: {e}")
        metadata_logger.log(
            status=MetaDataLoggingStatus.RUN_FAILURE,
            logs=pipeline_logging.get_logs()
        )  # log error
        pipeline_logging.logger.handlers.clear()

if __name__ == "__main__":
    load_dotenv()
    LOGGING_SERVER_NAME = os.getenv("LOGGING_SERVER_NAME")
    LOGGING_DATABASE_NAME = os.getenv("LOGGING_DATABASE_NAME")
    LOGGING_USERNAME = os.getenv("LOGGING_USERNAME")
    LOGGING_PASSWORD = os.getenv("LOGGING_PASSWORD")
    LOGGING_PORT = os.getenv("LOGGING_PORT")

    log_client = PostgreSqlClient(
        server_name=LOGGING_SERVER_NAME,
        database_name=LOGGING_DATABASE_NAME,
        username=LOGGING_USERNAME,
        password=LOGGING_PASSWORD,
        port=LOGGING_PORT,
    )

    PIPELINE_NAME = "ingresos_detallado_pipeline"

    run_pipeline(
        pipeline_name=PIPELINE_NAME,
        log_client=log_client
    )

import os
import re
import pandas as pd
import boto3
from dotenv import load_dotenv
from sqlalchemy import MetaData

from app.etl_central.connectors.postgresql import PostgreSqlClient
from app.etl_central.assets.egresos_detallado import (
    get_egresos_detallado_table,
    generate_surrogate_key,
    extract_egresos_detallado_data,
    transform_egresos_detallado_data,
    bulk_load,
)
from app.etl_central.assets.pipeline_logging import PipelineLogging
from app.etl_central.assets.metadata_logging import MetaDataLogging, MetaDataLoggingStatus

def find_all_egresos_files(bucket_name="centralfiles3", prefix="finanzas/Egresos_Detallado/raw/"):
    """
    Detect all valid egresos detallado Excel files in the S3 bucket.
    Returns a list of (year, quarter) tuples for each file found.
    """
    s3 = boto3.client("s3")
    pattern = r"F6_a_EAPED_Clas_Obj_Gas_LDF_([1-4]T)(\d{4})\.xlsx"

    quarter_map = {"1T": "Q1", "2T": "Q2", "3T": "Q3", "4T": "Q4"}
    detected = set()

    paginator = s3.get_paginator("list_objects_v2")
    for page in paginator.paginate(Bucket=bucket_name, Prefix=prefix):
        for obj in page.get("Contents", []):
            key = obj["Key"].split("/")[-1]
            match = re.match(pattern, key)
            if match:
                qt = quarter_map[match.group(1)]
                yr = int(match.group(2))
                detected.add((yr, qt))
    sorted_detected = sorted(detected)
    print(f"🔍 Archivos detectados en S3: {sorted_detected}")  # Debugging log
    return sorted_detected

def pipeline(pipeline_logging: PipelineLogging):
    pipeline_logging.logger.info("Starting dynamic bulk pipeline run")

    # Load DB credentials
    SERVER_NAME = os.getenv("SERVER_NAME")
    DATABASE_NAME = os.getenv("DATABASE_NAME")
    DB_USERNAME = os.getenv("DB_USERNAME")
    DB_PASSWORD = os.getenv("DB_PASSWORD")
    PORT = os.getenv("PORT")
    BUCKET_NAME = os.getenv("BUCKET_NAME")

    # Detect available files dynamically
    file_keys = find_all_egresos_files(bucket_name=BUCKET_NAME)
    if not file_keys:
        raise FileNotFoundError("No valid .xlsx files found for bulk processing.")

    transformed_dfs = []
    for year, quarter in file_keys:
        pipeline_logging.logger.info(f"100 | Processing year={year} quarter={quarter}")
        df_raw, file_path = extract_egresos_detallado_data(
            year, quarter, source="s3", bucket_name=BUCKET_NAME
        )

        if df_raw.empty:
            pipeline_logging.logger.warning(f"400 | File {file_path} is empty or could not be read.")
            continue

        df_clean = transform_egresos_detallado_data(df_raw, file_path)
        df_clean = generate_surrogate_key(df_clean)
        transformed_dfs.append(df_clean)

    if not transformed_dfs:
        raise ValueError("No data was extracted and transformed from any of the files.")

    # Concatenate and prepare final DataFrame
    final_df = pd.concat(transformed_dfs, ignore_index=True)

    # Prepare DB connection and table
    postgresql_client = PostgreSqlClient(
        server_name=SERVER_NAME,
        database_name=DATABASE_NAME,
        username=DB_USERNAME,
        password=DB_PASSWORD,
        port=PORT,
    )
    metadata = MetaData()
    table = get_egresos_detallado_table(metadata)

    # Load to DB (truncating before insert)
    bulk_load(
        df=final_df,
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

    PIPELINE_NAME = "egresos_detallado_bulk_pipeline"

    run_pipeline(
        pipeline_name=PIPELINE_NAME,
        log_client=log_client
    )

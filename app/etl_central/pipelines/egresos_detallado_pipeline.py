import os
import re
import time
import pandas as pd
import boto3
from pathlib import Path
from dotenv import load_dotenv
from sqlalchemy import MetaData
from app.etl_central.assets.ingresos_detallado import (
    get_ingresos_detallado_table,
    generate_surrogate_key,
    extract_ingresos_detallado_data,
    transform_ingresos_detallado_data
)
from app.etl_central.assets.pipeline_logging import PipelineLogging
from app.etl_central.connectors.postgresql import PostgreSqlClient


def find_latest_ingresos_file():

    bucket_name = "centralfiles3"
    prefix = "finanzas/Ingresos_Detallado/raw/"
    s3 = boto3.client("s3")
    pattern = r"F6_a_EAPED_Clas_Obj_Gas_LDF_([1-4]T)(\d{4})\.xlsx"
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
                quarter = int(quarter_str[0])
                year = int(match.group(2))
                if (year > latest_year) or (year == latest_year and quarter > latest_quarter):
                    latest_year = year
                    latest_quarter = quarter
                    latest_q = quarter_map[quarter_str]
                    latest_y = year

    if latest_q and latest_y:
        return latest_y, latest_q
    else:
        return None, None


if __name__ == "__main__":
    # Load environment variables
    load_dotenv()

    # Initialize logging with proper folder and pipeline name
    pipeline_logging = PipelineLogging(
        pipeline_name="ingresos_detallado_pipeline",
        log_folder_path="./logs"
    )
    pipeline_logging.logger.info("100 | Starting ETL pipeline for Ingresos Detallado")

    # Load environment variables
    DB_USERNAME = os.getenv("DB_USERNAME")
    DB_PASSWORD = os.getenv("DB_PASSWORD")
    SERVER_NAME = os.getenv("SERVER_NAME")
    DATABASE_NAME = os.getenv("DATABASE_NAME")
    PORT = os.getenv("PORT", 5432)

    try:
        # PostgreSQL Connection
        pipeline_logging.logger.info("100 | Initializing PostgreSQL client")
        postgresql_client = PostgreSqlClient(
        server_name=SERVER_NAME,
        database_name=DATABASE_NAME,
        username=DB_USERNAME,
        password=DB_PASSWORD,
        port=PORT,
)
        pipeline_logging.logger.info("200 | PostgreSQL client initialized successfully")

        year, quarter = find_latest_ingresos_file()
        if not year or not quarter:
            raise FileNotFoundError("No valid ingresos file found in the S3 bucket.")

        print(f"File detected: year = {year}, quarter= {quarter}")

        # Find the most recent file
        pipeline_logging.logger.info(f"100 | Extracting data for year {year} and quarter {quarter}")
        extracted_df, file_path = extract_ingresos_detallado_data(
            year=year,
            quarter=quarter,
            source="s3",
            bucket_name= "centralfiles3"
        )
        pipeline_logging.logger.info("200 | Data extraction completed")

        print(f"🧪 Archivo extraído: {file_path}")
        print(f"🧪 Filas extraídas: {extracted_df.shape[0]}")

        pipeline_logging.logger.info("100 | Transforming data")
        transformed_df = transform_ingresos_detallado_data(extracted_df, file_path)
        transformed_df = generate_surrogate_key(transformed_df)
        pipeline_logging.logger.info("200 | Data transformation completed")

        metadata = MetaData()
        table = get_ingresos_detallado_table(metadata)
        pipeline_logging.logger.info("100 | Preparing PostgreSQL table schema")

        pipeline_logging.logger.info("100 | Loading data into PostgreSQL")
        postgresql_client.upsert(
            data=transformed_df.to_dict(orient='records'),
            table=table,
            metadata=metadata
        )
        pipeline_logging.logger.info("200 | Data successfully loaded into PostgreSQL")
        pipeline_logging.logger.info("200 | ETL pipeline completed successfully")

    except Exception as e:
        pipeline_logging.logger.error(f"500 | ETL pipeline failed: {e}")
        raise

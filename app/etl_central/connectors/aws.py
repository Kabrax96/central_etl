# app/etl_central/connectors/aws.py

import pandas as pd
import logging

def read_excel_from_s3(bucket: str, key: str, sheet_name: str = "F4 BAP") -> pd.DataFrame:
    """
    Reads an Excel file from S3 using s3fs backend.

    Args:
        bucket (str): S3 bucket name
        key (str): S3 object key (path inside bucket)
        sheet_name (str): Excel sheet to read

    Returns:
        pd.DataFrame
    """
    try:
        s3_path = f"s3://{bucket}/{key}"
        return pd.read_excel(s3_path, sheet_name=sheet_name, header=None, engine="openpyxl")
    except Exception as e:
        logging.error(f"Failed to read {key} from bucket {bucket}: {e}")
        return pd.DataFrame()

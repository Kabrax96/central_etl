import os
import re
import pandas as pd
import hashlib
import boto3
from sqlalchemy import MetaData, Table
from app.etl_central.connectors.postgresql import PostgreSqlClient
import logging
from app.etl_central.assets.transform_utils import (
    parse_fecha_header,
    extraer_codigo_y_sublabel,
    clean_amount
)
from app.etl_central.connectors.aws import read_excel_from_s3
import boto3
from io import BytesIO
from sqlalchemy import text
from sqlalchemy import Table, Column, String, Float, MetaData
from sqlalchemy.dialects.postgresql import insert as pg_insert
import base64
import uuid

# Configure logging
logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")


def extract_balance_presupuestario_data(
    year: int,
    quarter: str,
    source: str = "local",  # "local" or "s3"
    bucket_name: str = None
) -> tuple[pd.DataFrame, str | None]:
    """
    Extracts the appropriate Balance Presupuestario file either from local or S3.

    Args:
        year: e.g., 2025
        quarter: e.g., "Q1"
        source: "local" or "s3"
        bucket_name: required if source == "s3"

    Returns:
        A tuple with DataFrame and file_path or s3_key
    """
    reverse_quarter_map = {"Q1": "1T", "Q2": "2T", "Q3": "3T", "Q4": "4T"}
    file_quarter = reverse_quarter_map.get(quarter, quarter)
    file_name = f"F4_Balance_Presupuestario_LDF_{file_quarter}{year}.xlsx"

    if source == "local":
        base_dir = os.path.dirname(os.path.abspath(__file__))
        data_dir = os.path.abspath(os.path.join(base_dir, "..", "..", "data", "presupuestos"))
        file_path = os.path.join(data_dir, file_name)

        if os.path.exists(file_path):
            try:
                return pd.read_excel(file_path, sheet_name="F4 BAP", header=None), file_path
            except Exception as e:
                logging.error(f"Error reading Excel file {file_path}: {e}")
                return pd.DataFrame(), None
        else:
            logging.warning(f"File {file_path} not found.")
            return pd.DataFrame(), None

    elif source == "s3":
        if bucket_name is None:
            raise ValueError("bucket_name is required for S3 extraction")

        s3_key = f"finanzas/Balance_Presupuestario/raw/{file_name}"
        try:
            s3 = boto3.client("s3")
            obj = s3.get_object(Bucket=bucket_name, Key=s3_key)
            df = pd.read_excel(BytesIO(obj["Body"].read()), sheet_name="F4 BAP", header=None)
            return df, f"s3://{bucket_name}/{s3_key}"
        except Exception as e:
            logging.error(f"Failed to read file from S3: s3://{bucket_name}/{s3_key}. Error: {e}")
            return pd.DataFrame(), None

    else:
        raise ValueError("Invalid source. Use 'local' or 's3'")


def generate_truly_unique_key():
    uuid_part = uuid.uuid4().bytes
    entropy = os.urandom(16)
    raw = uuid_part + entropy
    return base64.urlsafe_b64encode(raw).rstrip(b'=').decode('ascii')


def generate_surrogate_key(df: pd.DataFrame) -> pd.DataFrame:
    df["surrogate_key"] = [generate_truly_unique_key() for _ in range(len(df))]
    return df


#def generate_surrogate_key(df: pd.DataFrame) -> pd.DataFrame:
#    """Genera una surrogate key a partir de las columnas clave"""
#    concatenated = (
#        df["concept"].astype(str) +
#        "|" +
#        df["sublabel"].astype(str) +
#        "|" +
#        df["year_quarter"].astype(str) +
#        "|" +
#        df["type"].astype(str)+
#        "|" +
#        df["amount"].astype(str)
#    )
#    df["surrogate_key"] = concatenated.apply(
#        lambda x: hashlib.md5(x.encode("utf-8")).hexdigest()
#    )
#    return df

def get_balance_presupuestario_table(metadata: MetaData) -> Table:
    """Devuelve la definición de la tabla balance_presupuestario"""
    return Table(
        "nuevo_leon_balance_presupuestario",
        metadata,
        Column("surrogate_key", String, primary_key=True),
        Column("concept", String),
        Column("sublabel", String),
        Column("year_quarter", String),
        Column("full_date", String),
        Column("type", String),
        Column("amount", Float),
    )

def transform_balance_presupuestario_data(df: pd.DataFrame, file_path: str) -> pd.DataFrame:
    """
    Transforms raw balance_presupuestario Excel data from Consejo Nuevo León into a clean DataFrame
    suitable for PostgreSQL insertion.
    """

    # Extraer texto del encabezado para calcular la fecha completa y el year_quarter
    header_text = df.iloc[3, 1] if not df.empty else ""
    full_date, year_quarter = parse_fecha_header(header_text)

    # Filtrar filas por patrón de código presupuestario
    pattern = r'^(A[123]|B[12]|C[12]|E[12]|F[12]|G[12])\.'
    mask = df[1].astype(str).str.match(pattern, na=False)
    df_codes = df[mask].copy()

    # Eliminar duplicados y renombrar columnas
    df_codes['code'] = df_codes[1].str.extract(pattern)
    df_unique = df_codes.drop_duplicates(subset='code', keep='first').drop(columns='code')
    df_final = df_unique.iloc[:, 1:].reset_index(drop=True)
    df_final.columns = ['raw_concept', 'estimated_or_approved', 'devengado', 'recaudado_pagado']

    # Separar concepto y sublabel
    df_final[['concept', 'sublabel']] = df_final['raw_concept'].apply(
        lambda x: pd.Series(extraer_codigo_y_sublabel(str(x)))
    )
    df_final.drop(columns=['raw_concept'], inplace=True)

    # Convertir a formato largo (long format)
    df_long = df_final.melt(
        id_vars=['concept', 'sublabel'],
        value_vars=['estimated_or_approved', 'devengado', 'recaudado_pagado'],
        var_name='type',
        value_name='amount'
    )

    df_long['year_quarter'] = year_quarter
    df_long['full_date'] = full_date
    df_long = df_long[['concept', 'sublabel', 'year_quarter', 'full_date', 'type', 'amount']]

    # Limpieza de montos
    df_long['amount'] = df_long['amount'].apply(clean_amount)

    return df_long




def find_all_presupuesto_files(bucket_name="centralfiles3", prefix="finanzas/Balance_Presupuestario/raw/"):
    """
    Finds all presupuesto file keys in the specified S3 bucket folder.
    Returns a list of (year, quarter) tuples based on filenames.
    """
    s3 = boto3.client("s3")
    paginator = s3.get_paginator("list_objects_v2")
    pattern = r"F4_Balance_Presupuestario_LDF_([1-4]T)(\d{4})\.xlsx"

    file_keys = []
    for page in paginator.paginate(Bucket=bucket_name, Prefix=prefix):
        for obj in page.get("Contents", []):
            filename = obj["Key"].split("/")[-1]
            match = re.match(pattern, filename)
            if match:
                quarter_map = {"1T": "Q1", "2T": "Q2", "3T": "Q3", "4T": "Q4"}
                quarter = quarter_map[match.group(1)]
                year = int(match.group(2))
                file_keys.append((year, quarter))

    return sorted(file_keys)


def load(
    df: pd.DataFrame,
    postgresql_client: PostgreSqlClient,
    table: Table,
    metadata: MetaData,
    load_method: str = "upsert",
) -> None:
    """
    Load the transformed DataFrame to PostgreSQL.
    """
    if load_method == "insert":
        postgresql_client.insert(
            data=df.to_dict(orient="records"), table=table, metadata=metadata
        )
    elif load_method == "upsert":
        postgresql_client.upsert(
            data=df.to_dict(orient="records"), table=table, metadata=metadata
        )
    elif load_method == "overwrite":
        postgresql_client.overwrite(
            data=df.to_dict(orient="records"), table=table, metadata=metadata
        )
    else:
        raise ValueError("Invalid load method: choose from [insert, upsert, overwrite]")






def single_load(
    df: pd.DataFrame,
    postgresql_client,
    table: Table,
    metadata: MetaData,
) -> None:
    """
    Performs a single (incremental) load with upsert behavior in PostgreSQL.
    Uses surrogate_key as the primary key.
    """
    try:
        # Ensure the table exists
        metadata.create_all(postgresql_client.engine)

        with postgresql_client.engine.connect() as conn:
            insert_stmt = pg_insert(table).values(df.to_dict(orient="records"))

            update_stmt = insert_stmt.on_conflict_do_update(
                index_elements=["surrogate_key"],
                set_={
                    "concept": insert_stmt.excluded.concept,
                    "sublabel": insert_stmt.excluded.sublabel,
                    "year_quarter": insert_stmt.excluded.year_quarter,
                    "full_date": insert_stmt.excluded.full_date,
                    "type": insert_stmt.excluded.type,
                    "amount": insert_stmt.excluded.amount,
                }
            )

            conn.execute(update_stmt)
            conn.commit()
    except Exception as e:
        raise RuntimeError(f"Single load (upsert) failed: {e}")




def bulk_load(
    df: pd.DataFrame,
    postgresql_client,
    table: Table,
    metadata: MetaData,
) -> None:
    """
    Realiza una carga bulk a PostgreSQL, truncando primero la tabla destino.
    Esta función debe usarse únicamente para cargas históricas completas.
    """
    try:
        metadata.create_all(postgresql_client.engine)

        with postgresql_client.engine.connect() as conn:
            # Truncate the table before bulk insert
            conn.execute(text(f"TRUNCATE TABLE {table.name};"))
            conn.execute(table.insert(), df.to_dict(orient="records"))
            conn.commit()
    except Exception as e:
        raise RuntimeError(f"Bulk load failed: {e}")

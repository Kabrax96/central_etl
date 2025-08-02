import os
import re
import pandas as pd
import boto3
import base64
import uuid
from io import BytesIO
from sqlalchemy import MetaData, Table, Column, String, Float, Integer, text
from sqlalchemy.dialects.postgresql import insert as pg_insert
from app.etl_central.connectors.postgresql import PostgreSqlClient
import logging


# Configure logging
logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")



#-------------------------------------------------------------
#--------------------------EXTRACT----------------------------
#-------------------------------------------------------------

def extract_ingresos_detallado_data(
    year: int,
    quarter: str,
    source: str = "s3",
    bucket_name: str = None
) -> tuple[pd.DataFrame, str | None]:
    reverse_quarter_map = {"Q1": "1T", "Q2": "2T", "Q3": "3T", "Q4": "4T"}
    file_quarter = reverse_quarter_map.get(quarter, quarter)
    file_name = f"F5_Edo_Ana_Ing_Det_LDF_{file_quarter}{year}.xlsx"

    if source == "s3":
        if bucket_name is None:
            raise ValueError("bucket_name is required for S3 extraction")

        s3_key = f"finanzas/Ingresos_Detallado/raw/{file_name}"
        try:
            s3 = boto3.client("s3")
            obj = s3.get_object(Bucket=bucket_name, Key=s3_key)
            df = pd.read_excel(BytesIO(obj["Body"].read()), sheet_name="F5 EAI", header=None)
            return df, f"s3://{bucket_name}/{s3_key}"
        except Exception as e:
            logging.error(f"Failed to read file from S3: s3://{bucket_name}/{s3_key}. Error: {e}")
            return pd.DataFrame(), None

    else:
        raise ValueError("Invalid source. Use 's3'")

#-------------------------------------------------------------
#--------------------------TRANSFORM--------------------------
#-------------------------------------------------------------

def transform_ingresos_detallado_data(df: pd.DataFrame, file_path: str = None) -> pd.DataFrame:
    date_cells     = df.iloc[3, 1:8].astype(str).str.strip()
    date_range_str = ' '.join(date_cells)
    month_map = {
        'enero':1, 'febrero':2, 'marzo':3, 'abril':4,
        'mayo':5, 'junio':6, 'julio':7, 'agosto':8,
        'septiembre':9, 'octubre':10, 'noviembre':11, 'diciembre':12
    }
    m = re.search(r'al (\d{1,2}) de (\w+) de (\d{4})', date_range_str)
    if m:
        day_n       = int(m.group(1))
        month_n     = month_map[m.group(2).lower()]
        year_n      = int(m.group(3))
        fecha_final = f"{day_n:02d}/{month_n:02d}/{year_n}"
        cuarto      = (month_n - 1) // 3 + 1
    else:
        fecha_final, cuarto = '', ''

    columns = [
        'Concepto',
        'Estimado',
        'Ampliaciones/Reducciones',
        'Modificado',
        'Devengado',
        'Recaudado',
        'Diferencia'
    ]

    prim_pat = r'^([A-Z]\.)'
    sec_pat  = r'^([a-z]\d+\))'

    data1 = df.iloc[7:44, 1:8].values
    tbl1 = pd.DataFrame(data1, columns=columns)
    tbl1['ClavePrimaria']   = tbl1['Concepto'].astype(str).str.extract(prim_pat)[0]
    tbl1['ClaveSecundaria'] = tbl1['Concepto'].astype(str).str.extract(sec_pat)[0]
    tbl1['Fecha']  = fecha_final
    tbl1['Cuarto'] = cuarto

    data2 = df.iloc[45:76, 1:8].values
    tbl2 = pd.DataFrame(data2, columns=columns)
    tbl2['ClavePrimaria']   = tbl2['Concepto'].astype(str).str.extract(prim_pat)[0]
    tbl2['ClaveSecundaria'] = tbl2['Concepto'].astype(str).str.extract(sec_pat)[0]
    tbl2['Fecha']  = fecha_final
    tbl2['Cuarto'] = cuarto

    df_final = pd.concat([tbl1, tbl2], ignore_index=True)
    return df_final

#-------------------------------------------------------------
#----------------------------LOAD-----------------------------
#-------------------------------------------------------------

def get_ingresos_detallado_table(metadata: MetaData) -> Table:
    return Table(
        "nuevo_leon_ingresos_detallado",
        metadata,
        Column("concepto", String, nullable=True),
        Column("estimado", Float, nullable=True),
        Column("ampliaciones_reducciones", Float, nullable=True),
        Column("modificado", Float, nullable=True),
        Column("devengado", Float, nullable=True),
        Column("recaudado", Float, nullable=True),
        Column("diferencia", Float, nullable=True),
        Column("clave_primaria", String, nullable=True),
        Column("clave_secundaria", String, nullable=True),
        Column("fecha", String, nullable=True),
        Column("cuarto", Integer, nullable=True),
        Column("id", String, primary_key=True, nullable=False),
    )

def generate_surrogate_key(df: pd.DataFrame) -> pd.DataFrame:
    df["id"] = (
        df["Concepto"].astype(str)
        + df["Fecha"].astype(str)
        + df["Cuarto"].astype(str)
    ).apply(lambda x: str(abs(hash(x))))
    return df

def single_load(df: pd.DataFrame, postgresql_client, table: Table, metadata: MetaData) -> None:
    try:
        metadata.create_all(postgresql_client.engine)
        with postgresql_client.engine.connect() as conn:
            insert_stmt = pg_insert(table).values(df.to_dict(orient="records"))
            update_stmt = insert_stmt.on_conflict_do_update(
                index_elements=["id"],
                set_={col.name: insert_stmt.excluded[col.name] for col in table.columns if col.name != "id"}
            )
            conn.execute(update_stmt)
            conn.commit()
    except Exception as e:
        raise RuntimeError(f"Single load (upsert) failed: {e}")

def bulk_load(df: pd.DataFrame, postgresql_client, table: Table, metadata: MetaData) -> None:
    try:
        metadata.create_all(postgresql_client.engine)
        with postgresql_client.engine.connect() as conn:
            conn.execute(text(f"TRUNCATE TABLE {table.name};"))
            conn.execute(table.insert(), df.to_dict(orient="records"))
            conn.commit()
    except Exception as e:
        raise RuntimeError(f"Bulk load failed: {e}")

def load(df: pd.DataFrame, postgresql_client, table: Table, metadata: MetaData, load_method: str = "upsert") -> None:
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

import os
import re
import pandas as pd
import hashlib
import base64
import uuid
import boto3
from io import BytesIO
from sqlalchemy import MetaData, Table, Column, String, Float, text
from sqlalchemy.dialects.postgresql import insert as pg_insert

from app.etl_central.connectors.postgresql import PostgreSqlClient
from app.etl_central.connectors.aws import read_excel_from_s3

import logging



# Configure logging
logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")


#-------------------------------------------------------------
#--------------------------EXTRACT----------------------------
#-------------------------------------------------------------



def extract_egresos_detallado_data(
    year: int,
    quarter: str,
    source: str = "s3",
    bucket_name: str = None
) -> tuple[pd.DataFrame, str | None]:
    reverse_quarter_map = {"Q1": "1T", "Q2": "2T", "Q3": "3T", "Q4": "4T"}
    file_quarter = reverse_quarter_map.get(quarter, quarter)
    file_name = f"F6_a_EAPED_Clas_Obj_Gas_LDF_{file_quarter}{year}.xlsx"


    if source == "s3":
        if bucket_name is None:
            raise ValueError("bucket_name is required for S3 extraction")

        s3_key = f"finanzas/Egresos_Detallado/raw/{file_name}"
        try:
            s3 = boto3.client("s3")
            obj = s3.get_object(Bucket=bucket_name, Key=s3_key)
            df = pd.read_excel(BytesIO(obj["Body"].read()), sheet_name="F6a COG", header=None)
            return df, f"s3://{bucket_name}/{s3_key}"
        except Exception as e:
            logging.error(f"Failed to read file from S3: s3://{bucket_name}/{s3_key}. Error: {e}")
            return pd.DataFrame(), None

    else:
        raise ValueError("Invalid source. Use 'local' or 's3'")



#-------------------------------------------------------------
#--------------------------TRANSFROM--------------------------
#-------------------------------------------------------------


def generate_truly_unique_key():
    uuid_part = uuid.uuid4().bytes
    entropy = os.urandom(16)
    raw = uuid_part + entropy
    return base64.urlsafe_b64encode(raw).rstrip(b'=').decode('ascii')


def generate_surrogate_key(df: pd.DataFrame) -> pd.DataFrame:
    df["surrogate_key"] = [generate_truly_unique_key() for _ in range(len(df))]
    return df

def get_egresos_detallado_table(metadata: MetaData) -> Table:
    return Table(
        "nuevo_leon_egresos_detallado",
        metadata,
        Column("surrogate_key", String, primary_key=True),
        Column("Codigo", String),
        Column("Concepto", String),
        Column("Aprobado", Float),
        Column("Ampliaciones/Reducciones", Float),
        Column("Modificado", Float),
        Column("Devengado", Float),
        Column("Pagado", Float),
        Column("Subejercicio", Float),
        Column("Fecha", String),
        Column("Cuarto", String),
        Column("Seccion", String),
    )


def transform_egresos_detallado_data(df: pd.DataFrame, file_path: str) -> pd.DataFrame:
    date_cell = str(df.iloc[4, 1])
    m = re.search(r'al (\d{1,2}) de (\w+) de (\d{4})', date_cell)
    month_map = {
        'enero': 1, 'febrero': 2, 'marzo': 3, 'abril': 4,
        'mayo': 5, 'junio': 6, 'julio': 7, 'agosto': 8,
        'septiembre': 9, 'octubre': 10, 'noviembre': 11, 'diciembre': 12
    }
    if m:
        day_n = int(m.group(1))
        mon_txt = m.group(2).lower()
        yr = int(m.group(3))
        mon_num = month_map.get(mon_txt, 0)
        fecha = f"{yr}-{mon_num:02d}-{day_n:02d}"
        cuarto = f"Q{(mon_num - 1) // 3 + 1}"
    else:
        fecha, cuarto = '', None

    idx_ii_candidates = df[1].astype(str).str.contains(r'^\s*II\.\s*Gasto Etiquetado', regex=True, na=False)
    if not idx_ii_candidates.any():
        raise ValueError("Header 'II. Gasto Etiquetado' not found.")
    idx_ii_header = idx_ii_candidates.idxmax()

    columnas = ['Concepto', 'Aprobado', 'Ampliaciones/Reducciones', 'Modificado', 'Devengado', 'Pagado', 'Subejercicio']
    data_I = df.iloc[8:idx_ii_header, 1:8].values
    tbl_I = pd.DataFrame(data_I, columns=columnas)

    fila_inicio_II = idx_ii_header + 1
    resto = df.iloc[fila_inicio_II:, 1].astype(str).str.strip()
    vacias = resto[resto == ''].index
    fila_fin_II = vacias[0] if len(vacias) > 0 else df.shape[0]
    data_II = df.iloc[fila_inicio_II:fila_fin_II, 1:8].values
    tbl_II = pd.DataFrame(data_II, columns=columnas)

    df_I = procesar_tabla(tbl_I, fecha, cuarto)
    df_II = procesar_tabla(tbl_II, fecha, cuarto)

    df_I["Seccion"] = "I"
    df_II["Seccion"] = "II"
    return pd.concat([df_I, df_II], ignore_index=True)

def procesar_tabla(df_tabla, fecha: str, cuarto: str) -> pd.DataFrame:
    df_tabla = df_tabla.copy()
    df_tabla['Codigo'] = df_tabla['Concepto'].apply(extract_codigo)
    df_tabla = df_tabla[df_tabla['Codigo'].notna()].drop_duplicates(subset='Codigo').reset_index(drop=True)
    df_tabla['Fecha'] = fecha
    df_tabla['Cuarto'] = cuarto
    return df_tabla

def extract_codigo(texto):
    m = re.match(r'^\s*([A-Za-z])([0-9]+)\)', str(texto))
    if m:
        return m.group(1).upper() + m.group(2)
    return None


def find_all_presupuesto_files(bucket_name="centralfiles3", prefix="finanzas/Egresos_Detallado/raw/"):
    s3 = boto3.client("s3")
    paginator = s3.get_paginator("list_objects_v2")
    pattern = r"F6_a_EAPED_Clas_Obj_Gas_LDF_([1-4]T)(\d{4})\.xlsx"

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


#-------------------------------------------------------------
#----------------------------LOAD-----------------------------
#-------------------------------------------------------------

def single_load(df: pd.DataFrame, postgresql_client, table: Table, metadata: MetaData) -> None:
    try:
        metadata.create_all(postgresql_client.engine)
        with postgresql_client.engine.connect() as conn:
            insert_stmt = pg_insert(table).values(df.to_dict(orient="records"))
            update_stmt = insert_stmt.on_conflict_do_update(
                index_elements=["surrogate_key"],
                set_={col.name: insert_stmt.excluded[col.name] for col in table.columns if col.name != "surrogate_key"}
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

# app/etl_central/tests/test_s3_single_balance.py

from dotenv import load_dotenv
from app.etl_central.assets.balance_presupuestario import (
    extract_balance_presupuestario_data,
    transform_balance_presupuestario_data
)

load_dotenv()


def test_s3_extract_and_transform():
    year = 2020
    quarter = "Q4"

    df_raw, source = extract_balance_presupuestario_data(
        year=year,
        quarter=quarter,
        source="s3",
        bucket_name="centralfiles3"
    )

    print("✅ Archivo leído desde S3:", source)
    print("🔢 Filas crudas extraídas:", len(df_raw))

    if df_raw.empty:
        print("❌ El DataFrame está vacío. Revisa si el archivo existe o si hubo un error.")
        return

    df_clean = transform_balance_presupuestario_data(df_raw, source)

    print("✅ Transformación completada.")
    print("🔢 Filas transformadas:", len(df_clean))
    print(df_clean.head())

if __name__ == "__main__":
    test_s3_extract_and_transform()

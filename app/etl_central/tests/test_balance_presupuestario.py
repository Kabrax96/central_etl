import sys
import os
import pandas as pd

# Agrega la raíz del proyecto al sys.path para importar desde cualquier ubicación
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "../../../")))

from app.etl_central.assets.balance_presupuestario import transform_financial_data


def test_transform_financial_data():
    # Simulación del input (como si viniera de Excel)
    data = [
        ["", "", "", "", ""],
        ["", "", "", "", ""],
        ["", "", "", "", ""],
        ["", "al 31 de marzo de 2023", "", "", ""],
        ["", "A1. Concepto 1", "1,000", "2,000", "3,000"],
        ["", "B1. Concepto 2", "4,000", "5,000", "6,000"],
        ["", "C1. Concepto 3", "7,000", "8,000", "9,000"],
    ]
    df_input = pd.DataFrame(data)

    expected_data = [
        {"concept": "A1", "sublabel": "Concepto 1", "year_quarter": "2023_Q1", "full_date": "2023-03-31", "type": "estimated_or_approved", "amount": 1000.0},
        {"concept": "A1", "sublabel": "Concepto 1", "year_quarter": "2023_Q1", "full_date": "2023-03-31", "type": "devengado", "amount": 2000.0},
        {"concept": "A1", "sublabel": "Concepto 1", "year_quarter": "2023_Q1", "full_date": "2023-03-31", "type": "recaudado_pagado", "amount": 3000.0},
        {"concept": "B1", "sublabel": "Concepto 2", "year_quarter": "2023_Q1", "full_date": "2023-03-31", "type": "estimated_or_approved", "amount": 4000.0},
        {"concept": "B1", "sublabel": "Concepto 2", "year_quarter": "2023_Q1", "full_date": "2023-03-31", "type": "devengado", "amount": 5000.0},
        {"concept": "B1", "sublabel": "Concepto 2", "year_quarter": "2023_Q1", "full_date": "2023-03-31", "type": "recaudado_pagado", "amount": 6000.0},
        {"concept": "C1", "sublabel": "Concepto 3", "year_quarter": "2023_Q1", "full_date": "2023-03-31", "type": "estimated_or_approved", "amount": 7000.0},
        {"concept": "C1", "sublabel": "Concepto 3", "year_quarter": "2023_Q1", "full_date": "2023-03-31", "type": "devengado", "amount": 8000.0},
        {"concept": "C1", "sublabel": "Concepto 3", "year_quarter": "2023_Q1", "full_date": "2023-03-31", "type": "recaudado_pagado", "amount": 9000.0},
    ]
    df_expected = pd.DataFrame(expected_data)

    # Ejecutar transformación
    df_transformed = transform_financial_data(df_input, "dummy_path")

    # Ordenar para comparación
    df_transformed = df_transformed.sort_values(by=["concept", "type"]).reset_index(drop=True)
    df_expected = df_expected.sort_values(by=["concept", "type"]).reset_index(drop=True)

    # Verificar igualdad
    pd.testing.assert_frame_equal(df_transformed, df_expected)
    print("✅ Test passed: Transformation logic is correct.")

if __name__ == "__main__":
    test_transform_financial_data()

# app/etl_central/assets/transform_utils.py
import re
import pandas as pd

def parse_fecha_header(text: str) -> tuple:
    """
    Extracts both full date and year_quarter from a header string.
    Example: "al 31 de marzo de 2023"
    Returns tuple: ("2023-03-31", "2023_Q1")
    """
    match = re.search(r'(\d{1,2}) de (\w+)(?: de)? (\d{4})', text.lower())
    month_map = {
        'enero': 1, 'febrero': 2, 'marzo': 3, 'abril': 4,
        'mayo': 5, 'junio': 6, 'julio': 7, 'agosto': 8,
        'septiembre': 9, 'octubre': 10, 'noviembre': 11, 'diciembre': 12
    }
    if match:
        day = int(match.group(1))
        month = month_map.get(match.group(2), 0)
        year = int(match.group(3))
        full_date = f"{year}-{month:02d}-{day:02d}"
        quarter = (month - 1) // 3 + 1
        year_quarter = f"{year}_Q{quarter}"
        return full_date, year_quarter
    return None, "unknown"

def extraer_codigo_y_sublabel(texto: str):
    """
    Extracts the code (e.g., A1) and sublabel (e.g., Concepto) from a string like "A1. Concepto"
    Returns a tuple: ("A1", "Concepto")
    """
    match = re.match(r'^(A[123]|B[12]|C[12]|E[12]|F[12]|G[12])\.\s*(.*)', texto)
    return (match.group(1), match.group(2)) if match else (None, texto)

def clean_amount(val):
    """
    Cleans monetary values in string format (e.g., "1,000" or "$2,000") and converts to float.
    """
    try:
        val = str(val).replace(",", "").replace("$", "").replace(" ", "")
        return float(val)
    except:
        return None

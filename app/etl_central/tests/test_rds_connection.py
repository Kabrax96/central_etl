from sqlalchemy import create_engine, text
import os
from dotenv import load_dotenv

load_dotenv()

def test_connection():
    user = os.getenv("DB_USERNAME")
    password = os.getenv("DB_PASSWORD")
    host = os.getenv("SERVER_NAME")
    db = os.getenv("DATABASE_NAME")
    port = os.getenv("PORT", 5432)

    url = f"postgresql+psycopg2://{user}:{password}@{host}:{port}/{db}"
    engine = create_engine(url)

    try:
        with engine.connect() as conn:
            result = conn.execute(text("SELECT version();"))
            print("✅ Conexión exitosa:")
            for row in result:
                print(row[0])
    except Exception as e:
        print("❌ Error al conectar a RDS:")
        print(e)

if __name__ == "__main__":
    test_connection()

from sqlalchemy import create_engine, Table, MetaData
from sqlalchemy.engine import URL
from sqlalchemy.dialects import postgresql


class PostgreSqlClient:
    """
    A client for querying PostgreSQL database using SQLAlchemy 2.x style.
    """

    def __init__(
        self,
        server_name: str,
        database_name: str,
        username: str,
        password: str,
        port: int = 5432,
    ):
        self.host_name = server_name
        self.database_name = database_name
        self.username = username
        self.password = password
        self.port = port

        connection_url = URL.create(
            drivername="postgresql+pg8000",
            username=username,
            password=password,
            host=server_name,
            port=port,
            database=database_name,
        )

        self.engine = create_engine(connection_url, future=True)

    def select_all(self, table: Table) -> list[dict]:
        with self.engine.connect() as conn:
            result = conn.execute(table.select())
            return [dict(row) for row in result.fetchall()]

    def create_table(self, metadata: MetaData, table: Table) -> None:
        metadata.create_all(self.engine, tables=[table])

    def drop_table(self, table_name: str) -> None:
        with self.engine.begin() as conn:
            conn.execute(f"DROP TABLE IF EXISTS {table_name};")

    def insert(self, data: list[dict], table: Table, metadata: MetaData) -> None:
        metadata.create_all(self.engine)
        insert_stmt = postgresql.insert(table).values(data)
        with self.engine.begin() as conn:
            conn.execute(insert_stmt)

    def overwrite(self, data: list[dict], table: Table, metadata: MetaData) -> None:
        self.drop_table(table.name)
        self.insert(data=data, table=table, metadata=metadata)

    def upsert(self, data: list[dict], table: Table, metadata: MetaData) -> None:
        metadata.create_all(self.engine)
        key_columns = [col.name for col in table.primary_key.columns]

        insert_stmt = postgresql.insert(table).values(data)
        update_cols = {
            col.name: insert_stmt.excluded[col.name]
            for col in table.columns
            if col.name not in key_columns
        }

        upsert_stmt = insert_stmt.on_conflict_do_update(
            index_elements=key_columns,
            set_=update_cols
        )

        with self.engine.begin() as conn:
            conn.execute(upsert_stmt)

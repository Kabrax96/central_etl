from app.etl_central.connectors.postgresql import PostgreSqlClient
from datetime import datetime
from sqlalchemy import Table, Column, Integer, String, MetaData, JSON, DateTime, insert, select, func
from sqlalchemy import text
from sqlalchemy import insert
from datetime import datetime


class MetaDataLoggingStatus:
    """Data class for log status"""

    RUN_START = "start"
    RUN_SUCCESS = "success"
    RUN_FAILURE = "fail"


class MetaDataLogging:
    def __init__(
        self,
        pipeline_name: str,
        postgresql_client: PostgreSqlClient,
        config: dict = {},
        log_table_name: str = "pipeline_logs",
    ):
        self.pipeline_name = pipeline_name
        self.log_table_name = log_table_name
        self.postgresql_client = postgresql_client
        self.config = config
        self.metadata = MetaData()
        self.table = Table(
            self.log_table_name,
            self.metadata,
            Column("id", Integer, primary_key=True, autoincrement=True),
            Column("pipeline_name", String),
            Column("run_id", Integer),
            Column("timestamp", DateTime),
            Column("status", String),
            Column("config", JSON),
            Column("logs", String),
        )
        self.run_id: int = self._get_run_id()


    def _create_log_table(self) -> None:
        """Create log table if it does not exist."""
        self.postgresql_client.create_table(metadata=self.metadata, table=self.table)

    def _get_run_id(self) -> int:
        """Retrieve the next run ID for the current pipeline."""
        try:
            self._create_log_table()
            with self.postgresql_client.engine.connect() as conn:
                result = conn.execute(
                text(f"""
                    SELECT MAX(run_id)
                    FROM {self.table.name}
                    WHERE pipeline_name = :pipeline_name
                """),
                {"pipeline_name": self.pipeline_name}
            )
            max_run_id = result.scalar() or 0
            return max_run_id + 1

        except Exception as e:
            raise RuntimeError(f"Failed to retrieve run ID: {e}")




    def log(
    self,
    status: MetaDataLoggingStatus = MetaDataLoggingStatus.RUN_START,
    timestamp: datetime = None,
    logs: str = None,
) -> None:
        """Writes pipeline metadata log to a database"""
        if timestamp is None:
            timestamp = datetime.now()
        try:
            insert_statement = insert(self.table).values(
                pipeline_name=self.pipeline_name,
                timestamp=timestamp,
                run_id=self.run_id,
                status=status,
                config=self.config,
                logs=logs,
            )
            with self.postgresql_client.engine.connect() as conn:
                conn.execute(insert_statement)
                conn.commit()
        except Exception as e:
            raise RuntimeError(f"Failed to log metadata: {e}")

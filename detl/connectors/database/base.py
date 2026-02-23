import polars as pl
from detl.connectors.base import Source, Sink
from detl.exceptions import ConnectionConfigurationError

class DatabaseSource(Source):
    def __init__(self, connection_uri: str, query: str, batch_size: int | None = None):
        self.connection_uri = connection_uri
        self.query = query
        if batch_size is not None and (not isinstance(batch_size, int) or batch_size <= 0):
            raise ConnectionConfigurationError(f"batch_size must be a positive integer, got: {batch_size}")
        self.batch_size = batch_size
        
    def read(self) -> pl.DataFrame:
        try:
            return pl.read_database_uri(self.query, self.connection_uri, engine="connectorx")
        except Exception as e:
            raise ConnectionConfigurationError(f"Failed to execute database query. Error: {e}")

class DatabaseSink(Sink):
    def __init__(self, connection_uri: str, table_name: str, if_table_exists: str = "replace", batch_size: int | None = None):
        if connection_uri.startswith("mysql://"):
            connection_uri = connection_uri.replace("mysql://", "mysql+pymysql://")
            
        self.connection_uri = connection_uri
        self.table_name = table_name
        self.if_table_exists = if_table_exists
        
        if batch_size is not None and (not isinstance(batch_size, int) or batch_size <= 0):
            raise ConnectionConfigurationError(f"batch_size must be a positive integer, got: {batch_size}")
        self.batch_size = batch_size
        
    def write(self, df: pl.DataFrame | pl.LazyFrame) -> None:
        try:
            if isinstance(df, pl.LazyFrame):
                df = df.collect()
            
            # Use SQLAlchemy or ADBC depending on dialect support, defaulting to SQLAlchemy 
            # for generic DBs like MySQL which lack primary ADBC python inserts.
            engine = "adbc" if "sqlite" in self.connection_uri or "postgres" in self.connection_uri else "sqlalchemy"
            
            df.write_database(
                table_name=self.table_name,
                connection=self.connection_uri,
                if_table_exists=self.if_table_exists,
                engine=engine
            )
        except Exception as e:
            raise ConnectionConfigurationError(f"Failed to write to database table '{self.table_name}'. Error: {e}")

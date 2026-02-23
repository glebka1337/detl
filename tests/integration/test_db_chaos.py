import pytest
import polars as pl
from detl.connectors.database.postgres import PostgresSource, PostgresSink
from detl.exceptions import ConnectionConfigurationError

# Ensure these tests are run while the docker-compose backend is active

def test_wrong_dialect_in_uri():
    # Attempting to use a PostgresSource but supplying a sqlite URI
    source = PostgresSource(connection_uri="sqlite:///fake.db", query="SELECT 1")
    with pytest.raises(ConnectionConfigurationError):
        source.read()

def test_credentials_nightmare():
    # Valid host but completely wrong credentials
    source = PostgresSource(connection_uri="postgresql://fakeuser:fakepass@localhost:5432/detldb", query="SELECT 1")
    with pytest.raises(ConnectionConfigurationError, match="Failed to execute database query"):
        source.read()

def test_source_table_hallucination():
    # Querying a table that definitely doesn't exist
    source = PostgresSource(connection_uri="postgresql://user:password@localhost:5432/detldb", query="SELECT * FROM absolutely_fake_table")
    with pytest.raises(ConnectionConfigurationError, match="Failed to execute database query"):
        source.read()

def test_sink_hostility_type_mismatch():
    # Create a strict integer schema table
    df_setup = pl.DataFrame({"strict_col": [1, 2, 3]})
    setup_sink = PostgresSink(connection_uri="postgresql://user:password@localhost:5432/detldb", table_name="chaos_strict", if_table_exists="replace")
    setup_sink.write(df_setup)

    # Attempt to forcefully append pure strings into the integer column
    df_attack = pl.DataFrame({"strict_col": ["Gleb", "Ivan"]})
    attack_sink = PostgresSink(connection_uri="postgresql://user:password@localhost:5432/detldb", table_name="chaos_strict", if_table_exists="append")
    
    with pytest.raises(ConnectionConfigurationError, match="Failed to write to database table 'chaos_strict'"):
        attack_sink.write(df_attack)

def test_sql_injection_attempt():
    # Attempting to stack queries to drop tables natively during a READ operation
    # connectorx uses cursor.execute under the hood bindings which historically block stacked queries
    source = PostgresSource(
        connection_uri="postgresql://user:password@localhost:5432/detldb", 
        query="SELECT * FROM users; DROP TABLE chaos_strict;--"
    )
    with pytest.raises(ConnectionConfigurationError):
        source.read()

def test_negative_batch_size():
    # Instantiate with highly invalid batch constraints
    with pytest.raises(ConnectionConfigurationError, match="positive integer"):
        PostgresSource(connection_uri="postgresql://user:password@localhost:5432/detldb", query="SELECT 1", batch_size=-5000)

def test_zero_batch_size():
    with pytest.raises(ConnectionConfigurationError, match="positive integer"):
        PostgresSink(connection_uri="postgresql://user:password@localhost:5432/detldb", table_name="test", batch_size=0)

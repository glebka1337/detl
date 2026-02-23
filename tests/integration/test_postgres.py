import pytest
import psycopg2
from testcontainers.postgres import PostgresContainer

from detl.connectors import PostgresSource, PostgresSink
from detl.config import Config
from detl.core import Processor

@pytest.fixture(scope="module")
def postgres_container():
    # Alpine image for fast startup
    with PostgresContainer("postgres:15-alpine") as postgres:
        yield postgres

def test_detl_postgres_end_to_end(postgres_container):
    """
    Test 100% native database ETL lifecycle.
    1. Read directly from Postgres avoiding Python loops (ConnectorX).
    2. Enforce Declarative Config assertions.
    3. Write directly to a newly generated Postgres Table (ADBC).
    """
    db_url = postgres_container.get_connection_url()
    
    # 1. Setup DB Schema & Raw Data via raw Psycopg2
    conn = psycopg2.connect(
        host=postgres_container.get_container_host_ip(),
        port=postgres_container.get_exposed_port(postgres_container.port),
        user=postgres_container.username,
        password=postgres_container.password,
        dbname=postgres_container.dbname
    )
    cursor = conn.cursor()
    cursor.execute("CREATE TABLE users (id SERIAL PRIMARY KEY, name VARCHAR(50), age INT)")
    cursor.execute("INSERT INTO users (name, age) VALUES ('Alice', 25), ('Bob', 15), ('Charlie', 35), ('David', NULL)")
    conn.commit()

    # 2. Configure Manifesto Config
    yaml_config = {
        "columns": {
            "name": {"dtype": "string"},
            "age": {
                "dtype": "int",
                "on_null": {"tactic": "fill_value", "value": 18},
                "constraints": {
                    "min_policy": {
                        "threshold": 18,
                        "violate_action": {"tactic": "drop_row"}
                    }
                 }
             }
        }
    }
    
    config = Config(yaml_config)
    proc = Processor(config)
    
    # 3. Setup Postgres Connectors
    # connectorx and adbc expect pure postgres:// or postgresql:// scheme
    pure_pg_url = db_url.replace("postgresql+psycopg2://", "postgresql://")
    
    source = PostgresSource(pure_pg_url, "SELECT * FROM users")
    sink = PostgresSink(pure_pg_url, "users_clean", if_table_exists="replace")
    
    # 4. Execute ETL Pipeline Phase!
    proc.execute(source, sink)
    
    # 5. Verify Output Matrix
    cursor.execute("SELECT name, age FROM users_clean ORDER BY name")
    results = cursor.fetchall()
    
    # Assertions
    # Bob (15) was dropped for being under 18
    # David (NULL) was imputed to 18, making him eligible for the cut-off
    assert len(results) == 3 
    
    names = [r[0] for r in results]
    ages = [r[1] for r in results]
    
    assert names == ["Alice", "Charlie", "David"]
    assert ages == [25, 35, 18]

    cursor.close()
    conn.close()

import pytest
import os
import sys
import duckdb

# Add parent directory to path for imports
sys.path.insert(0, os.path.dirname(os.path.dirname(__file__)))


@pytest.fixture(scope="session", autouse=True)
def install_duckdb_extensions():
    """Install DuckDB extensions once per test session.

    Server code uses LOAD-only (extensions are pre-installed in the Docker image).
    CI runs without the image, so this fixture installs them into the runner's
    extension directory before any test that calls build_tile_connection() or
    get_isolated_db() attempts a bare LOAD.
    """
    con = duckdb.connect()
    con.sql("INSTALL httpfs")
    con.sql("INSTALL spatial")
    con.sql("INSTALL h3 FROM community")
    con.close()


@pytest.fixture
def sample_setup_sql():
    """Fixture providing sample setup SQL."""
    return """```sql
INSTALL spatial;
LOAD spatial;
INSTALL httpfs;
LOAD httpfs;
```"""


@pytest.fixture
def sample_query_optimization():
    """Fixture providing sample optimization content."""
    return """# Query Optimization

1. Use column pruning
2. Apply filters early
3. Limit result sets
"""


@pytest.fixture(autouse=True)
def _reset_stac_miss_refetch_cooldown():
    """Each test starts with the on-miss re-walk cooldown clear (#429), so a miss
    in one test can't suppress the re-walk another test expects."""
    import stac
    stac._last_miss_refetch = None
    yield
    stac._last_miss_refetch = None

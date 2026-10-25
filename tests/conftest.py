import os

import pytest


@pytest.fixture(scope="session")
def spark():
    """A local SparkSession for transforms/ tests.

    Needs a JDK (17+). If none is usable, Spark tests are skipped with a reason rather than
    erroring, so `pytest -rs` shows exactly why. See CLAUDE.md Commands for JAVA_HOME.
    """
    pyspark_sql = pytest.importorskip("pyspark.sql")
    os.environ.setdefault("PYSPARK_PYTHON", os.sys.executable)
    try:
        session = (
            pyspark_sql.SparkSession.builder.master("local[1]")
            .appName("halfwave-tests")
            .config("spark.sql.shuffle.partitions", "1")
            .config("spark.ui.enabled", "false")
            .config("spark.sql.session.timeZone", "UTC")
            .getOrCreate()
        )
    except Exception as exc:  # no JVM: environment problem, not a code failure
        pytest.skip(f"local Spark unavailable (set JAVA_HOME to a JDK 17+): {exc}")
    yield session
    session.stop()

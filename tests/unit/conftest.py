"""
Infraestrutura compartilhada para testes unitários DuckDB.

Cada teste recebe uma conexão DuckDB in-memory isolada — sem banco real, sem credenciais.
Os SQLs são adaptações dos modelos BigQuery para dialeto DuckDB (diferenças documentadas abaixo).

Adaptações BigQuery → DuckDB:
  SAFE_CAST(x AS STRING)          → TRY_CAST(x AS VARCHAR)
  SAFE_CAST(x AS INT64)           → TRY_CAST(x AS BIGINT)
  SAFE_CAST(x AS NUMERIC) / 100   → TRY_CAST(x AS DECIMAL(18,2)) / 100
  SAFE_DIVIDE(a, b)               → CASE WHEN b=0 THEN NULL ELSE a::DOUBLE/b::DOUBLE END
  CAST(x AS FLOAT64)              → CAST(x AS DOUBLE)
  CROSS JOIN UNNEST(arr) AS tid   → CROSS JOIN UNNEST(arr) AS t(tid)
  SELECT * EXCEPT (col)           → SELECT * EXCLUDE (col)
  DATE(timestamp)                 → CAST(timestamp AS DATE)
"""

import duckdb
import pandas as pd
import pytest


@pytest.fixture
def db():
    """Conexão DuckDB in-memory isolada por teste."""
    conn = duckdb.connect(":memory:")
    yield conn
    conn.close()

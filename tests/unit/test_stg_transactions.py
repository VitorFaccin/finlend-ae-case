"""
Testes unitários para models/staging/stg_transactions.sql.

Lógica validada:
  - Filtro de status 'test' na origem
  - Cast de amount_cents → amount_brl (divisão por 100)
  - Derivação de transaction_date a partir de created_at
  - Tolerância a NULLs em campos opcionais (TRY_CAST não explode)
"""

import datetime

import pandas as pd
import pytest

# SQL equivalente ao stg_transactions.sql, adaptado para DuckDB.
# Diferenças documentadas em conftest.py.
STG_TRANSACTIONS_SQL = """
SELECT
    TRY_CAST(transaction_id  AS VARCHAR)              AS transaction_id,
    TRY_CAST(merchant_id     AS VARCHAR)              AS merchant_id,
    TRY_CAST(customer_id     AS VARCHAR)              AS customer_id,
    TRY_CAST(amount_cents    AS BIGINT)               AS amount_cents,
    TRY_CAST(amount_cents    AS DECIMAL(18,2)) / 100  AS amount_brl,
    TRY_CAST(status          AS VARCHAR)              AS status,
    TRY_CAST(payment_method  AS VARCHAR)              AS payment_method,
    TRY_CAST(created_at      AS TIMESTAMP)            AS created_at,
    TRY_CAST(updated_at      AS TIMESTAMP)            AS updated_at,
    CAST(created_at AS DATE)                          AS transaction_date,
    metadata
FROM raw_transactions
WHERE status != 'test'
"""


@pytest.fixture
def raw_transactions(db):
    """Tabela de origem com 4 linhas cobrindo todos os cenários testados."""
    df = pd.DataFrame({
        "transaction_id": ["T001", "T002", "T003", "T004"],
        "merchant_id":    ["M001", "M001", "M002", "M002"],
        "customer_id":    ["C001", "C002", None,   "C004"],  # T003 tem NULL
        "amount_cents":   [1500,   2000,   500,    9999],
        "status":         ["captured", "test", "refunded", "chargeback"],
        "payment_method": ["credit",   "debit", "credit",  "credit"],
        "created_at":     [
            "2024-01-15 10:30:00",
            "2024-01-16 09:00:00",
            "2024-01-17 14:00:00",
            "2024-01-18 08:00:00",
        ],
        "updated_at": [
            "2024-01-15 10:30:00",
            "2024-01-16 09:00:00",
            "2024-01-17 14:00:00",
            "2024-01-18 08:00:00",
        ],
        "metadata": [None, None, None, None],
    })
    db.register("raw_transactions", df)
    return df


def test_filtra_status_test(db, raw_transactions):
    """Transações com status='test' não devem aparecer no output."""
    result = db.execute(STG_TRANSACTIONS_SQL).df()

    assert "T002" not in result["transaction_id"].values
    assert len(result) == 3


def test_calcula_amount_brl(db, raw_transactions):
    """amount_cents=1500 deve gerar amount_brl=15.00 (divisão por 100)."""
    result = db.execute(STG_TRANSACTIONS_SQL).df()
    row = result[result["transaction_id"] == "T001"].iloc[0]

    assert float(row["amount_brl"]) == pytest.approx(15.00)


def test_deriva_transaction_date(db, raw_transactions):
    """transaction_date deve ser a data (sem hora) extraída de created_at."""
    result = db.execute(STG_TRANSACTIONS_SQL).df()
    row = result[result["transaction_id"] == "T001"].iloc[0]

    # pandas representa DATE como Timestamp; .date() converte para comparação
    assert row["transaction_date"].date() == datetime.date(2024, 1, 15)


def test_mantém_nulos_intactos(db, raw_transactions):
    """NULL em customer_id não deve causar erro; a linha deve aparecer no output."""
    result = db.execute(STG_TRANSACTIONS_SQL).df()
    row = result[result["transaction_id"] == "T003"].iloc[0]

    # TRY_CAST de NULL retorna NULL sem explodir
    assert pd.isna(row["customer_id"])

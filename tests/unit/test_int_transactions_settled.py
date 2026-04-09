"""
Testes unitários para models/intermediate/int_transactions_settled.sql.

Lógica validada:
  - UNNEST transforma array transaction_ids em uma linha por transação
  - Deduplicação mantém apenas o settlement mais recente por transaction_id
  - Tiebreaker determinístico por settlement_id DESC quando datas são iguais
  - Transação sem settlement aparece no output (LEFT JOIN), não some
  - Coluna auxiliar rn não vaza para o output (SELECT * EXCLUDE)
"""

import pandas as pd
import pytest

# SQL equivalente ao int_transactions_settled.sql, adaptado para DuckDB.
# Omitimos o bloco de incremental (is_incremental) — testamos a lógica full-refresh.
INT_TRANSACTIONS_SETTLED_SQL = """
WITH unnested AS (
    SELECT
        s.settlement_id,
        TRY_CAST(s.net_amount_cents AS DECIMAL(18,2)) / 100  AS net_amount,
        TRY_CAST(s.fee_amount_cents AS DECIMAL(18,2)) / 100  AS fee_amount,
        s.settlement_date,
        s.paid_at,
        t.tid AS transaction_id
    FROM raw_settlements s
    CROSS JOIN UNNEST(s.transaction_ids) AS t(tid)
),
deduped AS (
    SELECT
        t.transaction_id,
        t.merchant_id,
        t.customer_id,
        t.amount_cents,
        t.amount_brl,
        t.status,
        t.payment_method,
        t.created_at,
        t.updated_at,
        t.transaction_date,
        t.metadata,
        u.settlement_id,
        u.net_amount,
        u.fee_amount,
        u.settlement_date,
        u.paid_at,
        ROW_NUMBER() OVER (
            PARTITION BY t.transaction_id
            ORDER BY u.settlement_date DESC, u.settlement_id DESC
        ) AS rn
    FROM stg_transactions t
    LEFT JOIN unnested u USING (transaction_id)
    WHERE t.status IN ('captured', 'refunded', 'chargeback')
)
SELECT * EXCLUDE (rn)
FROM deduped
WHERE rn = 1
"""


def _stg_transactions(db, rows):
    """Registra stg_transactions com as colunas esperadas pelo intermediate."""
    df = pd.DataFrame(rows, columns=[
        "transaction_id", "merchant_id", "customer_id", "amount_cents",
        "amount_brl", "status", "payment_method", "created_at", "updated_at",
        "transaction_date", "metadata",
    ])
    db.register("stg_transactions", df)


def _raw_settlements(db, rows):
    """Registra raw_settlements com transaction_ids como lista Python (array DuckDB)."""
    df = pd.DataFrame(rows, columns=[
        "settlement_id", "net_amount_cents", "fee_amount_cents",
        "settlement_date", "paid_at", "transaction_ids",
    ])
    db.register("raw_settlements", df)


def test_unnest_expande_array(db):
    """Um settlement com 3 IDs no array deve gerar 3 linhas após o UNNEST."""
    _stg_transactions(db, [
        ("T001", "M001", "C001", 1000, 10.00, "captured", "credit",
         "2024-01-10 10:00:00", "2024-01-10 10:00:00", "2024-01-10", None),
        ("T002", "M001", "C002", 2000, 20.00, "captured", "credit",
         "2024-01-10 11:00:00", "2024-01-10 11:00:00", "2024-01-10", None),
        ("T003", "M001", "C003", 3000, 30.00, "captured", "debit",
         "2024-01-10 12:00:00", "2024-01-10 12:00:00", "2024-01-10", None),
    ])
    _raw_settlements(db, [
        ("S001", 6000, 300, "2024-01-15", "2024-01-16", ["T001", "T002", "T003"]),
    ])

    result = db.execute(INT_TRANSACTIONS_SETTLED_SQL).df()

    assert len(result) == 3
    assert set(result["transaction_id"]) == {"T001", "T002", "T003"}


def test_deduplicacao_mantém_settlement_mais_recente(db):
    """T001 em S1 (jan/05) e S2 (jan/10) → output deve referenciar S2."""
    _stg_transactions(db, [
        ("T001", "M001", "C001", 1500, 15.00, "captured", "credit",
         "2024-01-01 10:00:00", "2024-01-01 10:00:00", "2024-01-01", None),
    ])
    _raw_settlements(db, [
        ("S001", 1400, 100, "2024-01-05", "2024-01-06", ["T001"]),
        ("S002", 1400, 100, "2024-01-10", "2024-01-11", ["T001"]),
    ])

    result = db.execute(INT_TRANSACTIONS_SETTLED_SQL).df()

    assert len(result) == 1
    assert result.iloc[0]["settlement_id"] == "S002"


def test_tiebreaker_determinístico(db):
    """Empate de settlement_date → settlement_id DESC determina o vencedor (S002 > S001)."""
    _stg_transactions(db, [
        ("T001", "M001", "C001", 1500, 15.00, "captured", "credit",
         "2024-01-01 10:00:00", "2024-01-01 10:00:00", "2024-01-01", None),
    ])
    _raw_settlements(db, [
        ("S001", 1400, 100, "2024-01-10", "2024-01-11", ["T001"]),
        ("S002", 1400, 100, "2024-01-10", "2024-01-11", ["T001"]),  # mesma data
    ])

    result = db.execute(INT_TRANSACTIONS_SETTLED_SQL).df()

    assert len(result) == 1
    assert result.iloc[0]["settlement_id"] == "S002"


def test_transação_sem_settlement(db):
    """Transação sem settlement no LEFT JOIN deve aparecer no output com campos NULL."""
    _stg_transactions(db, [
        ("T001", "M001", "C001", 1500, 15.00, "captured", "credit",
         "2024-01-01 10:00:00", "2024-01-01 10:00:00", "2024-01-01", None),
        ("T099", "M001", "C099", 500,  5.00,  "captured", "debit",
         "2024-01-02 09:00:00", "2024-01-02 09:00:00", "2024-01-02", None),
    ])
    _raw_settlements(db, [
        ("S001", 1400, 100, "2024-01-05", "2024-01-06", ["T001"]),
        # T099 não aparece em nenhum settlement
    ])

    result = db.execute(INT_TRANSACTIONS_SETTLED_SQL).df()

    assert "T099" in result["transaction_id"].values
    row_t099 = result[result["transaction_id"] == "T099"].iloc[0]
    assert pd.isna(row_t099["settlement_id"])


def test_exclui_coluna_rn(db):
    """A coluna auxiliar rn não deve existir no output (SELECT * EXCLUDE)."""
    _stg_transactions(db, [
        ("T001", "M001", "C001", 1500, 15.00, "captured", "credit",
         "2024-01-01 10:00:00", "2024-01-01 10:00:00", "2024-01-01", None),
    ])
    _raw_settlements(db, [
        ("S001", 1400, 100, "2024-01-05", "2024-01-06", ["T001"]),
    ])

    result = db.execute(INT_TRANSACTIONS_SETTLED_SQL).df()

    assert "rn" not in result.columns

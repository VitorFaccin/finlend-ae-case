"""
Testes unitários para models/marts/revenue_report.sql.

Lógica validada:
  - revenue_impact positivo para status='captured'
  - revenue_impact negativo para status='refunded'
  - revenue_impact negativo para status='chargeback'
  - merchant_name e mcc_code enriquecidos via JOIN com stg_merchants
"""

import pandas as pd
import pytest

# SQL equivalente ao revenue_report.sql, adaptado para DuckDB.
# Omitimos o bloco incremental — testamos a lógica de negócio pura.
REVENUE_REPORT_SQL = """
SELECT
    t.transaction_id,
    t.merchant_id,
    m.trade_name      AS merchant_name,
    m.mcc_code,
    t.amount_brl,
    t.status,
    t.payment_method,
    t.transaction_date,
    t.created_at,
    t.settlement_id,
    t.net_amount,
    t.fee_amount,
    t.settlement_date,
    t.paid_at,
    CASE
        WHEN t.status = 'captured'   THEN  t.amount_brl
        WHEN t.status = 'refunded'   THEN -t.amount_brl
        WHEN t.status = 'chargeback' THEN -t.amount_brl
    END AS revenue_impact
FROM int_transactions_settled t
LEFT JOIN stg_merchants m USING (merchant_id)
"""


@pytest.fixture
def stg_merchants(db):
    df = pd.DataFrame({
        "merchant_id": ["M001", "M002"],
        "trade_name":  ["Loja Alpha", "Loja Beta"],
        "mcc_code":    ["5411", "5812"],
        "created_at":  ["2023-01-01 00:00:00", "2023-06-01 00:00:00"],
    })
    db.register("stg_merchants", df)
    return df


def _settled_row(transaction_id, merchant_id, amount_brl, status):
    """Gera uma linha de int_transactions_settled com campos mínimos para o teste."""
    return {
        "transaction_id": transaction_id,
        "merchant_id":    merchant_id,
        "customer_id":    "C001",
        "amount_cents":   int(amount_brl * 100),
        "amount_brl":     amount_brl,
        "status":         status,
        "payment_method": "credit",
        "created_at":     "2024-01-15 10:00:00",
        "updated_at":     "2024-01-15 10:00:00",
        "transaction_date": "2024-01-15",
        "metadata":       None,
        "settlement_id":  "S001",
        "net_amount":     amount_brl * 0.97,
        "fee_amount":     amount_brl * 0.03,
        "settlement_date": "2024-01-20",
        "paid_at":        "2024-01-21",
    }


@pytest.fixture
def int_transactions_settled(db):
    rows = [
        _settled_row("T001", "M001", 100.00, "captured"),
        _settled_row("T002", "M001",  50.00, "refunded"),
        _settled_row("T003", "M002",  75.00, "chargeback"),
    ]
    db.register("int_transactions_settled", pd.DataFrame(rows))


def test_revenue_impact_captured_positivo(db, stg_merchants, int_transactions_settled):
    """status='captured' deve gerar revenue_impact positivo (igual a amount_brl)."""
    result = db.execute(REVENUE_REPORT_SQL).df()
    row = result[result["transaction_id"] == "T001"].iloc[0]

    assert float(row["revenue_impact"]) == pytest.approx(100.00)


def test_revenue_impact_refunded_negativo(db, stg_merchants, int_transactions_settled):
    """status='refunded' deve gerar revenue_impact negativo (−amount_brl)."""
    result = db.execute(REVENUE_REPORT_SQL).df()
    row = result[result["transaction_id"] == "T002"].iloc[0]

    assert float(row["revenue_impact"]) == pytest.approx(-50.00)


def test_revenue_impact_chargeback_negativo(db, stg_merchants, int_transactions_settled):
    """status='chargeback' deve gerar revenue_impact negativo (−amount_brl)."""
    result = db.execute(REVENUE_REPORT_SQL).df()
    row = result[result["transaction_id"] == "T003"].iloc[0]

    assert float(row["revenue_impact"]) == pytest.approx(-75.00)


def test_join_merchant_enriquece_nome(db, stg_merchants, int_transactions_settled):
    """merchant_name e mcc_code devem vir de stg_merchants via LEFT JOIN."""
    result = db.execute(REVENUE_REPORT_SQL).df()
    row = result[result["transaction_id"] == "T001"].iloc[0]

    assert row["merchant_name"] == "Loja Alpha"
    assert row["mcc_code"] == "5411"

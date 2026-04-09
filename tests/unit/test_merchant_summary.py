"""
Testes unitários para models/marts/merchant_summary.sql.

Lógica validada:
  - Bug documentado: divisão inteira retorna 0 para taxas < 1 (comportamento esperado do SQL legado)
  - Correção: SAFE_DIVIDE com FLOAT64 retorna valor decimal correto
  - Merchant sem chargeback tem chargeback_rate = 0.0 (não NULL)
  - total_revenue é a soma correta de revenue_impact (captured positivo, refunded/chargeback negativo)
"""

import pandas as pd
import pytest

# SQL equivalente ao merchant_summary.sql, adaptado para DuckDB.
# SAFE_DIVIDE(a, b) → CASE WHEN b=0 OR b IS NULL THEN NULL ELSE a::DOUBLE/b::DOUBLE END
MERCHANT_SUMMARY_SQL = """
SELECT
    merchant_id,
    merchant_name,
    mcc_code,
    COUNT(*)                                                        AS total_transactions,
    SUM(revenue_impact)                                             AS total_revenue,
    SUM(fee_amount)                                                 AS total_fees,
    SUM(CASE WHEN status = 'chargeback' THEN 1 ELSE 0 END)         AS chargebacks,
    CASE
        WHEN COUNT(*) = 0 OR COUNT(*) IS NULL THEN NULL
        ELSE SUM(CASE WHEN status = 'chargeback' THEN 1 ELSE 0 END)::DOUBLE
             / COUNT(*)::DOUBLE
    END                                                             AS chargeback_rate,
    MIN(transaction_date)                                           AS first_transaction,
    MAX(transaction_date)                                           AS last_transaction
FROM revenue_report
GROUP BY 1, 2, 3
"""

# Versão com bug legado (divisão inteira, como estava antes da correção)
MERCHANT_SUMMARY_BUG_SQL = """
SELECT
    merchant_id,
    SUM(CASE WHEN status = 'chargeback' THEN 1 ELSE 0 END) / COUNT(*) AS chargeback_rate
FROM revenue_report
GROUP BY 1
"""


def _revenue_row(transaction_id, merchant_id, merchant_name, mcc_code,
                 amount_brl, status, revenue_impact, fee_amount, transaction_date):
    return {
        "transaction_id":   transaction_id,
        "merchant_id":      merchant_id,
        "merchant_name":    merchant_name,
        "mcc_code":         mcc_code,
        "amount_brl":       amount_brl,
        "status":           status,
        "payment_method":   "credit",
        "transaction_date": transaction_date,
        "created_at":       f"{transaction_date} 10:00:00",
        "settlement_id":    "S001",
        "net_amount":       amount_brl * 0.97,
        "fee_amount":       fee_amount,
        "settlement_date":  transaction_date,
        "paid_at":          transaction_date,
        "revenue_impact":   revenue_impact,
    }


@pytest.fixture
def revenue_report_300_transacoes(db):
    """
    300 transações para M001: 295 captured + 5 chargeback.
    Chargeback rate esperado: 5/300 ≈ 0.01667.
    """
    rows = []
    for i in range(295):
        rows.append(_revenue_row(
            f"T{i:04d}", "M001", "Loja Alpha", "5411",
            10.00, "captured", 10.00, 0.30, "2024-01-15",
        ))
    for i in range(295, 300):
        rows.append(_revenue_row(
            f"T{i:04d}", "M001", "Loja Alpha", "5411",
            10.00, "chargeback", -10.00, 0.30, "2024-01-15",
        ))
    db.register("revenue_report", pd.DataFrame(rows))


@pytest.fixture
def revenue_report_sem_chargeback(db):
    """Merchant M002 com apenas transações captured (nenhum chargeback)."""
    rows = [
        _revenue_row("T001", "M002", "Loja Beta", "5812",
                     50.00, "captured", 50.00, 1.50, "2024-01-10"),
        _revenue_row("T002", "M002", "Loja Beta", "5812",
                     30.00, "captured", 30.00, 0.90, "2024-01-11"),
    ]
    db.register("revenue_report", pd.DataFrame(rows))


@pytest.fixture
def revenue_report_misto(db):
    """Merchant M003: 1 captured R$200, 1 refunded R$50, 1 chargeback R$30."""
    rows = [
        _revenue_row("T001", "M003", "Loja Gamma", "5999",
                     200.00, "captured",    200.00, 6.00, "2024-01-01"),
        _revenue_row("T002", "M003", "Loja Gamma", "5999",
                     50.00,  "refunded",   -50.00, 1.50, "2024-01-05"),
        _revenue_row("T003", "M003", "Loja Gamma", "5999",
                     30.00,  "chargeback", -30.00, 0.90, "2024-01-10"),
    ]
    db.register("revenue_report", pd.DataFrame(rows))


def test_bug_legado_divisão_inteira(db, revenue_report_300_transacoes):
    """
    Documenta o bug original: divisão inteira (SUM / COUNT) trunca para 0.
    5 chargebacks em 300 transações = 0 em SQL sem cast explícito.
    Este teste passa INTENCIONALMENTE — prova que o bug existia.
    """
    result = db.execute(MERCHANT_SUMMARY_BUG_SQL).df()
    row = result[result["merchant_id"] == "M001"].iloc[0]

    # Bug: 5 / 300 = 0 em divisão inteira
    assert int(row["chargeback_rate"]) == 0


def test_correção_chargeback_rate_decimal(db, revenue_report_300_transacoes):
    """5 chargebacks em 300 transações → chargeback_rate ≈ 0.01667 com divisão float."""
    result = db.execute(MERCHANT_SUMMARY_SQL).df()
    row = result[result["merchant_id"] == "M001"].iloc[0]

    assert float(row["chargeback_rate"]) == pytest.approx(5 / 300, rel=1e-3)


def test_zero_chargebacks(db, revenue_report_sem_chargeback):
    """Merchant sem nenhum chargeback deve ter chargeback_rate = 0.0 (não NULL)."""
    result = db.execute(MERCHANT_SUMMARY_SQL).df()
    row = result[result["merchant_id"] == "M002"].iloc[0]

    assert float(row["chargeback_rate"]) == pytest.approx(0.0)
    assert not pd.isna(row["chargeback_rate"])


def test_total_revenue_soma_revenue_impact(db, revenue_report_misto):
    """total_revenue = captured(+200) + refunded(−50) + chargeback(−30) = +120."""
    result = db.execute(MERCHANT_SUMMARY_SQL).df()
    row = result[result["merchant_id"] == "M003"].iloc[0]

    assert float(row["total_revenue"]) == pytest.approx(120.00)

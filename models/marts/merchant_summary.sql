SELECT
    merchant_id,
    merchant_name,
    mcc_code,
    COUNT(*)                                                         AS total_transactions,
    SUM(revenue_impact)                                              AS total_revenue,
    SUM(fee_amount)                                                  AS total_fees,
    SUM(CASE WHEN status = 'chargeback' THEN 1 ELSE 0 END)          AS chargebacks,
    -- SAFE_DIVIDE with FLOAT64 cast: prevents integer division returning 0 for rates < 1
    SAFE_DIVIDE(
        SUM(CASE WHEN status = 'chargeback' THEN 1 ELSE 0 END),
        CAST(COUNT(*) AS FLOAT64)
    )                                                                AS chargeback_rate,
    MIN(transaction_date)                                            AS first_transaction,
    MAX(transaction_date)                                            AS last_transaction
FROM {{ ref('revenue_report') }}
GROUP BY 1, 2, 3

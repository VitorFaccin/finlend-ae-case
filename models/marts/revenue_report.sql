{{
    config(
        materialized='incremental',
        unique_key='transaction_id',
        on_schema_change='fail',
        partition_by={
            "field": "transaction_date",
            "data_type": "date",
            "granularity": "day"
        },
        cluster_by=['merchant_id', 'status']
    )
}}

SELECT
    t.transaction_id,
    t.merchant_id,
    m.trade_name       AS merchant_name,
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

FROM {{ ref('int_transactions_settled') }} t
LEFT JOIN {{ ref('stg_merchants') }} m USING (merchant_id)

{% if is_incremental() %}
-- Filtro duplo: captura tanto transações novas (por created_at) quanto transações
-- antigas que receberam um settlement novo ou corrigido após a última execução (por settlement_date).
-- Usa o mesmo var lookback_days do int_transactions_settled para comportamento consistente.
WHERE
    DATE(t.created_at)   >= DATE_SUB(CURRENT_DATE(), INTERVAL {{ var('lookback_days', 10) }} DAY)
    OR t.settlement_date >= DATE_SUB(CURRENT_DATE(), INTERVAL {{ var('lookback_days', 10) }} DAY)
{% endif %}

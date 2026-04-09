-- Regra de negócio: o sinal de revenue_impact deve ser consistente com o status da transação.
--   captured   → receita positiva  (+amount_brl)
--   refunded   → receita negativa  (-amount_brl)
--   chargeback → receita negativa  (-amount_brl)
--
-- Qualquer linha retornada indica falha no teste.
SELECT
    transaction_id,
    status,
    amount_brl,
    revenue_impact
FROM {{ ref('revenue_report') }}
WHERE revenue_impact IS NOT NULL
  AND (
      (status = 'captured'                     AND revenue_impact <= 0)
      OR (status IN ('refunded', 'chargeback') AND revenue_impact >  0)
  )

# Notas Semânticas — Domínio Financeiro FinLend

> **Para quem é este arquivo?** Para o desenvolvedor ou time que for plugar um agente de IA
> (LangChain, Vanna.ai, dbt Semantic Layer, etc.) nas tabelas deste projeto. Ele funciona como
> o "sistema de instruções" do agente — o que ele deve saber antes de gerar qualquer SQL.

---

## 1. Propósito e Granularidade

| Tabela | Granularidade | Para que usar |
|---|---|---|
| `revenue_report` | Uma linha por `transaction_id` | Análises de volume, receita, taxas, filtros por merchant/data/método |
| `merchant_summary` | Uma linha por `merchant_id` | KPIs consolidados por lojista (taxa de chargeback, receita total) |

**O que não está nessas tabelas:**
- Transações com `status = 'test'` (excluídas na staging)
- Transações `pending` ou `failed` (excluídas na intermediate)
- Tentativas múltiplas de settlement (deduplicadas — apenas o mais recente é mantido)

---

## 2. Definição de Métricas (Evita Alucinação de Cálculo)

Esta seção define como cada métrica de negócio deve ser calculada. O agente **deve** seguir estas
definições e não tentar inferir fórmulas a partir dos nomes das colunas.

### Volume de Transações (GMV)
> "Qual foi o volume de Pix do merchant X no último mês?"

```sql
SELECT SUM(amount_brl) AS volume_pix
FROM revenue_report
WHERE merchant_id = 'X'
  AND payment_method = 'pix'
  AND transaction_date >= DATE_TRUNC(DATE_SUB(CURRENT_DATE(), INTERVAL 1 MONTH), MONTH)
  AND transaction_date <  DATE_TRUNC(CURRENT_DATE(), MONTH)
```

**Use `amount_brl`** — representa o valor bruto movimentado (GMV). Não representa o que a FinLend ganhou.

---

### Receita Líquida da FinLend
> "Quanto a FinLend faturou de verdade?"

```sql
SELECT SUM(revenue_impact) AS receita_liquida
FROM revenue_report
WHERE transaction_date >= '2024-01-01'
```

**Use `revenue_impact`** — já contém o sinal correto: positivo para vendas, negativo para estornos e chargebacks. **Nunca** use `amount_brl` para esta pergunta.

---

### Receita de Taxas
> "Quanto a Franq faturou em taxas na última semana?"

```sql
SELECT SUM(fee_amount) AS receita_taxas
FROM revenue_report
WHERE merchant_name = 'Franq'
  AND transaction_date >= DATE_SUB(CURRENT_DATE(), INTERVAL 7 DAY)
```

**Use `fee_amount`** — é o valor que a FinLend retém da transação. É diferente de `revenue_impact` (que inclui o valor do merchant).

---

### Taxa de Chargeback por Merchant
> "Quais merchants tiveram taxa de chargeback acima de 2% esse trimestre?"

**Opção A — usar `merchant_summary` (pré-calculado, período total):**
```sql
SELECT merchant_name, chargeback_rate
FROM merchant_summary
WHERE chargeback_rate > 0.02
```

**Opção B — calcular por período específico a partir de `revenue_report`:**
```sql
SELECT
  merchant_id,
  merchant_name,
  SAFE_DIVIDE(
    COUNTIF(status = 'chargeback'),
    COUNT(*)
  ) AS chargeback_rate
FROM revenue_report
WHERE transaction_date >= DATE_TRUNC(CURRENT_DATE(), QUARTER)
GROUP BY 1, 2
HAVING chargeback_rate > 0.02
```

⚠️ **Nunca** aplicar `SUM(chargeback_rate)` ou `AVG(chargeback_rate)` da `merchant_summary` para obter uma taxa agregada — o resultado será matematicamente incorreto.

---

## 3. Armadilhas do Agente (Anti-Alucinação)

### Armadilha 1: Confundir `amount_brl` com receita
**O que o agente faria errado:** Para "quanto a FinLend faturou?", usar `SUM(amount_brl)`.  
**Por que está errado:** `amount_brl` é o valor bruto da transação — inclui o dinheiro do merchant, não apenas a taxa da FinLend. Também não desconta estornos.  
**O que previne:** A descrição e `synonyms` de `revenue_impact` no schema.yml deixam claro que ele é o campo correto para faturamento. O meta tag `synonyms: ["faturamento", "receita real"]` mapeia as perguntas naturais para o campo certo.

---

### Armadilha 2: Re-joinar com `raw.settlements`
**O que o agente faria errado:** Tentar dar JOIN em `raw.settlements` para obter dados de taxas.  
**Por que está errado:** O UNNEST e a deduplicação já foram feitos na camada intermediate. Um novo JOIN geraria duplicatas — exatamente o problema que o projeto herdado tinha.  
**O que previne:** O meta tag `note: "Não joinar raw.settlements"` na descrição de `settlement_id` e a nota neste documento.

---

### Armadilha 3: Filtrar `status = 'test'` manualmente
**O que o agente faria errado:** Adicionar `WHERE status != 'test'` nas queries.  
**Por que está errado:** Esse filtro já foi aplicado na staging. Adicionar novamente não quebra nada, mas indica que o agente não entende a arquitetura.  
**O que previne:** A descrição de `status` no schema.yml informa que os valores possíveis são apenas `['captured', 'refunded', 'chargeback']` — status 'test' não existe nesta tabela.

---

### Armadilha 4: Assumir fuso horário de Brasília
**O que o agente faria errado:** Para "última semana", usar `CURRENT_DATE()` sem considerar fuso.  
**Por que está errado:** Todos os timestamps estão em UTC. Para perguntas sobre "hoje" em horário de Brasília, a data correta é `DATE(CURRENT_TIMESTAMP(), 'America/Sao_Paulo')`.  
**O que previne:** O meta tag `timezone: "UTC"` no campo `transaction_date` e esta nota.

---

### Armadilha 5: Re-agregar `chargeback_rate` da `merchant_summary`
**O que o agente faria errado:** `SELECT AVG(chargeback_rate) FROM merchant_summary` para a taxa média geral.  
**Por que está errado:** A média de taxas individuais não é a taxa geral (problema de pesos). Um merchant com 1000 transações tem peso diferente de um com 10.  
**O que previne:** O meta tag `agg: "none"` e a descrição explícita no schema.yml.

---

## 4. Regras de Filtragem Temporal

| Expressão | SQL BigQuery correto |
|---|---|
| "último mês" | `transaction_date >= DATE_TRUNC(DATE_SUB(CURRENT_DATE(), INTERVAL 1 MONTH), MONTH) AND transaction_date < DATE_TRUNC(CURRENT_DATE(), MONTH)` |
| "esse trimestre" | `transaction_date >= DATE_TRUNC(CURRENT_DATE(), QUARTER)` |
| "última semana" | `transaction_date >= DATE_SUB(CURRENT_DATE(), INTERVAL 7 DAY)` |
| "hoje (BRT)" | `transaction_date = DATE(CURRENT_TIMESTAMP(), 'America/Sao_Paulo')` |

---

## 5. O que Falta para uma Camada Semântica Completa

O projeto atual representa uma fundação sólida (Tier 1: metadados ricos no schema.yml). Para
chegar ao estado ideal de produção, as seguintes evoluções seriam necessárias:

### Curto prazo
- **API MetricFlow em produção**: Configurar o dbt Semantic Layer no dbt Cloud para expor as
  métricas de `semantic_layer.yml` via API (GraphQL/JDBC). O agente passaria a requisitar
  `metric: net_revenue, dimension: merchant_name, grain: month` e o dbt geraria o SQL — sem
  risco de alucinação de fórmula.

- **Tabela de dimensão de MCC**: `mcc_code` é um código numérico (ex: `5411`). Sem um dicionário,
  o agente não sabe que `5411 = Supermercados`. Uma tabela `dim_mcc.sql` com código → categoria
  habilitaria perguntas como "qual categoria teve mais chargebacks?".

### Médio prazo
- **Snapshots de merchants (`dbt snapshot`)**: Para rastrear mudanças históricas no cadastro
  (ex: merchant que mudou de categoria). Necessário para análises de cohort.

- **Métricas de cohort**: Retenção de merchants, LTV por segmento de MCC, análise de churn.
  Requer tabelas de dimensão de tempo (date_spine).

### Longo prazo
- **Data Contracts**: Garantir via testes que as fontes raw nunca quebrem os contratos de schema
  assumidos pela staging (usando `dbt-contracts` ou `elementary`).

- **Conversão de moeda**: Caso a FinLend expanda internacionalmente, uma tabela de câmbio diário
  e lógica de conversão para USD/EUR seria necessária no mart.

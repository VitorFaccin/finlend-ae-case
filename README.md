# FinLend — Refatoração dbt e Preparação para IA

**Stack:** dbt · BigQuery · MetricFlow (dbt Semantic Layer)
---

## Como Executar

**Pré-requisitos:** Python 3.9+, conta GCP com BigQuery, service account com permissão de leitura/escrita nos datasets.

```bash
# 1. Instalar dbt-bigquery
pip install dbt-bigquery==1.8.0

# 2. Configurar a conexão com BigQuery
cp profiles.yml.example ~/.dbt/profiles.yml
# Editar ~/.dbt/profiles.yml com seu project ID e caminho do service account

# 3. Instalar pacotes dbt
dbt deps

# 4. Validar o projeto (sem conexão com banco)
dbt parse

# 5. Executar o pipeline completo
dbt run

# 6. Rodar todos os testes
dbt test

# 7. Reprocessar apenas os últimos 30 dias (sem rebuild completo)
dbt run --vars '{"lookback_days": 30}'

# 8. Rebuild completo (útil após mudança de schema)
dbt run --full-refresh
```

---

## Contexto

Ao herdar o projeto dbt da FinLend, três problemas foram imediatamente visíveis:

1. **CFO**: a conta do BigQuery triplicou sem crescimento proporcional de volume
2. **Analistas**: "os números nunca batem" entre relatórios
3. **Produto**: quer que um agente de IA responda perguntas de negócio via linguagem natural

Este repositório documenta o diagnóstico, as correções implementadas e a estratégia adotada para
preparar os modelos para consumo por IA.

---

## Parte 1 — Diagnóstico Técnico

Problemas identificados no projeto legado, ordenados do mais grave ao menos grave:

---

### Problema 1 — Materialização full-table sem incrementalidade _(severidade: crítica)_

**O que está errado:**
`revenue_report` está configurado como `materialized='table'`. A cada execução do pipeline, o dbt
descarta a tabela inteira e processa todo o histórico de transações desde o primeiro dia da empresa.

**Impacto real:**
Esta é a causa direta do custo triplicado no BigQuery. O BigQuery cobra por volume de dados
processados (bytes scanned). Um projeto com 12 meses de história processa o dobro de um com 6
meses — e o custo cresce exponencialmente com o tempo, não linearmente com o volume de novos dados.

**Correção:**
Converter para `materialized='incremental'` com `unique_key='transaction_id'` e um filtro de
lookback configurável via `var('lookback_days', 10)` com janela dupla: re-processa transações
novas (por `created_at`) e transações antigas que receberam settlements corrigidos (por
`settlement_date`). Adicionar `cluster_by` para reduzir o custo de queries filtradas por merchant
ou status. A camada intermediate (`int_transactions_settled`) também é incremental, evitando o
UNNEST completo de `raw.settlements` a cada execução.

---

### Problema 2 — JOIN via `IN UNNEST()` acoplado ao mart _(severidade: crítica)_

**O que está errado:**
A query `LEFT JOIN settlements ON transaction_id IN UNNEST(s.transaction_ids)` está diretamente
dentro do mart `revenue_report`. Esta operação força o BigQuery a avaliar o array `transaction_ids`
de *cada linha de settlements* contra *cada linha de transactions* — o equivalente a um cross-join
com filtro, computacionalmente caro.

Além disso, a deduplicação por `ROW_NUMBER() OVER (...) QUALIFY rn = 1` usa `ORDER BY settlement_date DESC`
sem um tiebreaker determinístico. Quando dois settlements têm a mesma data, o resultado pode
mudar entre execuções — gerando os relatórios inconsistentes que os analistas reportam.

Um detalhe adicional: o `SELECT *` com QUALIFY vaza a coluna `rn` (artefato interno de
ROW_NUMBER) para o output final do mart, poluindo o schema.

**Impacto real:**
- "Os números nunca batem": deduplicação não-determinística gera resultados diferentes por execução
- Custo elevado: cross-join em tabelas históricas a cada pipeline run
- Coluna `rn` exposta em dashboards e para agentes de IA como se fosse um campo de negócio

**Correção:**
Criar uma camada intermediate (`int_transactions_settled`) que isola o UNNEST, faz o JOIN e a
deduplicação com tiebreaker determinístico (`ORDER BY settlement_date DESC, settlement_id DESC`).
O mart `revenue_report` passa a consumir apenas um `ref()` limpo, sem ver o UNNEST.

---

### Problema 3 — Divisão inteira em `chargeback_rate` _(severidade: crítica)_

**O que está errado:**
```sql
-- Código legado
SUM(CASE WHEN status = 'chargeback' THEN 1 ELSE 0 END) / COUNT(*) as chargeback_rate
```
No BigQuery (e na maioria dos bancos SQL), divisão entre dois inteiros retorna um inteiro.
Para um merchant com 5 chargebacks em 300 transações: `5 / 300 = 0`. O resultado é sempre zero
para qualquer taxa abaixo de 100%.

**Impacto real:**
O campo `chargeback_rate` é silenciosamente incorreto em 100% dos casos reais. O time de risco
tomou decisões baseadas em uma coluna que sempre retorna zero. Este é um erro de dados que não
gera exceção nem log de erro — passa completamente despercebido sem testes.

**Correção:**
```sql
SAFE_DIVIDE(
    SUM(CASE WHEN status = 'chargeback' THEN 1 ELSE 0 END),
    CAST(COUNT(*) AS FLOAT64)
) AS chargeback_rate
```

---

### Problema 4 — `source('raw', 'merchants')` dentro de um mart _(severidade: alta)_

**O que está errado:**
O mart `revenue_report` acessa diretamente `{{ source('raw', 'merchants') }}`. Isso acopla a
lógica de negócio ao schema bruto da fonte.

**Impacto real:**
Qualquer renomeação de coluna em `raw.merchants` (ex: `trade_name` → `name`) quebra o mart de
receita imediatamente. Em projetos maiores, esse padrão se replica: 10 marts referenciando a mesma
fonte raw = 10 pontos de falha para cada mudança na fonte.

**Correção:**
Criar `stg_merchants.sql` que encapsula o acesso à fonte raw. Os marts referenciam apenas
`ref('stg_merchants')`. Mudanças na fonte raw impactam somente um arquivo.

---

### Problema 5 — Ausência total de testes de dados _(severidade: alta)_

**O que está errado:**
O `schema.yml` legado contém apenas descrições genéricas. Nenhum teste de `unique`, `not_null`,
`accepted_values` ou regra de negócio foi implementado.

**Impacto real:**
Duplicatas no `transaction_id`, valores nulos em `amount_brl` e chargebacks com `revenue_impact`
positivo entram nos relatórios do CFO sem qualquer alerta. A ausência de testes é a razão pela
qual o problema da divisão inteira no `chargeback_rate` passou despercebido.

**Correção:**
Testes genéricos nas PKs de todas as camadas + teste singular de regra de negócio validando o
sinal do `revenue_impact` em relação ao status da transação.

---

## Parte 2 — Refatoração Seletiva

### Arquitetura Adotada

O projeto foi reestruturado seguindo dois princípios complementares:

#### Princípio 1: I/O Sandwich (Separação de Responsabilidades por Camada)

Cada camada tem uma responsabilidade exclusiva e não a compartilha com outras:

```
raw.transactions
raw.merchants       ──► STAGING (I) ──► INTERMEDIATE ──► MARTS (O)
raw.settlements
```

- **Staging (Entrada / I):** responsabilidade única — cast de tipos, filtro de dados de teste,
  renomeação de colunas para convenção do projeto. Sem lógica de negócio, sem joins.

- **Intermediate (Processamento):** concentra toda a complexidade SQL — UNNEST do array de
  settlements, LEFT JOIN entre fontes, deduplicação por ROW_NUMBER/QUALIFY com tiebreaker
  determinístico. O analista nunca precisa abrir este arquivo para entender o negócio.

- **Marts (Saída / O):** tabelas limpas e planas, otimizadas para consumo por analistas,
  dashboards e agentes de IA. Sem UNNEST, sem ROW_NUMBER, sem subqueries. Apenas SELECTs
  simples com lógica de negócio legível.

**Por que isso resolve o "numbers never match":** o sandwich cria um mapa de depuração claro.
Se o número está correto no intermediate mas errado no mart, o problema está no mart. Se está
errado no intermediate, o problema está no staging ou na fonte. Elimina a caça ao bug em queries
de 80 linhas com múltiplos JOINs e window functions misturados.

---

#### Princípio 2: Domain-Driven Design (DDD) aplicado a dados

Os modelos refletem conceitos de negócio, não estruturas técnicas:

- **Staging:** espelha as fontes (`stg_transactions`, `stg_merchants`) — nomes técnicos
- **Marts:** refletem domínios de negócio (`revenue_report`, `merchant_summary`) — nomes de negócio

A camada intermediate funciona como a **anti-corruption layer** do DDD: ela absorve as
peculiaridades da fonte raw (arrays em settlements, formatos inconsistentes) e entrega um contrato
estável para os marts.

**Por que isso torna o projeto adaptável:** se o time de settlements mudar a estrutura de
`transaction_ids` de um ARRAY para uma tabela relacional, apenas `int_transactions_settled.sql`
precisa ser alterado. O mart `revenue_report` não sabe que essa mudança ocorreu — seu contrato
com a intermediate permanece intacto.

---

### Por que esses 3 fixes e não outros?

| Fix | Problema que resolve | Critério de priorização |
|---|---|---|
| 1. Incremental + clustering | BigQuery triplicou | Impacto financeiro direto e imediato |
| 2. Camada intermediate | Números nunca batem | Resolve o problema do usuário final + elimina o custo do UNNEST |
| 3. `SAFE_DIVIDE` + testes | Métricas silenciosamente erradas | Erro invisível que contamina decisões de negócio sem alertar |

O fix 4 (stg_merchants) foi implementado como parte natural do fix 2, sem custo adicional.
O fix 5 (testes) é a rede de segurança que garante que os três fixes anteriores não regridam.

---

### Estrutura do Projeto Refatorado

```
models/
├── staging/
│   ├── schema.yml                       # fontes raw + testes + docs
│   ├── stg_transactions.sql             # cast de tipos, filtro 'test'
│   └── stg_merchants.sql                # encapsula raw.merchants
├── intermediate/
│   ├── schema.yml
│   └── int_transactions_settled.sql     # UNNEST + JOIN + QUALIFY isolados
├── marts/
│   ├── schema.yml                       # meta tags ricas para IA
│   ├── revenue_report.sql               # incremental + clustered
│   └── merchant_summary.sql             # SAFE_DIVIDE corrigido
└── semantic_layer.yml                   # MetricFlow: métricas como código
tests/
├── assert_revenue_impact_sign_is_correct.sql
└── unit/                                # testes locais com DuckDB (sem BigQuery)
    ├── conftest.py                      # fixture de conexão + adaptações BigQuery→DuckDB
    ├── test_stg_transactions.py         # filtro, cast, derivação de data, NULLs
    ├── test_int_transactions_settled.py # UNNEST, dedup, tiebreaker, LEFT JOIN, rn excluído
    ├── test_revenue_report.py           # sinal de revenue_impact, enriquecimento por merchant
    └── test_merchant_summary.py         # bug legado documentado + correção + edge cases
```

### Testes Unitários Locais (sem BigQuery)

A pasta `tests/unit/` contém 17 testes pytest que rodam inteiramente offline usando **DuckDB
in-memory** — nenhuma credencial, nenhuma conexão, em menos de 1 segundo.

Cada teste registra DataFrames pandas como tabelas DuckDB, executa a lógica SQL do modelo
correspondente (adaptada do BigQuery para o dialeto DuckDB) e verifica o resultado com
assertions precisas.

```bash
# Instalar dependências de teste
pip install -r requirements-test.txt

# Rodar todos os testes
pytest tests/unit/ -v
```

**O par de testes mais estratégico** está em `test_merchant_summary.py`:

- `test_bug_legado_divisão_inteira` — roda o código do enunciado original e prova que
  `5 chargebacks / 300 transações = 0` em divisão inteira SQL. Passa intencionalmente:
  é documentação executável do bug que existia.

- `test_correção_chargeback_rate_decimal` — roda o código atual com `SAFE_DIVIDE` e prova que
  o resultado correto é `≈ 0.01667`. Diagnóstico e correção, ambos verificáveis.

---

## Parte 3 — Preparação para IA

### Estratégia em Dois Níveis

**Nível 1 — MVP (entregue neste projeto):** o `schema.yml` foi enriquecido com `meta` tags
estruturadas. Qualquer agente baseado em SQL (Vanna.ai, LangChain SQL Agent, etc.) consegue
ler essas tags e gerar queries corretas:

- `synonyms`: mapeia linguagem natural para campos (`"faturamento"` → `revenue_impact`)
- `enum`: previne alucinação de valores (`status` só tem 3 valores possíveis nesta tabela)
- `agg`: informa o método de agregação correto por campo
- `note`: instrui o agente sobre armadilhas específicas (ex: não re-agregar `chargeback_rate`)

**Nível 2 — Produção (roadmap):** o arquivo `models/semantic_layer.yml` define métricas usando
a sintaxe do dbt Semantic Layer (MetricFlow). Em produção, o agente não escreveria SQL — ele
enviaria uma requisição à API do dbt: `metric: net_revenue, dimension: merchant_name, grain: month`.
O MetricFlow geraria o SQL otimizado, garantindo que o número da IA seja **idêntico** ao número
do dashboard do CFO, pois ambos bebem da mesma definição de métrica.

| Característica | Nível 1 (schema.yml) | Nível 2 (MetricFlow) |
|---|---|---|
| Geração de SQL | A IA escreve baseada em dicas | O dbt gera SQL automaticamente |
| Confiabilidade | Média (sujeita a alucinações) | Alta (lógica determinística) |
| Consistência BI ↔ IA | Depende do agente | Garantida — mesma métrica |
| Custo BigQuery | Pode variar (IA pode ser ineficiente) | Otimizado pelo MetricFlow |

Para armadilhas específicas e exemplos de SQL por pergunta de negócio, ver `SEMANTIC_NOTES.md`.

---

## Parte 4 — Documento de Decisões

### Processo

O ponto de partida foi o problema mais urgente e quantificável: o custo triplicado no BigQuery.
Diagnóstico de custo revela `materialized='table'` imediatamente — é um anti-padrão óbvio em
qualquer projeto com histórico crescente.

Ao investigar o custo, o segundo problema emergiu naturalmente: o `UNNEST` em cross-join dentro
do mart era o segundo maior consumo de CPU por execução. A solução (camada intermediate) resolvia
simultaneamente o custo e o "numbers never match" — um fix com dois benefícios.

O terceiro problema (divisão inteira em `chargeback_rate`) foi identificado na revisão linha a
linha do código, não durante a análise de custo. É o tipo de erro que só aparece em revisão de
código cuidadosa — não em profiling de performance.

Os testes vieram por último porque são a rede de segurança das correções anteriores: sem eles,
qualquer regressão futura passaria despercebida exatamente como os bugs originais passaram.

---

### Uso de IA

O Gemini foi usado como parceiro de brainstorming para a estratégia geral. Ele acertou a direção
principal (incremental, intermediate layer, meta tags para IA) mas produziu erros que precisaram
de correção manual, já que ele considerou um cenário ideal que não há erros e inconsistências:

**1. Nunca identificou a divisão inteira em `chargeback_rate`**
O Gemini discutiu melhorias na métrica de chargeback, sugeriu documentação e meta tags — mas
não identificou que `SUM(CASE...) / COUNT(*)` em BigQuery sempre retorna zero para taxas abaixo
de 1.0. Este é um bug de SQL que o modelo de linguagem não detectou por análise estática, apenas
por conhecimento semântico do dialeto BigQuery. Identificado manualmente na revisão linha a linha.

**2. Filtro incremental perigoso: `MAX(updated_at) FROM {{ this }}`**
O Gemini sugeriu usar `WHERE t.updated_at >= (SELECT MAX(updated_at) FROM {{ this }})` como
filtro incremental. O problema: `updated_at` reflete quando a *transação* foi atualizada, não
quando o *settlement* chegou. Um settlement para uma transação de 2 semanas atrás chegando hoje
seria ignorado completamente por esse filtro. Corrigido para um lookback baseado em data:
`DATE(t.created_at) >= DATE_SUB(CURRENT_DATE(), INTERVAL 3 DAY)`.

**3. Nomenclatura incorreta: `dim_merchants`**
O Gemini renomeou `merchant_summary` para `dim_merchants`. Em modelagem dimensional, `dim_`
indica uma tabela de referência lentamente mutável (slowly changing dimension) — nome, categoria,
endereço. `merchant_summary` contém KPIs agregados (total_revenue, chargebacks, chargeback_rate)
— é um fato agregado, não uma dimensão. Chamar isso de `dim_merchants` indicaria ao agente de BI
que é uma tabela de lookup, não de métricas. Mantido como `merchant_summary`.

**4. Teste de negócio matematicamente incorreto**
O Gemini propôs `WHERE ABS(revenue_impact) > amount_brl` como teste de consistência de receita.
Para um estorno corretamente calculado: `revenue_impact = -amount_brl`, portanto
`ABS(-amount_brl) = amount_brl` — a condição `ABS(revenue_impact) > amount_brl` nunca se torna
verdadeira para dados válidos. O teste passaria mesmo com lógica de sinal completamente quebrada.
Reescrito para validar o sinal por status: `captured` deve ter `revenue_impact > 0`,
`refunded`/`chargeback` devem ter `revenue_impact < 0`.

**5. `rn` vazando para o output do mart (não identificado)**
O `SELECT *` com `QUALIFY rn = 1` expõe a coluna `rn` (artefato interno de ROW_NUMBER) no output
final do mart. O Gemini nunca mencionou isso. Corrigido com `SELECT * EXCEPT (rn)` na
intermediate, eliminando o campo antes de chegar ao mart.

---

### O que faria com mais tempo

**Prioridade 1 — CI/CD com dbt Cloud Slim CI**
Configurar `dbt build --select state:modified+` para rodar apenas os modelos alterados em cada
Pull Request. Hoje, qualquer mudança em `stg_transactions` rebuildaria todo o pipeline. Com Slim
CI, apenas os modelos downstream afetados seriam reprocessados, reduzindo custo e tempo de CI.

**Prioridade 2 — Data Contracts**
Implementar testes de schema nas fontes raw usando `dbt-contracts` ou `elementary`. Se a equipe
de engenharia mudar a estrutura de `raw.settlements` (ex: remover `fee_amount_cents`), o pipeline
quebraria de forma controlada com mensagem de erro clara — em vez de produzir silenciosamente
`NULL` em `fee_amount` por semanas.

**Prioridade 3 — MetricFlow em Produção**
Configurar o dbt Semantic Layer no dbt Cloud e expor as métricas de `semantic_layer.yml` via API.
O próximo passo seria conectar um agente de IA (LangChain + Tool calling) que requisitasse métricas
via API em vez de gerar SQL — garantindo que os números da IA sejam idênticos aos do relatório
oficial.

**Prioridade 4 — Dashboard de Validação**
Construir um dashboard simples em Evidence.dev conectado diretamente ao mart refatorado para
validar visualmente que os números corrigiram. Isso tornaria concreto para o CFO e os analistas
a diferença antes/depois.

---

## Parte 5 — Desafios Extras

### Custo BigQuery: por que triplicou?

Sem acesso ao `INFORMATION_SCHEMA`, analisando apenas o código dbt:

**Causa principal — Full table scan diário:**
`materialized='table'` no `revenue_report` instrui o BigQuery a deletar e recriar toda a tabela
a cada execução. Com particionamento por `transaction_date` mas sem filtro incremental, o BigQuery
varre todas as partições existentes — o custo cresce proporcionalmente ao histórico, não ao volume
de dados novos.

**Causa secundária — UNNEST como correlated cross-join:**
`t.transaction_id IN UNNEST(s.transaction_ids)` no BigQuery gera um broadcast join que avalia
cada linha de `settlements` contra cada linha de `transactions`. Para N transações e M settlements,
a complexidade é O(N×M). Com crescimento de volume, esse custo cresce quadráticamente.

**Solução implementada:**
- `materialized='incremental'` com filtro duplo (`created_at OR settlement_date >= lookback`): processa apenas dados novos e settlements tardios por execução, sem perder atualizações em transações antigas
- `var('lookback_days', 10)`: janela de reprocessamento configurável sem necessidade de `--full-refresh`
- `int_transactions_settled` também incremental: o UNNEST de `raw.settlements` roda apenas sobre settlements recentes, não o histórico inteiro
- `CROSS JOIN UNNEST(s.transaction_ids) AS tid`: join explícito, otimizável pelo planner do BigQuery
- `cluster_by=['merchant_id', 'status']`: reduz bytes scanned em queries com filtros por merchant

**Estimativa de redução:** em um projeto com 6+ meses de histórico, a transição para incremental
tipicamente reduz o volume processado por execução em 95–99%. O clustering reduz adicionalmente
o custo das queries analíticas em 70–90% para filtros comuns (por merchant, por status).

### Orquestração em Produção
Tenho um projeto de portfólio que estou construindo end-to-end em desenvolvimento para orquestração de dados que está público no github, está em desenvolvimento necessitando acrescentar etapas, mas já está funcional para clone utilização: https://github.com/VitorFaccin/data-projects-portfolio

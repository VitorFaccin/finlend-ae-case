# Tutorial: dbt + Docker + BigQuery

Este arquivo explica as mudanças introduzidas no branch `docker-pipeline` — como executar
o projeto de ponta a ponta, o que cada peça faz e por que foi construída assim.

---

## O que mudou em relação ao `main`

O `main` tinha o projeto dbt completo (modelos SQL, testes unitários DuckDB, CI/CD), mas
**não era executável contra BigQuery** sem configuração manual. Este branch adiciona a camada
de execução:

| O que era antes | O que é agora |
|---|---|
| `dbt run` só funcionava com BigQuery configurado manualmente | `docker-compose up` executa tudo do zero |
| Sem dados fake — você precisaria de dados reais | CSVs fake em `data/raw/` simulam o sistema transacional |
| Sem script de ingestão | `docker/loader/` carrega os CSVs no BigQuery como dataset `raw` |
| Profiles.yml manual em `~/.dbt/` | Gerado automaticamente pelo entrypoint via variáveis de ambiente |

### Novos arquivos

```
data/raw/
  transactions.csv     # 25 transações fake (captured, refunded, chargeback, test)
  merchants.csv        # 5 merchants com MCC codes reais
  settlements.csv      # 10 lotes de liquidação com transaction_ids pipe-separados

docker/
  dbt/
    Dockerfile         # Imagem dbt-bigquery 1.8 com fix de SSL corporativo
    entrypoint.sh      # Gera profiles.yml em runtime e executa dbt
    sitecustomize.py   # Desabilita verificação SSL (redes com proxy corporativo)
  loader/
    Dockerfile         # Python 3.11 slim com google-cloud-bigquery
    load_raw_data.py   # Carrega os 3 CSVs no BigQuery com schema explícito

docker-compose.yml     # Orquestra raw-loader → dbt em sequência
.env.example           # Template de variáveis de ambiente
Makefile               # Atalhos: make up, make test, make docs, etc.
selectors.yml          # Seletores de domínio para orquestradores
```

---

## Pré-requisitos

1. **Docker Desktop** rodando
2. **Conta GCP** com BigQuery ativo (free tier funciona)
3. **Service account** com os roles:
   - `BigQuery Data Editor`
   - `BigQuery Job User`
4. **Arquivo JSON** da service account baixado

---

## Como executar

### 1. Configurar credenciais

```bash
cp .env.example .env
# Editar .env com seu GCP_PROJECT_ID
mkdir credentials/
# Copiar seu service-account.json para credentials/service-account.json
```

### 2. Rodar o pipeline completo

```bash
docker-compose up --build
```

Isso faz em sequência:
1. Constrói as imagens Docker
2. **raw-loader**: carrega `data/raw/*.csv` no BigQuery como dataset `raw`
3. **dbt**: executa `dbt build --full-refresh` (run + test de todos os modelos)

Resultado esperado no terminal:
```
Done. PASS=34 WARN=0 ERROR=0 SKIP=0 TOTAL=34
```

### 3. Comandos individuais

```bash
# Ver os dados de um modelo específico
docker-compose run --rm dbt show --select revenue_report --limit 10

# Rodar só um modelo
docker-compose run --rm dbt run --select stg_transactions

# Rodar apenas testes
docker-compose run --rm dbt test

# Gerar e abrir documentação
docker-compose run --rm dbt docs generate
docker-compose run --rm -p 8080:8080 dbt docs serve --port 8080
# Abre http://localhost:8080
```

Se você tiver `make` instalado (Linux/Mac ou Windows com Git Bash):
```bash
make up       # pipeline completo
make test     # apenas testes
make docs     # documentação em localhost:8080
make seed     # apenas recarrega dados raw
make clean    # remove containers e artefatos
```

---

## Como o pipeline funciona por dentro

```
[data/raw/*.csv]
      │
      ▼
[raw-loader container]              ← simula Fivetran/Airbyte
  load_raw_data.py
  • cria dataset BigQuery 'raw'
  • carrega transactions (25 linhas)
  • carrega merchants (5 linhas)
  • carrega settlements (10 linhas, transaction_ids como ARRAY<STRING>)
      │ exited with code 0
      ▼
[dbt container]                     ← depends_on: service_completed_successfully
  dbt build --full-refresh
  • stg_transactions (view)
  • stg_merchants (view)
  • int_transactions_settled (incremental table)
  • revenue_report (incremental table)
  • merchant_summary (table)
  • 28 data tests
```

### Por que raw-loader em vez de `dbt seed`?

`raw.settlements` tem a coluna `transaction_ids ARRAY<STRING>`. O dbt seeds não suporta
tipos ARRAY em CSV. O loader usa o SDK do BigQuery para criar a tabela com schema explícito
incluindo campos `REPEATED` (equivalente ao ARRAY).

Além disso, ter um loader separado simula o padrão real de produção: um conector (Fivetran,
Airbyte, Stitch) carrega dados brutos para o dataset `raw`, e o dbt transforma a partir daí.
O dbt nunca toca na camada raw — só lê.

### Por que `--full-refresh` no docker-compose?

O raw-loader usa `WRITE_TRUNCATE` — apaga e recarrega as tabelas raw toda vez. O dbt precisa
acompanhar esse comportamento, recriando as tabelas incrementais do zero para ficarem em sync.

Em produção, o raw-loader seria substituído por um conector de ingestão incremental (CDC),
e o dbt rodaria sem `--full-refresh` — só processando o delta de novas transações.

---

## Os dados fake (`data/raw/`)

### transactions.csv — cenários cobertos

| ID | Status | Situação |
|---|---|---|
| T001 | captured | Aparece em S001 e S010 — testa deduplicação de settlements |
| T002–T019 | captured | Transações normais com settlement |
| T020 | captured | **Sem settlement** — testa LEFT JOIN no intermediate |
| T021, T022 | refunded | Testa revenue_impact negativo |
| T023, T024 | chargeback | Testa chargeback_rate e revenue_impact negativo |
| T025 | test | **Deve ser filtrado** pelo stg_transactions |

### settlements.csv — cenário de deduplicação

T001 aparece em dois settlements:
- **S001** (2026-02-15): covers T001, T002, T003
- **S010** (2026-02-26): covers T001 — mais recente

O `int_transactions_settled` usa `ROW_NUMBER() ORDER BY settlement_date DESC` para manter
apenas S010 para T001. Isso valida a lógica anti-duplicação do modelo intermediário.

### Por que datas em 2026?

O dataset BigQuery `projeto_dbt` tem **partition expiration de 60 dias**. Se os dados
fossem de 2024, as partições das tabelas dbt (particionadas por date) seriam expiradas
imediatamente pelo BigQuery. Datas recentes (últimos 60 dias) garantem que os dados persistam.

---

## O que é `selectors.yml` (novidade)

`selectors.yml` define grupos nomeados de modelos que podem ser chamados com uma única flag
`--selector`. Sem selectors, você precisa conhecer os nomes individuais dos modelos:

```bash
# Sem selector — frágil, acoplado aos nomes
dbt run --select stg_transactions int_transactions_settled revenue_report merchant_summary

# Com selector — desacoplado, legível
dbt run --selector finance_pipeline
```

### Os 3 selectors definidos

```yaml
finance_staging:    # só staging (validar ingestão raw)
finance_pipeline:   # staging + intermediate + marts (pipeline completo)
marts_only:         # só os marts (refresh rápido de KPIs)
```

Os modelos são selecionados por **tag** — cada camada tem sua tag definida no `dbt_project.yml`:
```yaml
staging:      +tags: ["staging"]
intermediate: +tags: ["intermediate"]
marts:        +tags: ["marts"]
```

### Como usar

```bash
# Via docker-compose
docker-compose run --rm dbt run --selector finance_pipeline
docker-compose run --rm dbt test --selector finance_staging
docker-compose run --rm dbt run --selector marts_only

# Em um orquestrador (Airflow, Prefect, GitHub Actions)
dbt run --selector finance_pipeline
```

### Por que selectors importam para um portfólio

Em produção, o orquestrador (Airflow, Dagster) não deveria conhecer os modelos dbt
individualmente — isso cria acoplamento. Com selectors:

- O Airflow chama `dbt run --selector finance_pipeline` e não sabe quais modelos existem
- Você adiciona um novo modelo à camada intermediate → o selector o inclui automaticamente
- Você muda um nome de modelo → o orquestrador não precisa ser atualizado

É o princípio de **"interface estável, implementação variável"** aplicado à orquestração de dados.

---

## Estrutura de arquivos (branch docker-pipeline)

```
finlend-ae-case/
├── data/
│   └── raw/                    # NOVO — dados fake para BigQuery
│       ├── transactions.csv
│       ├── merchants.csv
│       └── settlements.csv
├── docker/
│   ├── dbt/                    # NOVO — imagem dbt customizada
│   │   ├── Dockerfile
│   │   ├── entrypoint.sh
│   │   └── sitecustomize.py
│   └── loader/                 # NOVO — ingestão Python
│       ├── Dockerfile
│       └── load_raw_data.py
├── models/
│   ├── staging/                # igual ao main
│   ├── intermediate/           # igual ao main
│   └── marts/                  # igual ao main (schema.yml corrigido)
├── tests/unit/                 # igual ao main (17 testes DuckDB)
├── .github/workflows/ci.yml    # igual ao main (pytest + dbt parse)
├── docker-compose.yml          # NOVO
├── Makefile                    # NOVO
├── selectors.yml               # NOVO
├── .env.example                # NOVO
└── .env                        # gitignored — suas credenciais
```

---

## Diferença entre este tutorial e o `tutorial.md` do main

O `tutorial.md` (branch main) explica a **lógica SQL e as decisões de modelagem**:
como ler os modelos, quais bugs foram corrigidos, como usar o semantic layer.

Este arquivo (`tutorial_docker.md`) explica a **camada de execução**:
como rodar o projeto de verdade contra BigQuery, o que os containers fazem, e
como os selectors conectam o dbt a um orquestrador.

Use os dois juntos para ter o quadro completo do projeto.

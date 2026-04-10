#!/bin/bash
set -e

# SSL desabilitado em sitecustomize.py (executado pelo Python antes de qualquer outro código).
# Não duplicar aqui — REQUESTS_CA_BUNDLE="" causa comportamento inesperado no requests.

# Gera profiles.yml a partir das variáveis de ambiente — sem credenciais no código.
mkdir -p ~/.dbt
cat > ~/.dbt/profiles.yml << EOF
finlend:
  outputs:
    dev:
      type: bigquery
      method: service-account
      project: ${GCP_PROJECT_ID}
      dataset: ${GCP_DATASET_DEV:-analytics_dev}
      threads: 4
      timeout_seconds: 300
      location: ${GCP_LOCATION:-US}
      keyfile: ${GCP_KEYFILE:-/credentials/service-account.json}
  target: dev
EOF

exec dbt "$@"

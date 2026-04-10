#!/bin/bash
set -e

# Desabilita verificação SSL — necessário em redes corporativas com proxy/VPN
# que re-assinam certificados. Feito aqui (runtime) porque PYTHONHTTPSVERIFY=0
# não afeta a biblioteca requests usada pelo dbt deps.
export PYTHONHTTPSVERIFY=0
export REQUESTS_CA_BUNDLE=""
export CURL_CA_BUNDLE=""
export SSL_CERT_FILE=""

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

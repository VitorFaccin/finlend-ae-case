FROM ghcr.io/dbt-labs/dbt-bigquery:1.8.0

WORKDIR /usr/app

# sitecustomize.py é executado pelo Python antes de qualquer outro código —
# desabilita SSL globalmente, cobrindo dbt deps e todas as chamadas HTTP do dbt.
COPY docker/dbt/sitecustomize.py /usr/local/lib/python3.11/sitecustomize.py

COPY packages.yml dbt_project.yml ./
RUN dbt deps

COPY . .

COPY docker/dbt/entrypoint.sh /entrypoint.sh
RUN chmod +x /entrypoint.sh

ENTRYPOINT ["/entrypoint.sh"]
CMD ["build"]

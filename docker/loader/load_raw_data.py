"""
Carrega os dados fake de data/raw/ para o BigQuery como dataset 'raw'.

Simula o papel de um conector de ingestão (Fivetran/Airbyte) que carrega
dados brutos do sistema transacional para o BigQuery antes do dbt processar.

Particularidade: raw.settlements tem transaction_ids como ARRAY<STRING>.
dbt seeds não suportam tipos ARRAY, por isso usamos o SDK do BigQuery
para criar a tabela com schema explícito e campo REPEATED.
"""

import csv
import os
import ssl
import urllib3

# Desabilita verificação SSL — necessário em redes corporativas com proxy/VPN
# que re-assinam certificados (mesmo padrão do airflow_portfolio)
os.environ["PYTHONHTTPSVERIFY"] = "0"
os.environ["REQUESTS_CA_BUNDLE"] = ""
os.environ["CURL_CA_BUNDLE"] = ""
ssl._create_default_https_context = ssl._create_unverified_context
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

from google.cloud import bigquery
import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

# Patch na sessão requests para ignorar SSL — o google-auth usa requests internamente
original_send = requests.Session.send
def patched_send(self, *args, **kwargs):
    kwargs["verify"] = False
    return original_send(self, *args, **kwargs)
requests.Session.send = patched_send

PROJECT_ID  = os.environ["GCP_PROJECT_ID"]
DATASET_ID  = os.environ.get("GCP_RAW_DATASET", "raw")
KEYFILE     = os.environ.get("GCP_KEYFILE", "/credentials/service-account.json")
DATA_DIR    = os.environ.get("DATA_DIR", "/data/raw")

client = bigquery.Client.from_service_account_json(KEYFILE, project=PROJECT_ID)


def ensure_dataset(dataset_id: str) -> None:
    dataset_ref = bigquery.Dataset(f"{PROJECT_ID}.{dataset_id}")
    dataset_ref.location = os.environ.get("GCP_LOCATION", "US")
    client.create_dataset(dataset_ref, exists_ok=True)
    print(f"Dataset '{dataset_id}' pronto.")


def load_transactions() -> None:
    schema = [
        bigquery.SchemaField("transaction_id",  "STRING",    mode="REQUIRED"),
        bigquery.SchemaField("merchant_id",      "STRING",    mode="REQUIRED"),
        bigquery.SchemaField("customer_id",      "STRING",    mode="NULLABLE"),
        bigquery.SchemaField("amount_cents",     "INTEGER",   mode="REQUIRED"),
        bigquery.SchemaField("status",           "STRING",    mode="REQUIRED"),
        bigquery.SchemaField("payment_method",   "STRING",    mode="REQUIRED"),
        bigquery.SchemaField("created_at",       "TIMESTAMP", mode="REQUIRED"),
        bigquery.SchemaField("updated_at",       "TIMESTAMP", mode="NULLABLE"),
        bigquery.SchemaField("metadata",         "STRING",    mode="NULLABLE"),
    ]
    _load_csv("transactions", f"{DATA_DIR}/transactions.csv", schema)


def load_merchants() -> None:
    schema = [
        bigquery.SchemaField("id",          "STRING",    mode="REQUIRED"),
        bigquery.SchemaField("trade_name",  "STRING",    mode="REQUIRED"),
        bigquery.SchemaField("mcc_code",    "STRING",    mode="NULLABLE"),
        bigquery.SchemaField("created_at",  "TIMESTAMP", mode="NULLABLE"),
    ]
    _load_csv("merchants", f"{DATA_DIR}/merchants.csv", schema)


def load_settlements() -> None:
    """
    settlements.csv usa '|' como separador de transaction_ids (ex: 'T001|T002').
    Usa load_table_from_json (job de carga) em vez de insert_rows_json (streaming)
    porque streaming insert não está disponível no free tier do BigQuery.
    """
    import json
    import tempfile

    schema = [
        bigquery.SchemaField("settlement_id",      "STRING",    mode="REQUIRED"),
        bigquery.SchemaField("net_amount_cents",   "INTEGER",   mode="REQUIRED"),
        bigquery.SchemaField("fee_amount_cents",   "INTEGER",   mode="REQUIRED"),
        bigquery.SchemaField("settlement_date",    "DATE",      mode="REQUIRED"),
        bigquery.SchemaField("paid_at",            "TIMESTAMP", mode="NULLABLE"),
        bigquery.SchemaField("transaction_ids",    "STRING",    mode="REPEATED"),
    ]

    rows = []
    with open(f"{DATA_DIR}/settlements.csv", newline="") as f:
        for row in csv.DictReader(f):
            rows.append({
                "settlement_id":    row["settlement_id"],
                "net_amount_cents": int(row["net_amount_cents"]),
                "fee_amount_cents": int(row["fee_amount_cents"]),
                "settlement_date":  row["settlement_date"],
                "paid_at":          row["paid_at"] or None,
                "transaction_ids":  [t.strip() for t in row["transaction_ids"].split("|") if t.strip()],
            })

    table_ref = f"{PROJECT_ID}.{DATASET_ID}.settlements"
    job_config = bigquery.LoadJobConfig(
        schema=schema,
        source_format=bigquery.SourceFormat.NEWLINE_DELIMITED_JSON,
        write_disposition=bigquery.WriteDisposition.WRITE_TRUNCATE,
    )

    # Serializa para NDJSON em arquivo temporário e carrega via job (gratuito)
    with tempfile.NamedTemporaryFile(mode="w", suffix=".ndjson", delete=False) as tmp:
        for row in rows:
            tmp.write(json.dumps(row) + "\n")
        tmp_path = tmp.name

    with open(tmp_path, "rb") as f:
        job = client.load_table_from_file(f, table_ref, job_config=job_config)
    job.result()

    table = client.get_table(table_ref)
    print(f"  settlements: {table.num_rows} linhas carregadas.")


def _load_csv(table_name: str, filepath: str, schema: list) -> None:
    table_ref = f"{PROJECT_ID}.{DATASET_ID}.{table_name}"
    job_config = bigquery.LoadJobConfig(
        schema=schema,
        source_format=bigquery.SourceFormat.CSV,
        skip_leading_rows=1,
        write_disposition=bigquery.WriteDisposition.WRITE_TRUNCATE,
        null_marker="",
    )
    with open(filepath, "rb") as f:
        job = client.load_table_from_file(f, table_ref, job_config=job_config)
    job.result()
    table = client.get_table(table_ref)
    print(f"  {table_name}: {table.num_rows} linhas carregadas.")


if __name__ == "__main__":
    print(f"Carregando dados raw para {PROJECT_ID}.{DATASET_ID} ...")
    ensure_dataset(DATASET_ID)
    load_transactions()
    load_merchants()
    load_settlements()
    print("Carga completa. Pipeline dbt pode iniciar.")

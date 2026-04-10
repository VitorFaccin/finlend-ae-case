FROM python:3.11-slim

WORKDIR /app

RUN pip install --no-cache-dir \
    --trusted-host pypi.org \
    --trusted-host files.pythonhosted.org \
    --trusted-host pypi.python.org \
    google-cloud-bigquery==3.17.2

COPY docker/loader/load_raw_data.py .

CMD ["python", "load_raw_data.py"]

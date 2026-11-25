
FROM apache/airflow:3.2.1

USER airflow

RUN pip install --no-cache-dir \
    "apache-airflow==3.2.1" \
    nbconvert \
    ipykernel \
    pandas \
    numpy \
    matplotlib \
    requests \
    pdfplumber \
    inflect \
    pyspellchecker \
    rapidfuzz \
    sqlalchemy \
    python-dotenv \
    psycopg2-binary \
    openpyxl
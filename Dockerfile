FROM apache/airflow:2.3.3
COPY docker-requirements.txt .
RUN pip install -r docker-requirements.txt
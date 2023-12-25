FROM apache/airflow:2.7.1rc2-python3.11
ADD requirements.txt .
RUN pip install apache-airflow==${AIRFLOW_VERSION} -r requirements.txt
COPY ./envfile/.pyiceberg.yaml  /home/airflow/
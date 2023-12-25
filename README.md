# Building Data Lakehouse

## Architecture

![Architecture](/images/architecture.png "Architecture")

## Ingestion Layer
- Get data from Spotify API and load to Minio
- **Airflow UI:**    http://localhost:8080

![Airflow UI](/images/airflow_ui.png "Airflow")

## Storage Layer
- Storage data by Minio with medallion architecture
- **Minio UI:**    http://localhost:9000

![Minio UI](/images/minio_ui.png "Minio")

## Catalog Layer
- Organizing and managing data with Apache Iceberg

![Iceberg](/images/apache_iceberg.png "Iceberg")

## Processing Layer
- Transform data with Apache Spark
- **Spark master UI:**    http://localhost:7077
- **Spark worker UI:**    http://localhost:8081
- **Jupyter Notebook:**    http://localhost:8888

![Jupyter Notebook](/images/jupyter_notebook.png "Jupyter Notebook")

## Consumption Layer
- Build simple webapp with Streamlit

![Streamlit](/images/streamlit_web.png "Streamlit")

## Built With
- Spark
- Iceberg
- Minio
- Docker
- Airflow

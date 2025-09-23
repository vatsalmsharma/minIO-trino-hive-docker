FROM apache/airflow:2.9.2

RUN pip install --upgrade pip setuptools wheel

USER airflow

# Install papermill and dependencies into system Python
RUN pip install --no-cache-dir "apache-airflow==${AIRFLOW_VERSION}" papermill jupyter nbformat nbconvert pendulum

# RUN pip install --no-cache-dir pyspark apache-airflow-providers-apache-spark
# Base image
FROM apache/airflow:2.9.2

# Install AWS provider package
RUN pip install apache-airflow-providers-amazon
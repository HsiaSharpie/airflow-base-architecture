docker compose exec -it airflow_metadata_db  psql "host=127.0.0.1 port=5432 dbname=airflow user=db_user password=db_pass"
from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.python import PythonOperator
import polars as pl
import os





def main():
    csv_file_path = "/opt/airflow/data/data/alkon-hinnasto-tekstitiedostona_uusi.csv"
    delta_table_path = "/opt/airflow/data/data/polars_delta"


    # Read CSV file into a Polars DataFrame
    df = pl.read_csv(csv_file_path, separator=';', columns=[
        "Numero", "Nimi", "Valmistaja", "Pullokoko", "Hinta", "Litrahinta", 
        "Uutuus", "Hinnastojärjestyskoodi", "Tyyppi", "Alatyyppi", 
        "Erityisryhmä", "Oluttyyppi", "Valmistusmaa", "Alue", "Vuosikerta", 
        "Etikettimerkintöjä", "Huomautus", "Rypäleet", "Luonnehdinta", 
        "Pakkaustyyppi", "Suljentatyyppi", "Alkoholi-%", "Hapot g/l", 
        "Sokeri g/l", "Kantavierrep-%", "Väri EBC", "Katkerot EBU", 
        "Energia kcal/100 ml", "Valikoima", "EAN"
    ], truncate_ragged_lines=True, ignore_errors=True)  # Set truncate_ragged_lines to True

    # Write DataFrame as a Delta table
    df.write_delta(delta_table_path)




default_args = {
    'owner': 'Atte',
    'retries': 5,
    'retry_delay': timedelta(minutes=5)
}



with DAG(
    default_args=default_args,
    dag_id='Read all products as delta',
    description='Reads all products as delta table called polars_delta',
    start_date=datetime(2023, 10, 6),
    schedule_interval='@daily'
) as dag:
    task1 = PythonOperator(
        task_id='read_all',
        python_callable=main,
    )

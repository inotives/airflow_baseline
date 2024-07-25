from airflow.hooks.base_hook import BaseHook
import requests
import json

from dag_factory import create_dag, add_python_task
from models.db_sqlalchemy import SqlAlc
from models.sample_coffees import SampleCoffees

dag = create_dag(name='get_coffees_data_store2db', descr='A simple DAG to read JSON from API and store data to PostgreSQL')

conn = BaseHook.get_connection('airflowdb_conn')

# Fetch JSON from api then store to PostgresDB
def fetch_data():

    api_url = 'https://api.sampleapis.com/coffee/hot'

    # Fetch the JSON data from the API
    response = requests.get(api_url)
    data = response.json()

    print(data)
    return data 

def create_table():
    sqalc=SqlAlc( dbname=conn.schema, user=conn.login, password=conn.password, host=conn.host, port=conn.port )
    sqalc.create_table(SampleCoffees)
    sqalc.close()

def insert_data(ti):
    # Get data from xcom 
    data = ti.xcom_pull(task_ids='fetch_coffee_data')

    sqalc=SqlAlc( dbname=conn.schema, user=conn.login, password=conn.password, host=conn.host, port=conn.port )

    sqalc.insert_data(SampleCoffees, data)
    sqalc.close()


fetch_coffee_data = add_python_task(name='fetch_coffee_data', function=fetch_data, dag=dag)
create_table_task = add_python_task(name='create_table', function=create_table, dag=dag)
insert_data_task = add_python_task(name='insert_data2db', function=insert_data, dag=dag)

create_table_task >> fetch_coffee_data >> insert_data_task
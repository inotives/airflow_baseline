from airflow.hooks.base_hook import BaseHook
from dag_factory import create_dag, add_python_task, add_bash_task
import pandas as pd

from models.db_psql import Psql

# Create a DAG
dag = create_dag(
    name="readcsv_store2db", 
    descr="a simple dag that read data from csv then store to db with psycopg2"
)

# function to read csv 
def read_csv():
    csv_file = 'data/stores.csv'
    data_df = pd.read_csv(csv_file)
    data = data_df.to_dict('records')
    return data 

def store_to_db(ti):
    
    # get db conn detail from airflow connections 
    conn = BaseHook.get_connection('airflowdb_conn')
    psql = Psql(host=conn.host, dbname=conn.schema, user=conn.login, password=conn.password, port=conn.port)
    psql.connect()

    # create table if not exist 
    table_name = 'sample_stores'
    columns = {
        'store_id': 'SERIAL PRIMARY KEY',
        'store_address': 'TEXT',
        'store_manager_id': 'INTEGER NOT NULL'
    }
    psql.create_table(table_name, columns)


    # get data of CSV from Xcomm
    data = ti.xcom_pull(task_ids='read_csv')
    psql.upsert_data(table_name, data, ['store_id'])

    psql.close()


# create tasks 
read_csv_task = add_python_task(name='read_csv', function=read_csv, dag=dag)
store2db_task = add_python_task(name='store_2_db', function=store_to_db, dag=dag)

# task seq 
read_csv_task >> store2db_task
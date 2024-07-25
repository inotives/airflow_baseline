from airflow.hooks.base_hook import BaseHook

from dag_factory import create_dag, add_python_task
from models.db_psql import Psql

# Create a DAG
dag = create_dag(
    name="insert_to_postgres_with_psycopg2", 
    descr="a simple DAG to insert data to posgres using psycopg2."
)

# Function to insert data into PostgreSQL
def insert_data_to_postgres(**kwargs):
    # Get the connection parameters
    conn = BaseHook.get_connection('airflowdb_conn')
    psql = Psql(host=conn.host, dbname=conn.schema, user=conn.login, password=conn.password, port=conn.port)

    psql.connect()
    
        
    # Define the insert statement
    table_name = 'sample_table_1'
    columns = {
        'id': 'SERIAL PRIMARY KEY',
        'value': 'VARCHAR(100)'
    }
    data = [{'value': 'This is insertion using Psycopg2!!!!!'}]
    
    psql.create_table(table_name, columns)
    psql.insert_data(table_name, data)

    psql.close()
        
    
# Define the task
insert_data_with_psycopg = add_python_task(
    name='dag_insert2db_with_psycopg2',
    function=insert_data_to_postgres,
    dag=dag
)

insert_data_with_psycopg
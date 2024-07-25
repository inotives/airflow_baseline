from airflow.hooks.base_hook import BaseHook

from dag_factory import create_dag, add_python_task, add_bash_task
from models.db_sqlalchemy import SqlAlc
from models.sample_table_1 import SampleTable1

dag = create_dag(
    name="insert_to_postgres_with_sqlalchemy", 
    descr="insert to db with sqlalchemy"
)

conn = BaseHook.get_connection('airflowdb_conn')

def create_table(): 
    sqalc=SqlAlc(
        dbname=conn.schema,
        user=conn.login,
        password=conn.password,
        host=conn.host,
        port=conn.port
    )
    sqalc.create_table(SampleTable1)
    sqalc.close()

def insert_data(): 
    sqalc=SqlAlc(
        dbname=conn.schema,
        user=conn.login,
        password=conn.password,
        host=conn.host,
        port=conn.port
    )
    data = [
        {'value': 'Insert using SqlAlchemy !!!! 123'}
    ]
    sqalc.insert_data(SampleTable1, data)
    sqalc.close()

# Defining Tasks
show_time_task = add_bash_task(name='display_date', command='date', dag=dag)
stop_for_2sec_task = add_bash_task(name='stop_2sec', command='sleep 2', dag=dag)
create_table_task = add_python_task(
    name='create_table_with_sqlachemy',
    function=create_table,
    dag=dag
)

insert_data_task = add_python_task(
    name='insert_data_to_table_with_sqlalchemy',
    function=insert_data,
    dag=dag
)


show_time_task >> create_table_task >> stop_for_2sec_task >> insert_data_task
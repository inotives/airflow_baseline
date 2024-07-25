from airflow.hooks.base_hook import BaseHook
from bs4 import BeautifulSoup
import requests as req 

from dag_factory import create_dag, add_python_task
from utilities.tools import generate_unique_key
from models.db_psql import Psql

dag = create_dag(
    name='crawl_data_from_site_store2db', 
    descr='Simple data crawler using BS4 then store to DB.',
    schedule='@daily'
)

def crawl_data_from_site(**context):

    # Extract row data text
    def rowgetDataText(tr, coltag='td'): # td (data) or th (header)       
        return [td.get_text(strip=True) for td in tr.find_all(coltag)]  
    
    start_date = context['execution_date'].date()

    url = f"https://www.sgrates.com/bankrate/dbs.html?date={start_date}"
    html_content = req.get(url).text
    soup = BeautifulSoup(html_content, "lxml")

    data = []
    page_table = soup.find("table", attrs={"class": "table"})
    source_bank = 'DBS'

    table_row = page_table.find_all('tr')
    for index, row in enumerate(table_row):
        if index == 0: continue # skip first row, header
        row_data = rowgetDataText(row) 
        
        currency_date = start_date.strftime('%Y-%m-%d')
        uniq_key = generate_unique_key(currency_date, row_data[0])

        currency_data = {
            'uniq_id': uniq_key,
            'source_bank': source_bank,
            'currency_date': currency_date,
            'currency': row_data[0],
            'bank_buy_tt': row_data[1],
            'bank_sell_tt': row_data[2],
            'bank_buy_od': row_data[3],
            'bank_sell_od': row_data[4]
        }

        data.append(currency_data)
    
    return data 

def store_to_db(ti):
    # get db conn detail from airflow connections 
    conn = BaseHook.get_connection('airflowdb_conn')
    psql = Psql(host=conn.host, dbname=conn.schema, user=conn.login, password=conn.password, port=conn.port)
    psql.connect()

    # create table if not exist 
    table_name = 'sample_currency_rates'
    columns = {
        'uniq_id': 'VARCHAR(100) PRIMARY KEY',
        'source_bank': 'VARCHAR(40)',
        'currency_date': 'DATE NOT NULL',
        'currency': 'VARCHAR(255) NOT NULL',
        'bank_buy_tt': 'VARCHAR(40)',
        'bank_sell_tt': 'VARCHAR(40)',
        'bank_buy_od': 'VARCHAR(40)',
        'bank_sell_od': 'VARCHAR(40)'
    }
    psql.create_table(table_name, columns)

    # get the data to be inserted from xcomm 
    data = ti.xcom_pull(task_ids='crawl_data')
    psql.upsert_data(table_name, data, ['uniq_id'])

    psql.close()


# Define tasks 
crawl_data_task = add_python_task(name='crawl_data', function=crawl_data_from_site, dag=dag)
insert_data2db_task = add_python_task(name='insert_crawl_data2db', function=store_to_db, dag=dag)


# Task Seq
crawl_data_task >> insert_data2db_task
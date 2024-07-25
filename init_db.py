import psycopg2 
from psycopg2 import sql

# Database connection parameters -- !!! make sure to change these to environments variable in productions !!!
db_params = {
    'dbname': 'airflow',
    'user': 'airflow',
    'password': 'airflow',
    'host': 'localhost',
    'port': 5432
}

def execute_sql(sql):
    try: 
        # Connect to the PostgreSQL database
        conn = psycopg2.connect(**db_params)
        
        # Create a cursor object
        cur = conn.cursor()

        # Execute the CREATE TABLE statement
        cur.execute(sql)
        
        # Commit the changes
        conn.commit()

        # Close the cursor and connection
        cur.close()
        conn.close()

        return True
        
    except Exception as e:
        print(f"Error: {e}")
        return False

def create_sample_table_1 ():

   # Define the CREATE TABLE statement
    table_name = 'sample_table_1'
    create_table_query = f'''
    CREATE TABLE IF NOT EXISTS {table_name} (
        id SERIAL PRIMARY KEY,
        value VARCHAR(200)
    );
    ''' 
    ran_query = execute_sql(create_table_query)
    print(f"Table: {table_name} created!") if ran_query else print('Error!!')


def create_sample_table_2():
   # Define the CREATE TABLE statement
    table_name = 'sample_table_coffees'
    create_table_query = f'''
    CREATE TABLE IF NOT EXISTS {table_name} (
        id SERIAL PRIMARY KEY,
        title VARCHAR(200),
        description TEXT,
        images TEXT,
        ingredients TEXT []
    );
    '''
    ran_query = execute_sql(create_table_query)
    print(f"Table: {table_name} created!") if ran_query else print('Error!!')
    

if __name__ == "__main__": 
    create_sample_table_1()
    create_sample_table_2()

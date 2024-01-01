from airflow import DAG
from datetime import datetime, timedelta
from airflow.operators.python_operator import PythonOperator
from airflow.hooks.postgres_hook import PostgresHook
from psycopg2.extras import execute_values

default_args = {
    'owner': 'Airflow',
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
    'start_date': datetime(2019, 11, 26),
}

dag = DAG('hooks_demo', default_args=default_args, schedule_interval='@daily')

def transfer_function(ds, **kwargs):
    # SQL query to select all records from the source_city_table
    query = "SELECT * FROM source_city_table"

    # Creating a connection to the source database using the PostgresHook
    source_hook = PostgresHook(postgres_conn_id='postgres_conn', schema='airflow')
    source_conn = source_hook.get_conn()

    # Creating a connection to the destination database using the PostgresHook
    destination_hook = PostgresHook(postgres_conn_id='postgres_conn', schema='airflow')
    destination_conn = destination_hook.get_conn()

    # Creating cursors for the source and destination connections
    source_cursor = source_conn.cursor()
    destination_cursor = destination_conn.cursor()

    # Executing the SQL query on the source database
    source_cursor.execute(query)

    # Fetching all records from the source_cursor
    records = source_cursor.fetchall()

    # Checking if there are records to transfer
    if records:
        # Using execute_values to efficiently insert records into the destination table
        execute_values(destination_cursor, "INSERT INTO target_city_table VALUES %s", records)
        
        # Committing the changes to the destination database
        destination_conn.commit()

    # Closing the cursors and connections to release resources
    source_cursor.close()
    destination_cursor.close()
    source_conn.close()
    destination_conn.close()

    # Printing a success message
    print("Data transferred successfully!")


t1 = PythonOperator(task_id='transfer', python_callable=transfer_function, provide_context=True, dag=dag)


# create table source_city_table(city_name varchar (50), city_code varchar (20));
# insert into source_city_table (city_name, city_code) values('New York', 'ny'), ('Los Angeles', 'la'), ('Chicago', 'cg'), ('Houston', 'ht');
# create table target_city_table as (select * from source_city_table) with no data;
# select * from target_city_table;

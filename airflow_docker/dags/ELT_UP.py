from datetime import datetime
from airflow import DAG
from airflow.operators.python import PythonOperator
import pandas as pd
import psycopg2
import os   
import logging
import io



dag_path = os.getcwd()     #path original.. home en Docker


# Leer credenciales desde archivos
with open(dag_path+'/keys/'+"db.txt",'r') as f:
    data_base= f.read()
with open(dag_path+'/keys/'+"user.txt",'r') as f:
    user= f.read()
with open(dag_path+'/keys/'+"pwd.txt",'r') as f:
    pwd= f.read()
# Configuración de conexión a Redshift
redshift_conn = {
    'host': "data-engineer-cluster.cyhh5bfevlmn.us-east-1.redshift.amazonaws.com",
    'dbname': data_base,
    'user': user,
    'port': '5439',
    'password': pwd
}
# Nombre de la tabla y esquema en Redshift
tabla_redshift = 'CHURN'
esquema_redshift = 'jtroconiz_coderhouse'

def map_dtype(dtype):
    if "int" in dtype:
        return "INTEGER"
    if "float" in dtype:
        return "FLOAT"
    if "object" in dtype:  # Suponiendo que la columna del objeto es una cadena
        return "VARCHAR(MAX)"
    # Añadir más conversiones según sea necesario
    return "VARCHAR(MAX)"


def test_redshift_connection(**kwargs):
    try:
        conn = psycopg2.connect(**redshift_conn)
        conn.close()
        logging.info("Connected to Redshift successfully!")  # Modificado aquí
    except Exception as e:
        logging.error("Unable to connect to Redshift.")  # Modificado aquí
        logging.error(e)  # Modificado aquí

def load_local_data(**kwargs):
    ti = kwargs['ti']
    dataset = pd.read_csv("./raw_data/DataSet-Troconiz-E1.csv")
    ti.xcom_push(key='dataset', value=dataset.to_csv(index=False))

def create_table_in_redshift():
    dataset = pd.read_csv("./raw_data/DataSet-Troconiz-E1.csv")
    
    create_table_query = f"CREATE TABLE IF NOT EXISTS {esquema_redshift}.{tabla_redshift} ("
    for col, dtype in zip(dataset.columns, dataset.dtypes):
        redshift_dtype = map_dtype(str(dtype))
        create_table_query += f'"{col}" {redshift_dtype}, '
    create_table_query = create_table_query[:-2] + ");"
    
    conn = psycopg2.connect(**redshift_conn)
    cur = conn.cursor()
    cur.execute(create_table_query)
    conn.commit()
    cur.close()
    conn.close()


# Consulta el número de registros en Redshift
def count_records_in_redshift():
    global num_records
    conn = psycopg2.connect(**redshift_conn)
    cur = conn.cursor()
    cur.execute(f"SELECT COUNT(*) FROM {esquema_redshift}.{tabla_redshift};")
    num_records = cur.fetchone()[0]
    print(f"Number of records in the table: {num_records}")  # Print the count for logging purposes
    cur.close()
    conn.close()

def insert_random_records(**kwargs):
    ti = kwargs['ti']
    csv_content = ti.xcom_pull(task_ids='load_local_data', key='dataset')
    dataset = pd.read_csv(io.StringIO(csv_content))

    conn = psycopg2.connect(**redshift_conn)
    cur = conn.cursor()

    random_subset = dataset.sample(20)
    for index, row in random_subset.iterrows():

        # Asegúrese de que la columna requerida exista en el conjunto de datos
        if "Customer ID" not in row:
            raise Exception("La columna 'Customer ID' no existe en el conjunto de datos.")
        
        values = ', '.join(['%s'] * len(row))
        columns_str = ', '.join(['"'+str(col)+'"' for col in dataset.columns])
        
        insert_query = f"""
            INSERT INTO {esquema_redshift}.{tabla_redshift} ({columns_str})
            SELECT {values}
            WHERE NOT EXISTS (
                SELECT 1 FROM {esquema_redshift}.{tabla_redshift}
                WHERE "Customer ID" = %s
            )
        """
        cur.execute(insert_query, tuple(row) + (row["Customer ID"],))
        logging.info(f"Insertando registro: {row.to_dict()}")
        
    conn.commit()
    conn.close()



    
# Definición del DAG
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2023, 9, 3),
    'retries': 1,
}

dag = DAG('load_random_data_to_redshift', default_args=default_args, schedule_interval=None)

test_connection = PythonOperator(
    task_id='test_redshift_connection',
    python_callable=test_redshift_connection,
    dag=dag,
    provide_context=True,
)

load_task = PythonOperator(
    task_id='load_local_data',
    python_callable=load_local_data,
    dag=dag,
)

count_before_insert_task = PythonOperator(
    task_id='count_records_in_redshift_before_insert',
    python_callable=count_records_in_redshift,
    dag=dag,
)

count_after_insert_task = PythonOperator(
    task_id='count_records_in_redshift_after_insert',
    python_callable=count_records_in_redshift,
    dag=dag,
)

create_table_task = PythonOperator(
    task_id='create_table_in_redshift',
    python_callable=create_table_in_redshift,
    dag=dag,
)
insert_task = PythonOperator(
    task_id='insert_random_records',
    python_callable=insert_random_records,
    dag=dag,
)


# Secuencia en DAG
test_connection >> create_table_task >> count_before_insert_task >> insert_task >> count_after_insert_task
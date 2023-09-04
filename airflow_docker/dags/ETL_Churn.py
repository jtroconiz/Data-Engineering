from datetime import timedelta, datetime
import os
from airflow import DAG
from airflow.operators.python_operator import PythonOperator

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

# Funcion conexion a redshift
def conexion_redshift(**kwargs):
    exec_date = kwargs['execution_date']
    print(f"Conectandose a la BD en la fecha: {exec_date}") 
    # El resto de tu código sigue igual...


    try:
        conn = psycopg2.connect(**redshift_conn)
        print(conn)
        print("Connected to Redshift successfully!")
        conn.close()  # Recuerda cerrar la conexión
    except Exception as e:
        print("Unable to connect to Redshift.")
        print(e)

# Nombre de la tabla y esquema en Redshift
tabla_redshift = 'CHURN'
esquema_redshift = 'jtroconiz_coderhouse'

# Definición del DAG
default_args = {
    'owner': 'me',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

dag = DAG(
    'redshift_connection_test',
    default_args=default_args,
    description='A simple DAG to test Redshift connection',
    #schedule_interval=timedelta(days=1),
    schedule_interval=None, 
    start_date=datetime(2000, 9, 4),
    catchup=False,
)

def fetch_redshift_data():
    """
    Función para consultar y mostrar los registros de la tabla de Redshift.
    """
    try:
        with psycopg2.connect(**redshift_conn) as conn:
            with conn.cursor() as cursor:
                cursor.execute(f"SELECT * FROM {esquema_redshift}.{tabla_redshift} LIMIT 10;")  # Limitamos a 10 registros para la demostración
                rows = cursor.fetchall()
                for row in rows:
                    print(row)
            print(f"Successfully fetched data from {esquema_redshift}.{tabla_redshift}")
    except Exception as e:
        print("Error fetching data from Redshift.")
        print(e)



# Definir tarea
test_connection = PythonOperator(
    task_id='test_connection',
    python_callable=conexion_redshift,
    provide_context=True,  # Esto es crucial para pasar el contexto a tu función
    dag=dag
)


# tarea para consultar los datos
fetch_data_task = PythonOperator(
    task_id='fetch_data_from_redshift',
    python_callable=fetch_redshift_data,
    dag=dag,
)

# Definir el orden de las tareas
test_connection >> fetch_data_task
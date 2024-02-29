import mysql.connector
from mysql.connector import errorcode
import os
import pandas as pd
import threading

from mysql_queries.query_clap import sql_query_clap
from mysql_queries.query_bansur import sql_query_bansur


def connect():
    try:
        conn = mysql.connector.connect(
            host=os.environ.get('DB_HOST', 'localhost'),
            user=os.environ.get('DB_USER', 'root'),
            password=os.environ.get('DB_PASSWORD', '123456789'),
            database=os.environ.get('DB_DATABASE', 'clients_comparisons')
        )

        return conn
    except mysql.connector.Error as err:
        if err.errno == errorcode.ER_ACCESS_DENIED_ERROR:
            print("Error: Acceso denegado. Revisa tus credenciales.")
        elif err.errno == errorcode.ER_BAD_DB_ERROR:
            print("Error: Base de datos no existe.")
        else:
            print(f"Error: {err}")
        
        return None

def load_dataframe(conn, query):
    cursor = conn.cursor()
    cursor.execute(query)
    results = cursor.fetchall()
    columns = [desc[0] for desc in cursor.description]
    data =  pd.DataFrame(results, columns=columns)
    cursor.close()

    return data


def exercise_5_6(df, cross_transaction):
    df_crossed = df[df['cross_transaction'] == cross_transaction]
    porcentaje_cruzadas = (len(df_crossed) / len(df)) * 100
    print(f"Porcentaje de transacciones de CLAP que cruzaron contra BANSUR: {porcentaje_cruzadas:.2f}%")
    

def compare_and_update(df_clap, df_bansur, start_index, end_index, result_array):
    print("🚀 ~ end_index:", end_index)
    print("🚀 ~ start_index:", start_index)
    for i in range(start_index, end_index):
        print("🚀 ~ i dentro del hilo:", i)
        row_clap = df_clap.iloc[i]

        # Filtrar las filas en df_bansur que cumplen con los criterios de comparación
        filtered_rows = df_bansur[
            (df_bansur['ID'] == row_clap['ID']) &
            (df_bansur['TARJETA'] == row_clap['TARJETA']) &
            (abs(df_bansur['MONTO'] - row_clap['MONTO']) <= 0.99) &
            (df_bansur['FECHA_TRANSACCION'] == row_clap['FECHA_TRANSACCION'])
        ]

        # Actualizar el resultado en el array
        result_array[i] = 'SI' if not filtered_rows.empty else 'NO'
        # print("🚀 ~ result_array:", result_array[start_index:end_index])
        # sleep(1)

# Función principal para agregar la columna 'cross_transaction'
def add_cross_transaction_column(df_clap, df_bansur):
    # Crear una nueva columna 'cross_transaction' con valores iniciales 'NO'
    df_clap['cross_transaction'] = 'NO'

    # Crear un array para almacenar los resultados de la comparación
    result_array = [None] * len(df_clap)

    # Dividir el DataFrame en subconjuntos para procesar en paralelo
    num_threads = 16  # Puedes ajustar el número de hilos según tus necesidades
    chunk_size = len(df_clap) // num_threads
    threads = []

    # Crear y ejecutar hilos
    for i in range(num_threads):
        start_index = i * chunk_size
        end_index = start_index + chunk_size if i < num_threads - 1 else len(df_clap)
        thread = threading.Thread(
            target=compare_and_update,
            args=(df_clap, df_bansur, start_index, end_index, result_array)
        )
        threads.append(thread)
        thread.start()

    # Esperar a que todos los hilos terminen
    for thread in threads:
        thread.join()

    # Asignar los resultados al DataFrame
    df_clap['cross_transaction'] = result_array


conn = connect()

if conn == None:
    print("No se pudo conectar a la base de datos")
    exit()


data_clap = load_dataframe(conn, sql_query_clap)
data_bansur = load_dataframe(conn, sql_query_bansur)

if conn.is_connected():
    conn.close()
# Llamar a la función principal
add_cross_transaction_column(data_clap, data_bansur)
add_cross_transaction_column(data_bansur, data_clap)

exercise_5_6(data_clap, 'SI')
exercise_5_6(data_bansur, 'NO')

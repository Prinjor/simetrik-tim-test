# Evaluación

## Metodología
La evaluación se estuvo analizando desde el punto de vista de base de datos por lo cual la importación se realizó directamente desde un ciente base de datos (se uso DBeaver). El archivo de BANSUR se diferencia por algunas columnas, las cuales son:
    * El archivo de CLAP tiene particionado el numero de tarjeta tomando los 6 digitios más significativos como iniciales en un campo y los 4 digitos menos significativos como finales. Mientras que en el archivo BANSUR estos digitos ya están concatenados en un solo campo llamado tarjeta
    * El archivo CLAP tiene el campo FECHA_TRANSACCION en formato datetime, mientras que el archivo de BANSUR tiene la fecha como un entero

La carga de datos también se puede realizar desde un punto de vista de aplicación a la base de datos solventando estas diferencias se pudiera realizar usando un proceso de ETL con python. Esto permite cargar los datos, realizar limpieza y transformaciones para luego cargarlos en ss respectivas tablas. POr motivo de tiempo se realizó desde el punto de vista de base de datos

## Ejercicio 1
1. Escriba el código de SQL que le permite conocer el monto y la cantidad de las transacciones que SIMETRIK considera como conciliables para la base de CLAP
   **Respuesta:**
    Una conciliación del CLAP es aquella en donde el último estado sea pagada. Por lo cual para obtener el monto y la cantidad de transacciones que son conciliables es el siguiente

    ```
        SELECT
            SUM(MONTO) AS total,
            COUNT(*) AS n_transactions
        FROM (
            SELECT
                INICIO6_TARJETA,
                FINAL4_TARJETA,
                MONTO,
                MAX(FECHA_TRANSACCION)
            FROM
                CLAP
            WHERE
                TIPO_TRX = 'PAGADA'
            GROUP BY
                INICIO6_TARJETA, FINAL4_TARJETA, MONTO, FECHA_TRANSACCION
        ) AS CLAPConciliation;
    ```
2. Escriba el código de SQL que le permite conocer el monto y la cantidad de las transacciones que SIMETRIK considera como conciliables para la base de BANSUR
    **Respuesta:**
    Al igual que la respuesta anterior, una conciliación de la base de BANSUR es aquella donde el estado sea igual a PAGO. Por lo cual para obtener el monto y la cantidad de transacciones que son conciliables es el siguiente

    ```
        SELECT
            SUM(MONTO) AS total,
            COUNT(*) AS n_transactions
        FROM (
            SELECT
                TARJETA,
                MONTO,
                MAX(STR_TO_DATE(FECHA_TRANSACCION, '%Y%m%d')) AS date_transaction
            FROM
                BANSUR
            WHERE
                TIPO_TRX = 'PAGO'
            GROUP BY
                TARJETA, MONTO, FECHA_TRANSACCION
        ) AS BANSURConciliation;
    ```

    dado que FECHA_TRANSACCION se colocó como un varchar ya que el archivo trae ese campo distinto a un date entonces se realiza la sentencia SQL convirtiendo ese valor a un tipo de dato DATE

3. ¿Cómo se comparan las cifras de los puntos anteriores respecto de las cifras totales en las fuentes desde un punto de vista del negocio?
   **Respuesta:**
   Desde un punto de vista del negocio, la comparación de las cifras de CLAP y BANSUR podría interpretarse de la siguiente manera:
    * CLAP ha registrado un monto total de 61,038,608, mientras que BANSUR ha registrado 53,362,089. Esto indica que CLAP ha procesado un mayor monto en transacciones conciliables en comparación con BANSUR.
    * CLAP tiene 147,331 transacciones conciliables, mientras que BANSUR tiene 131,049. Esto indica que CLAP tiene un mayor número de transacciones conciliables en comparación con BANSUR.

4. Teniendo en cuenta los criterios de cruce entre ambas bases conciliables, escriba una sentencia de SQL que contenga la información de CLAP y BANSUR; agregue una columna en la que se evidencie si la transacción cruzó o no con su contrapartida y una columna en la que se inserte un ID autoincremental para el control de la conciliación
   **Repuesta:**
   Para este caso se reo una tabla vista que permita mostrar información de los registros de CLAP y de BANSUR, cada uno identificado con una columna agregada llamada 'ORIGIN'. También se agrego la columna 'cross_transaction' el cual indica si la transacción cruzo con su contrapartida. Por lo cual la visa ha quedado de la siguiente forma

   ```
   CREATE OR REPLACE VIEW CROSS_TRANSACTION AS
    SELECT
        'CLAP' AS ORIGEN,
        CONCAT(c.INICIO6_TARJETA, LPAD(c.FINAL4_TARJETA, 4, '0')) as TARJETA,
        TIPO_TRX,
        MONTO,
        DATE(c.FECHA_TRANSACCION) AS FECHA_TRANSACCION,
        CODIGO_AUTORIZACION,
        ID_BANCO AS ID,
        FECHA_RECEPCION_BANCO as FECHA_RECEPCION,
        CASE
            WHEN EXISTS (
                SELECT 1
                FROM BANSUR b
                WHERE 
                    CONCAT(c.INICIO6_TARJETA, LPAD(c.FINAL4_TARJETA, 4, '0')) = b.TARJETA
                    AND ABS(b.MONTO - c.MONTO) <= 0.99
                    AND DATE(STR_TO_DATE(b.FECHA_TRANSACCION, '%Y%m%d')) = DATE(c.FECHA_TRANSACCION)
                    AND b.ID_ADQUIRIENTE = c.ID_BANCO
            ) THEN 'SI'
            ELSE 'NO'
        END AS cross_transaction
    FROM
        CLAP c
    WHERE c.TIPO_TRX = 'PAGADA'
    UNION ALL
    SELECT
        'BANSUR' AS ORIGEN,
        TARJETA,
        TIPO_TRX,
        MONTO,
        DATE(STR_TO_DATE(b.FECHA_TRANSACCION, '%Y%m%d')) AS FECHA_TRANSACCION,
        CODIGO_AUTORIZACION,
        ID_ADQUIRIENTE AS ID,
        FECHA_RECEPCION,
        CASE
            WHEN EXISTS (
                SELECT 1
                FROM CLAP c
                WHERE 
                    CONCAT(c.INICIO6_TARJETA, LPAD(c.FINAL4_TARJETA, 4, '0')) = b.TARJETA
                    AND ABS(b.MONTO - c.MONTO) <= 0.99
                    AND DATE(STR_TO_DATE(b.FECHA_TRANSACCION, '%Y%m%d')) = DATE(c.FECHA_TRANSACCION)
                    AND b.ID_ADQUIRIENTE = c.ID_BANCO
            ) THEN 'SI'
            ELSE 'NO'
        END AS cross_transaction
    FROM
        BANSUR b
    WHERE b.TIPO_TRX = 'PAGO';
   ```

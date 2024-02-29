sql_query_bansur = """
    SELECT
        'BANSUR' AS ORIGEN,
        TARJETA,
        TIPO_TRX,
        MONTO,
        DATE(STR_TO_DATE(b.FECHA_TRANSACCION, '%Y%m%d')) AS FECHA_TRANSACCION,
        CODIGO_AUTORIZACION,
        ID_ADQUIRIENTE AS ID,
        FECHA_RECEPCION
    FROM
        BANSUR b
    WHERE b.TIPO_TRX = 'PAGO';
"""
import pandas as pd

from calidad_datos import main as analisis
from transformacion_datos import etl as tranformar


ERROR = 4


def aproximar_numero(numero: float):
    '''
    Aproxima un número, añadiéndole un margen de error
    '''

    global ERROR

    return int(round(numero) + ERROR)


def extract(semana: int):
    '''
    Recoge los datos del csv correspondiente a esa semana.
    sino, llamará a la anterior ETL
    '''

    nombre_csv = 'csv_procesado_semana' + str(semana) + '.csv'

    # Lo primero será ver si existe el archivo
    try:

        return pd.read_csv(nombre_csv)

    # Sino, nos tocará extraer los datos de la otra ETL
    except FileNotFoundError:

        try:

            dataframe = tranformar(semana)

            return dataframe

        except Exception as ex:

            print(f'Fallo causado por la excepcion {ex}')

            return False


def transform(dataframe: pd.DataFrame):
    '''
    Dividimos el dataframe por semanas
    '''

    if isinstance(dataframe, pd.DataFrame):

        df_ingredientes = dataframe.tail(1)

        df_ingredientes.pop('pizza_type_id')
        df_ingredientes.pop('size')

        df_ingredientes = df_ingredientes.apply(aproximar_numero)

        df_ingredientes = pd.DataFrame(df_ingredientes)
        df_ingredientes = df_ingredientes.rename(columns={0: 'Nº esperado'})

        return df_ingredientes

    else:

        return False


def load(df: pd.DataFrame, nombre_csv: str):
    '''
    Guardaremos los ingredientes a comprar en un csv
    De igual manera, los imprimiremos por pantalla
    '''
    if isinstance(df, pd.DataFrame):

        df.to_csv(nombre_csv)

        print(df)
        print(f'Para más informacion, vaya al nuevo csv creado: {nombre_csv}')

        return df

    else:
        return False


def main(semana=-1):
    '''
    Ejecuta todo el programa en el siguiente orden:
    1) Hace un análisis de los datos > analisis_datos.txt
    2) Extrae los datos de otra ETL, que filtra los datos segun los meses
        indicados, guardando los pedidos (pizzas e ingredientes) en ese periodo
    3) Realiza un prediccion para ese mismo mes, semana por semana
    '''
    analisis()

    while 52 <= semana or semana < 0:
        try:
            semana = int(input('Inserte numero de semana del año: '))

        except ValueError:
            semana = -1

    nombre_csv = 'ingredientes_semana' + str(semana) + '.csv'

    return load(transform(extract(semana)), nombre_csv)


if __name__ == '__main__':

    main()

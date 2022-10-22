import pandas as pd
from typing import List

FICHEROS_CSV = [
            'data_dictionary.csv', 'order_details.csv',
            'orders.csv', 'pizza_types.csv', 'pizzas.csv']


def extract(fichero: str) -> pd.DataFrame:
    '''
    Estraemos fichero.csv como un Dataframe
    '''
    dataframe = pd.read_csv(fichero, sep=',', encoding='cp1252')

    return dataframe


def transform(df: pd.DataFrame, fichero: str) -> str:
    '''
    Cogemos las columnas del dataframe y sus tipos, y
    analizamos cuantos Null/NaN hay en cada una de ellas.
    Devolvemos un string que contenga todos estos datos
    '''

    mensaje = '\nEl fichero ' + fichero
    mensaje += ' contiene las siguientes columnas:\n\n'

    for colum in df.columns:
        tmp = colum
        tmp += '    ' + str(df[colum].dtype)
        tmp += '    Valores Null/Nan = ' + str(df[colum].isnull().sum())
        mensaje += tmp + '\n'

    return mensaje


def load(mensaje: str, fichero: str):
    '''
    Imprimimos mensaje en un fichero
    '''
    file = open(fichero, 'a')
    file.write(mensaje)
    file.close()

    return


def analisis_datos(ficheros: List[str], salida='analisis_datos.txt'):
    '''
    Analiza los datos de ficheros .csv
    Por ello, asumimos que los ficheros contenidos en ficheros
    cumplirán esta condicion
    '''

    file = open(salida, 'w')
    file.close()

    for fichero in ficheros:
        load(transform(extract(fichero), fichero), salida)

    return


def main(ficheros=None):
    '''
    Ejecuta todo el programa.
    Ideal para importarlo desde otros archivos
    '''
    global FICHEROS_CSV

    if not ficheros:

        ficheros = FICHEROS_CSV

    analisis_datos(ficheros)

    return


if __name__ == '__main__':

    main()

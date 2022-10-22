from ctypes.wintypes import SIZE
from logging import exception
import pandas as pd
import re


FICHEROS_CSV = [
            'data_dictionary.csv', 'order_details.csv',
            'orders.csv', 'pizza_types.csv', 'pizzas.csv']


# Establecemos una proporcion de ingredientes para cada tamaño
SIZES = {'S': 1, 'M': 1.25, 'L': 1.5, 'XL': 2, 'XXL': 2.5}

# Creamos todas las semanas del año

# Empezaremos en el año anterior, que como mucho tomará 6 días
ini = pd.Timestamp('26 Dec 2014')

# Finalizaremos el primer dia del año siguiente
fin = pd.Timestamp('1 Jan 2016')

SEMANAS = pd.date_range(start=ini, end=fin, freq='W')

FECHAS = []


def str_to_list(string: str):
    '''
    Funcion auxiliar para corregir los ingredientes del dataframe
    Además, se especifica de que siempre va a haber queso mozarrella,
    y si no hay salsa, se sobreentiende que lleva salsa de tomate
    '''

    if isinstance(string, str):

        salsa = 0
        queso = 0
        if not re.search('Sauce', string):

            salsa = 1
        if not re.search('Mozzarella Cheese', string):

            queso = 1
        string = string.split(', ')

        if salsa:
            string.append('Tomato Sauce')
        if queso:

            string.append('Mozarrella Cheese')

    return string


def filtrar_fechas(fecha: str):
    '''
    Vemos si fecha esta dentro del periodo de tiempo indicado
    '''

    global FECHAS

    fecha = pd.to_datetime(fecha, format='%d/%m/%Y')

    if fecha in FECHAS:

        return fecha

    else:

        return float('nan')


def extract(
            tipos_pizza='pizza_types.csv', pedidos='order_details.csv',
            pizzas='pizzas.csv', fechas='orders.csv'
            ):
    '''
    Abre un archivo con descripciones de la pizzas (tipos_pizza),
    otro archivo con pedidos (pedidos) y otro con los códigos de
    cada pizza (pizzas) como tres dataframes
    '''

    df_pizzas = pd.read_csv(tipos_pizza, sep=',', encoding='cp1252')
    df_pedidos = pd.read_csv(pedidos, sep=',', encoding='cp1252')
    df_tipos = pd.read_csv(pizzas, sep=',', encoding='cp1252')
    df_fechas = pd.read_csv(fechas, sep=',', encoding='cp1252')

    return df_pizzas, df_pedidos, df_tipos, df_fechas


def transform(
            df_ingredientes: pd.DataFrame, df_pedidos: pd.DataFrame,
            df_tipos: pd.DataFrame, df_fechas: pd.DataFrame,
            ):
    '''
    Agruparemos los pedidos segun el número de pizzas por cada tamaño,
    en el periodo de tiempo entre fechas_ini y fechas_fin
    '''

    # Filtramos por las fechas indicadas, y cogemos solo los
    # pedidos correspondientes a esas fechas

    df_fechas['date'] = df_fechas['date'].apply(filtrar_fechas)
    df_fechas = df_fechas.dropna()
    df_pedidos = df_pedidos.merge(
                                df_fechas, how='inner', left_on='order_id',
                                right_on='order_id')

    # Ahora vemos cuantas pizzas de cada tipo se han pedido.
    # Esto lo hacemos agrupando por pizzas, y sumando las columnas,
    # por lo que en la última tendremos el nº total de cada pizza
    df_pizzas = df_pedidos.groupby(df_pedidos['pizza_id']).sum(numeric_only=1)

    # Juntamos ahora con el dataframe que contenia el nombre y
    # tamaño de cada codigo de pizza
    df_pizzas_total = df_pizzas.merge(
                                df_tipos, how='inner',
                                left_on='pizza_id', right_on='pizza_id')

    # Llegados a este punto, ya solo nos interesa el nombre de pizza,
    # su tamaño y la cantidad pedida
    df_pizzas_total = df_pizzas_total[['pizza_type_id', 'size', 'quantity']]
    df_ingredientes = df_ingredientes[['pizza_type_id', 'ingredients']]

    # Ahora añadimos a este dataframe los ingredientes de cada pizza
    df_final = df_pizzas_total.merge(
                                    df_ingredientes, how='inner',
                                    left_on='pizza_type_id',
                                    right_on='pizza_type_id'
                                    )

    # Convertimos los ingredientes de cada pizza en una lista
    df_final['ingredients'] = df_final['ingredients'].apply(str_to_list)

    # Y ahora vamos a crear un dataframe que contenga columnas para
    # cada ingrediente. Además, iremos apuntando cuantos de cada ingrediente
    # vamos necesitando
    ingredientes = []
    dicc = {'pizza_type_id': [], 'size': [], 'Pedidos': []}
    total = {}

    # Lo primero es ver cuantos ingredientes hay en total
    for i in df_final.index:

        for j in range(len(df_final['ingredients'][i])):

            if df_final['ingredients'][i][j] not in ingredientes:

                ingredientes.append(df_final['ingredients'][i][j])
                dicc[df_final['ingredients'][i][j]] = []
                total[df_final['ingredients'][i][j]] = 0

    # Vamos a recorrer el dataframe para ir agregando los ingredientes
    # por separado a cada pizza, y lo añadiremos a dicc

    for i in df_final.index:

        # Recogemos los datos del dataframe que reutilizaremos
        pizza = df_final['pizza_type_id'][i]
        size = df_final['size'][i]
        cantidad = df_final['quantity'][i]

        dicc['pizza_type_id'].append(pizza)
        dicc['size'].append(size)
        dicc['Pedidos'].append(cantidad)

        # Y ahora, por cada ingrediente, hemos de ver si está en la pizza
        for ingrediente in ingredientes:

            # Si está, añadiremos a la lista la proporción del ingrediente
            # segun el tamaño
            if ingrediente in df_final['ingredients'][i]:

                dicc[ingrediente] += [SIZES[size]]
                total[ingrediente] += cantidad * SIZES[size]

            # Y si no está, fijaremos su valor a 0
            elif ingrediente not in df_final['ingredients'][i]:

                dicc[ingrediente] += [0]

    # Ahora añadiremos una última fila: el total
    dicc['pizza_type_id'].append('Total')
    dicc['size'].append('Total')
    dicc['Pedidos'].append(df_final['quantity'].sum())

    for ingrediente in ingredientes:

        dicc[ingrediente] += [total[ingrediente]]

    # Por último, guardaremos todo en un dataframe
    df_final = pd.DataFrame(dicc)

    return df_final


def load(df: pd.DataFrame, nombre_csv: str):
    '''
    Guarda el dataframe como un csv
    '''

    df.to_csv(nombre_csv)

    return df


def etl(semana=1):
    '''
    Filtra los pedidos de la semana del año indicada.
    Devuelve un dataframe con las cantidades de cada pizza pedidas,
    los ingredientes necesarios para cada tamaño de pizza,
    y la última fila es el total de ingredientes empleados
    '''

    global SEMANAS, FECHAS

    # creamos el rango de fechas, según los mes introducido
    principio = SEMANAS[semana - 1]
    fin = principio + pd.Timedelta('7 Day')
    FECHAS = pd.date_range(principio, fin)[:7]

    nombre_csv = 'csv_procesado_semana' + str(semana) + '.csv'
    return load(transform(*extract()), nombre_csv)


if __name__ == '__main__':

    try:
        etl()

    except Exception as e:

        print(f'ERROR: {e}')

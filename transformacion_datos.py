import pandas as pd
import re


FICHEROS_CSV = [
            'data_dictionary.csv', 'order_details.csv',
            'orders.csv', 'pizza_types.csv', 'pizzas.csv']


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


def extract(
            tipos_pizza='pizza_types.csv', pedidos='order_details.csv',
            pizzas='pizzas.csv'
            ):
    '''
    Abre un archivo con descripciones de la pizzas (tipos_pizza),
    otro archivo con pedidos (pedidos) y otro con los códigos de
    cada pizza (pizzas) como tres dataframes
    '''

    df_pizzas = pd.read_csv(tipos_pizza, sep=',', encoding='cp1252')
    df_pedidos = pd.read_csv(pedidos, sep=',', encoding='cp1252')
    df_tipos = pd.read_csv(pizzas, sep=',', encoding='cp1252')

    return df_pizzas, df_pedidos, df_tipos


def transform(
            df_ingredientes: pd.DataFrame, df_pedidos: pd.DataFrame,
            df_tipos: pd.DataFrame
            ):
    '''
    Agruparemos los pedidos segun el número de pizzas por cada tamaño
    '''

    df_pizzas = df_pedidos.groupby(df_pedidos['pizza_id']).sum()
    df_pizzas_total = df_pizzas.merge(
                                df_tipos, how='inner',
                                left_on='pizza_id', right_on='pizza_id')

    df_pizzas_total = df_pizzas_total[['pizza_type_id', 'size', 'quantity']]
    df_ingredientes = df_ingredientes[['pizza_type_id', 'ingredients']]

    df_final = df_pizzas_total.merge(
                                    df_ingredientes, how='inner',
                                    left_on='pizza_type_id',
                                    right_on='pizza_type_id'
                                    )

    df_final['ingredients'] = df_final['ingredients'].apply(str_to_list)

    dicc = {'pizza_type_id': [], 'size': []}
    ingredientes = []
    for i in df_final.index:
        for j in range(len(df_final['ingredients'][i])):
            if df_final['ingredients'][i][j] not in ingredientes:
                ingredientes.append(df_final['ingredients'][i][j])
                dicc[df_final['ingredients'][i][j]] = []
    sizes = {'S': 1, 'M': 1.5, 'L': 2, 'XL': 2.5, 'XXL': 3}
    # print(ingredientes)
    for i in df_final.index:
        pizza = df_final['pizza_type_id'][i]
        size = df_final['size'][i]
        dicc['pizza_type_id'].append(pizza)
        dicc['size'].append(size)
        for ingrediente in ingredientes:
            if ingrediente in df_final['ingredients'][i]:

                dicc[ingrediente] += [sizes[size]]
            elif ingrediente not in df_final['ingredients'][i]:
                dicc[ingrediente] += [0]
    df_final.pop('ingredients')
    df_tmp = pd.DataFrame(dicc)
    df_final = df_final.merge(
                        df_tmp, how='inner', left_on=['pizza_type_id', 'size'],
                        right_on=['pizza_type_id', 'size'])

    return df_final


def load(df: pd.DataFrame, nombre_csv='csv_procesado.csv'):
    '''
    Guarda el dataframe como un csv
    '''

    df.to_csv(nombre_csv)

    return df


def etl():
    return load(transform(*extract()))


if __name__ == '__main__':

    etl()

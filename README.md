# Maven_pizza_predictions
Realizaremos la predicción de los ingredientes necesarios para cocinar todas las pizzas pedidas en una semana
Esta semana se pedirá mediante un input.

Primero de todo analizaremos los tipos de datos (calidad_datos.py).

Luego uniremos todos los csv filtrados por semana.
Para ello, asumimos la cantidad de ingredientes que se emplea por cada tamaño de pizza

Todo esto se realiza ejecutando el archivo python predictions.py

De forma adicional se han creado dos ramas on distintos modos de ejecución.

En rama_docker se incluye un Dockerfile para crear una imagen y un contenedor que aloje todo el proceso

En rama_dagster se da soporte a este orquestador para seguir todo el proceso

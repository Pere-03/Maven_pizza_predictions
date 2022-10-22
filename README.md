# Maven_pizza_predictions
Realizaremos la predicción de los ingredientes necesarios para cocinar todas las pizzas pedidas en una semana
Esta semana se pedirá mediante un input.

Primero de todo analizaremos los tipos de datos (calidad_datos.py).

Luego uniremos todos los csv filtrados por semana.
Para ello, asumimos la cantidad de ingredientes que se emplea por cada tamaño de pizza

Todo esto se realiza ejecutando el archivo python predictions.py

En esta rama podemos crearnos una imagen y un contenedos de docker que ejecute el proceso

Para crear la imagen, hemos de ejecutar "docker build -t nombre_imagen ." en el directorio donde tengamos todos los archivos.

Posteriormente, ejecutamos "docker run -it nombre_imagen"

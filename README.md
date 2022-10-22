# Maven_pizza_predictions
Realizaremos la predicción de los ingredientes necesarios para cocinar todas las pizzas pedidas en una semana
Esta semana se pedirá mediante un input.

Primero de todo analizaremos los tipos de datos (calidad_datos.py).

Luego uniremos todos los csv filtrados por semana.
Para ello, asumimos la cantidad de ingredientes que se emplea por cada tamaño de pizza.

Todo esto se realiza ejecutando el archivo python predictions.py

En esta versión, podemos seguir el proceso gracias a dagster.

Para ello, tenemos que ejecutar el archivo con el comando: "dagit -f predictions.py", ir a la dirección que nos devuelve. 
Una vez alli, vamos a la pestaña de launchpads, y pulsamos el boton de Launch run (abajo a la derecha).

# Ejercicios-mqtt - Entrega 5

El protocolo MQTT es un machine-to-machine (M2M)/Internet of Things protocolo de conectividad. Está diseñado como un esquema ligero y eciente de intercambio de mensajesen modo publish/subscribe, con bajo consumo de recursos y ancho de banda.

## 1-Broker.py
Un componente esencial del sistema es un broker que se encarga de gestionar las publicaciones
y subscripciones de los distintos elementos que se conectan.Para los ejercicios posteriores
utilizaremos el broker en simba.fdi.ucm.es. Los usuarios que se conectan, pueden enviar y recibir mensajes en el topic clients. Tam-
bién podréis crear vuestros propios canales de forma jerárquica a partir de esta raíz. Es decir,
podéis publicar y leer en topics del estilo clients/mi_tema/mi_subtema. Comprueba, en primer lugar, que puedes conectarte al broker y enviar y recibir mensajes.

## 2-Números.py
En el topic numbers se están publicando constantemente números,los hay enteros y los hay reales. Escribe el código de un cliente mqtt que lea este topic y que realice tareas con los números leídos,por ejemplo, separar los enteros y reales,calcular la frecuencia de cada uno de ellos, estudiar propiedades (como ser o no primo) en los enteros, etc.

## 3-Temperaturas.py
En el topic temperature puede haber varios sensores emitiendo valores. Escribe el código de un cliente mqtt que lea los subtopics y que jado un intervalo de tiempo (mejor pequeño, entre 4 y 8 segundos) calcule la temperatura máxima, mínima y media para cada sensor y de todos los sensores.

## 4-Temperatura y humedad.py
Elige un termómetro concreto al que escuchar,es decir, uno de los sensores que publican en temperature. Escribe ahora el código para un cliente mqtt cuya misión es escuchar un termómetro y, si su valor supera una determinada temperatura,K 0, entonces pase a escuchar también en el topic humidity. Si la temperatura baja de K 0 o el valor de humidity sube de K 1 entonces el cliente dejará de escuchar en el topic humidity.

## 5-Temporizador.py
Escribe el código de un cliente mqtt que podamos utilizar como temporizador. El cliente leerá mensajes (elige tú mismo el topic) en los que se indicarán: tiempo de espera, topic y mensaje a publicar una vez pasado el tiempo de espera. El cliente tendrá que encargarse de esperar el tiempo adecuado y luego publicar el mensaje en el topic correspondiente.

## 6-Encadena clientes.py
Diseña e implementa un esquema en elque diferentes clientes mqtt, basados en las soluciones anteriores, encadenen su comportamiento.




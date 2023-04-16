"""
1- Broker:

Un componente esencial del sistema es un broker que se encarga de gestionar las publicaciones
y subscripciones de los distintos elementos que se conectan.Para los ejercicios posteriores
utilizaremos el broker en simba.fdi.ucm.es.
Los usuarios que se conectan, pueden enviar y recibir mensajes en el topic clients. También
podréis crear vuestros propios canales de forma jerárquica a partir de esta raíz. Es decir,
podéis publicar y leer en topics del estilo clients/mi_tema/mi_subtema.
Comprueba, en primer lugar, que puedes conectarte al broker y enviar y recibir mensajes.

"""

from paho.mqtt.client import Client


def on_message(cliente, userdata, mensaje):
    print(f'On_message-> userdata: {userdata}, topic: {mensaje.topic}, qos: {mensaje.qos}, payload: {mensaje.payload}, retain: {mensaje.retain}')

    # El broker publica el mensaje
    cliente.publish('clients/test', mensaje.payload)


def main(broker, topic):
    cliente = Client()
    cliente.on_message= on_message

    # Nos conectamos al servidor cliente
    cliente.connect(broker)
    
    # Nos subscribimos a topic
    cliente.subscribe(topic)

    cliente.loop_forever()


if __name__ == "__main__":
    import sys
    if len(sys.argv) < 3:
        print(f"Usage: {sys.argv[0]} broker topic")
        sys.exit(1)
    broker= sys.argv[1]
    topic= sys.argv[2]
    main(broker, topic)

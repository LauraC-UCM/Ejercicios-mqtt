"""
6- Encadena clientes:

Diseña e implementa un esquema en el que diferentes clientes mqtt, basados en las soluciones
anteriores, encadenen su comportamiento. Por ejemplo, un cliente escucha números y en
algunas circunstancias (recibe un entero primo, por ejemplo), decide poner una alarma en el
temporizador, durante ese tiempo, se pone a escuchar en el topic humidity para calcular el
valor medio. . . Imagina otros encademientos.
"""

from paho.mqtt.client import Client
import paho.mqtt.publish as publish
from multiprocessing import Process
from time import sleep


def work_on_message(message, broker):           
    print('Original message:', message) 
 
    topic, timeout, text= message[2:-1].split(',')
                                   
    print('Process body: ', timeout, topic, text)
    sleep(int(timeout))

    publish.single(topic, payload= text, hostname= broker) 
    print('End process body. The message is: ', message)


def on_message(cliente, userdata, mensaje):
    print(f'On_message-> topic: {mensaje.topic}, payload: {mensaje.payload}')
    
    worker = Process(target= work_on_message, args= (str(mensaje.payload), userdata['broker']))
    worker.start()

    print('End On_message: ', mensaje.payload) 


def on_log(cliente, userdata, level, string):
    print("LOG", userdata, level, string)
 

def on_connect(cliente, userdata, flags, rc):
    print("CONNECT:", userdata, flags, rc)


def main(broker):
    userdata = {'broker': broker} 
 
    cliente= Client(userdata=userdata)
    cliente.enable_logger() 
    cliente.on_message= on_message
    cliente.on_connect= on_connect
    cliente.connect(broker)     
    
    topic= 'clients/timeout' 
    cliente.subscribe(topic)
    
    cliente.loop_forever()


if __name__ == "__main__":
    import sys
    if len(sys.argv) < 2:
        print(f"Usage: {sys.argv[0]} broker")
        sys.exit(1) 
    broker= sys.argv[1]
    main(broker)

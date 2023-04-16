"""
3- Temperaturas:

En el topic temperature puede haber varios sensores emitiendo valores. Escribe el código de
un cliente mqtt que lea los subtopics y que jado un intervalo de tiempo (mejor pequeño,
entre 4 y 8 segundos) calcule la temperatura máxima, mínima y media para cada sensor y de
todos los sensores.
"""

from threading import Lock
from paho.mqtt.client import Client
from time import sleep


def on_message(cliente, data, mensaje):
    print(f'On_message-> topic: {mensaje.topic}, playload: {mensaje.payload}')

    n= len('temperature/')

    mutex= data['lock']

    mutex.acquire()
    try:
        key= mensaje.topic[n:]

        if key in data:
            data['temp'][key].append(mensaje.payload)
        else:
            data['temp'][key]= [mensaje.payload]

    finally:
        mutex.release() 

    print ('On_message', data)


def main(broker):
    data= {'lock':Lock(), 'temp':{}}

    cliente= Client(userdata= data) 
    cliente.on_message= on_message
    cliente.connect(broker)
    cliente.subscribe('temperature/#')

    cliente.loop_start() 
    
    while True:
        sleep(6)
        for key,temp in data['temp'].items():

            # Temperatura media
            mean= sum(map(lambda x: int(x), temp))/len(temp)
            print(f'mean {key}: {mean}')

            # Temperatura maxima
            maximum= max(map(lambda x: int(x),temp))
            print(f'maximum {key}: {maximum}')

            # Temperatura minima
            minimum = min(map(lambda x: int(x),temp))
            print(f'minimum {key}: {minimum}')

            data['temp'][key]=[]


if __name__ == "__main__":
    import sys
    if len(sys.argv) < 2:
        print(f"Usage: {sys.argv[0]} broker")
        sys.exit(1)
    broker= sys.argv[1]
    main(broker) 
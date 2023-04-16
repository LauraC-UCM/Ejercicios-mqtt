"""
2-Números:

En el topic numbers se están publicando constantemente números, los hay enteros y los hay
reales.Escribe el código de un cliente mqtt que lea este topic y que realice tareas con los
números leídos, por ejemplo, separar los enteros y reales, calcular la frecuencia de cada uno
de ellos, estudiar propiedades (como ser o no primo) en los enteros, etc.
"""

from paho.mqtt.client import Client
from multiprocessing import Process, Manager
from time import sleep
from math import floor
import random


def timer(time, data):
    cliente = Client()
    cliente.connect(data['broker'])

    mensaje = f'Timer working. timeout: {time}'
    print(mensaje)

    cliente.publish('Clients/ timerstop', mensaje) 

    sleep(time)

    cliente.publish('Clients/ timerstop', mensaje)
    print('Timer end working')

    cliente.disconnect()
    

def is_prime(n):
    if floor(n) == n:
        i= 2
        while i*i < n and (n % i) != 0:
            i += 1 
        return i*i > n  
    else:
        return False


def on_message(cliente, data, mensaje):
    print(f'On_message-> topic: {mensaje.topic}, playload: {mensaje.payload}')

    try:
        if is_prime(int(mensaje.payload)):
            print("It's prime number")
            worker = Process(target= timer, args= (random.random()*20, data))
            worker.start()
        else:
            print("It isn't prime number")  

    except ValueError as e:
        print(e)
        pass
    
    
def on_log(cliente, userdata, level, string):
    print("LOG", userdata, level, string)

   
def main(broker):
    data= {'client':None, 'broker': broker}

    # Asignamos el cliente
    cliente = Client(client_id= "Combine_numbers", userdata= data)
    data['client']= cliente

    # Iniciamos y nos suscribimos
    cliente.enable_logger()
    cliente.on_message = on_message
    cliente.on_log= on_log
    cliente.connect(broker)
    cliente.subscribe('numbers')
    cliente.loop_forever()
    
    
if __name__ == "__main__":
    import sys
    if len(sys.argv) < 2:
        print(f"Usage: {sys.argv[0]} broker")
        sys.exit(1)
    broker= sys.argv[1]
    main(broker)
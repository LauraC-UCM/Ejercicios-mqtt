"""
4- Temperatura y humedad:

Elige un termómetro concreto al que escuchar, es decir, uno de los sensores que publican
en temperature. Escribe ahora el código para un cliente mqtt cuya misión es escuchar un
termómetro y, si su valor supera una determinada temperatura,K0, entonces pase a escuchar
también en el topic humidity. Si la temperatura baja de K0 o el valor de humidity sube de
K1 entonces el cliente dejará de escuchar en el topic humidity.
"""

from paho.mqtt.client import Client


def on_message(cliente, data, mensaje):
    print (f'On_message-> topic: {mensaje.topic}, playload: {mensaje.payload}, data: {data}')

    if data['status'] == 0:
        
        temp = int(mensaje.payload)     # Escuchamos la temperatura
        if temp > data['K0']:           # Si supera K0, nos suscribimos a humedad
            print(f'Umbral de temperatura superado {temp}, suscribiendo a humidity')
            cliente.subscribe('humidity')
            data['status'] = 1

    elif data['status'] == 1:
        
        if 'temperature' in mensaje.topic:     
            temp = int(mensaje.payload)     # Escuchamos la temperatura
            if temp <= data['K0']:          # Si baja de K0, cancelamos la subscripcion de humedad
                print(f'Temperatura {temp} por debajo de umbral, cancelando suscripcion')
                data['status']=0
                cliente.unsubscribe('humidity')

        elif mensaje.topic == 'humidity':          
            humidity= int(mensaje.payload)      # Escuchamos la humedad
            if humidity > data['K1']:           # Si sube de K1, cancelamos la subscriccion de humedad
                print(f'Umbral de humedad {humidity} superado, cancelando suscripcion')
                cliente.unsubscribe('humidity') 
                data['status'] = 0


def on_log(cliente, data, level, mensaje):
    print(f'LOG: {data}:{mensaje}')


def main(broker): 
    data = {'K0': 20, 'K1': 80, 'status': 0}

    cliente= Client(userdata= data)

    cliente.on_message= on_message  
    cliente.enable_logger()
    cliente.connect(broker)

    cliente.subscribe('temperature/t1')

    cliente.loop_forever() 


if __name__ == "__main__":
    import sys
    if len(sys.argv) < 2: 
        print(f"Usage: {sys.argv[0]} broker")
        sys.exit(1)
    broker= sys.argv[1]
    main(broker)
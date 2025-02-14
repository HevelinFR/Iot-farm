import time
import json
import random
from counterfit_connection import CounterFitConnection
from counterfit_shims_grove.grove_light_sensor_v1_2 import GroveLightSensor
from counterfit_shims_grove.grove_led import GroveLed
import paho.mqtt.client as mqtt
from counterfit_shims_seeed_python_dht import DHT

CounterFitConnection.init('127.0.0.1', 5000)

sensor = DHT("11", 5)

# Configuração do broker MQTT
broker = 'mqtt.eclipseprojects.io'  # Endereço do broker MQTT
port = 1883  # Porta padrão para conexões MQTT
telemetry_topic = "capacitacao-iot/telemetria"  # Tópico de telemetria para envio de dados

# Gera um identificador único para o cliente MQTT
client_id = f'capacitacao-iot-temperature-sensor-client{random.randint(0, 100000)}'
print("Client ID: " + client_id)

# Função de callback chamada ao conectar-se ao broker MQTT
def on_connect(client, userdata, flags, reason_code, properties):
    # Verifica se a conexão foi bem-sucedida ou houve falha
    if reason_code != 0:
        print(f"Falha ao conectar: {reason_code}. 'loop_forever()' tentará reconectar.")
    else:
        print("Conectado ao broker MQTT!")

# Cria o cliente MQTT e configura a função de callback
mqttc = mqtt.Client(mqtt.CallbackAPIVersion.VERSION2, client_id=client_id)
mqttc.on_connect = on_connect  # Associa a função de callback ao evento de conexão
mqttc.user_data_set([])  # Configura dados de usuário, se necessário

# Conecta ao broker MQTT e inicia o loop de comunicação
mqttc.connect(broker, port)
mqttc.loop_start()

# Aguarda um tempo inicial para garantir que a conexão esteja estabelecida
time.sleep(1)

# Loop principal para monitorar e enviar dados de luminosidade via MQTT
while True:
    _, temp = sensor.read()
    
    print(f'Temperature {temp}°C')
    telemetry = json.dumps({'temperature' : temp})

    # Envia os dados de luminosidade ao broker MQTT
    print("Enviando JSON:", telemetry, "no tópico:", telemetry_topic)
    mqttc.publish(telemetry_topic, telemetry)
    time.sleep(5 * 1)
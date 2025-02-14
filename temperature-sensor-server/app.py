import time
import random
import json
import paho.mqtt.client as mqtt
from os import path
import csv
from datetime import datetime

# Configuração do broker MQTT
broker = 'mqtt.eclipseprojects.io'  # Endereço do broker MQTT
port = 1883  # Porta padrão do MQTT
telemetry_topic = "capacitacao-iot/telemetria"  # Tópico de telemetria para envio e recebimento de dados

# Gera um identificador único para o cliente MQTT
client_id = f'capacitacao-iot-temperature-sensor-client-{random.randint(0, 100000)}'
print("Client ID: " + client_id)

# Função de callback chamada quando o cliente se conecta ao broker MQTT
def on_connect(client, userdata, flags, reason_code, properties):
    # Verifica se a conexão foi bem-sucedida ou se houve falha
    if reason_code != 0:
        print(f"Falha ao conectar: {reason_code}. 'loop_forever()' tentará reconectar.")
    else:
        print("Conectado ao broker MQTT!")
        # Inscreve o cliente no tópico de telemetria
        client.subscribe(telemetry_topic)


temperature_file_name = 'temperature.csv'
fieldnames = ['date', 'temperature']

if not path.exists(temperature_file_name):
    with open(temperature_file_name, mode='w') as csv_file:
        writer = csv.DictWriter(csv_file, fieldnames=fieldnames)
        writer.writeheader()

# Função de callback chamada quando uma mensagem é recebida no tópico inscrito
def handle_telemetry(client, userdata, message):
    # Decodifica e exibe o payload da mensagem recebida
    payload = json.loads(message.payload.decode())
    print("Mensagem recebida:", payload)
    with open(temperature_file_name, mode='a') as temperature_file:        
        temperature_writer = csv.DictWriter(temperature_file, fieldnames=fieldnames)
        temperature_writer.writerow({'date' : datetime.now().astimezone().replace(microsecond=0).isoformat(), 'temperature' : payload['temperature']})

# Cria o cliente MQTT e configura os callbacks
mqttc = mqtt.Client(mqtt.CallbackAPIVersion.VERSION2,client_id=client_id)
mqttc.on_connect = on_connect  # Associa a função de callback ao evento de conexão
mqttc.on_message = handle_telemetry  # Associa a função de callback para mensagens recebidas

# Configura dados adicionais de usuário, se necessário
mqttc.user_data_set([])

# Conecta ao broker MQTT
mqttc.connect(broker, port)

# Inicia o loop do cliente MQTT (bloqueante), que gerencia eventos de conexão e mensagens
mqttc.loop_forever()
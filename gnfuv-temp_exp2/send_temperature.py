from kafka import KafkaProducer
import os
import time
import json
import socket
import Adafruit_DHT


KAFKA = os.getenv('KAFKA', '192.168.2.250:9092')
DELTA = float(os.getenv('DELTA', 3))

gpio = 23
sensor = Adafruit_DHT.DHT11

def getTempAndHumidity():
    return Adafruit_DHT.read_retry(sensor, gpio)

def send():
    try:
       humidity, temperature = getTempAndHumidity()
       message = {'device': socket.gethostname(), 'temperature': temperature, 'humidity': humidity}
       print 'sending', message
       producer = KafkaProducer(bootstrap_servers=KAFKA, value_serializer=lambda v: json.dumps(v).encode('utf-8'))
       producer.send('sensor-reading', message)
       producer.flush()
    except Exception as e:
       print 'where r u kafka?', e

try:
    while True:
        send()
        time.sleep(DELTA)
except KeyboardInterrupt:
    pass

from kafka import KafkaProducer
import os
import time
import json
import socket
import Adafruit_DHT
import collections
import numpy


KAFKA = os.getenv('KAFKA', '192.168.2.250:9092')
DELTA = float(os.getenv('DELTA', 1))
EXPERIMENT = float(os.getenv('EXP', 1))
LOGDIR = str(os.getenv('LOGFDIR', '/tmp'))

#variables needed
history_values_temp=collections.deque(maxlen=2)
history_values_hum=collections.deque(maxlen=2)
threshold = 0.4
send='false'

gpio = 23
sensor = Adafruit_DHT.DHT11

def getTempAndHumidity():
    return Adafruit_DHT.read_retry(sensor, gpio)

def savetext(message):
    str_filename = LOGDIR+'/'+str(socket.gethostname())+'_'+str(EXPERIMENT)+'.csv'
    with open(str_filename,'ab') as f:
        numpy.savetxt(f, [str(message)], fmt='%s')

def send():
    try:
       humidity, temperature = getTempAndHumidity()
       delta_temp=abs(temperature-history_values_temp)
       delta_hum= abs(humidity-history_values_hum)
       delta_overall=abs(delta_temp-delta_hum)
       if delta_overall>threshold:
           send='true'
           message = {'device': socket.gethostname(), 'temperature': temperature, 'humidity': humidity, 'experiment': exp, 'send_status': send}
           savetext(message)
           print 'sending', message
           producer = KafkaProducer(bootstrap_servers=KAFKA, value_serializer=lambda v: json.dumps(v).encode('utf-8'))
           producer.send('sensor-reading', message)
           producer.flush()
       else:
           send='false'
           message = {'device': socket.gethostname(), 'temperature': temperature, 'humidity': humidity, 'experiment': exp, 'send_status': send}
           savetext(message)
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

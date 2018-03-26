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
EXPERIMENT = float(os.getenv('EXP', 2))
LOGDIR = str(os.getenv('LOGDIR', '/tmp'))

THRESHOLD_T = float(os.getenv('THRESHOLD_T', 0.1))
THRESHOLD_H = float(os.getenv('THRESHOLD_H', 0.1))

#variables needed
history_values_temp=collections.deque(maxlen=2)
history_values_hum=collections.deque(maxlen=2)
send='false'

gpio = 23
#sensor = Adafruit_DHT.DHT11

def getTempAndHumidity():
    #import random
    #hum= random.randint(1,101)
    #temp= random.randint(1,200) 
    #return hum,temp
    return Adafruit_DHT.read_retry(sensor, gpio)

def savetext(message):
    str_filename = LOGDIR+'/'+str(socket.gethostname())+'_'+str(EXPERIMENT)+'.csv'
    with open(str_filename,'ab') as f:
        numpy.savetxt(f, [str(message)], fmt='%s')

def send():
    try:
       humidity, temperature = getTempAndHumidity()
       history_values_hum.append(humidity)
       history_values_temp.append(temperature)
       delta_temp=abs(temperature-history_values_temp[0])
       delta_hum= abs(humidity-history_values_hum[0])
       if (delta_temp>=THRESHOLD_T) and (delta_hum>=THRESHOLD_H):
           send='true'
           message = {'time': time.time(), 'device': socket.gethostname(), 'temperature': temperature, 'humidity': humidity, 'Delta temperature': delta_temp, 'Delta Humidity': delta_hum, 'Threshold Temperature': THRESHOLD_T, 'Threshold Humidity':THRESHOLD_H,'experiment': EXPERIMENT, 'send_status': send}
           savetext(message)
           #print(message)
           print 'sending', message
           producer = KafkaProducer(bootstrap_servers=KAFKA, value_serializer=lambda v: json.dumps(v).encode('utf-8'))
           producer.send('sensor-reading', message)
           producer.flush()
       else:
           send='false'
           message = {'time': time.time(), 'device': socket.gethostname(), 'temperature': temperature, 'humidity': humidity, 'Delta temperature': delta_temp, 'Delta Humidity': delta_hum, 'Threshold Temperature': THRESHOLD_T, 'Threshold Humidity':THRESHOLD_H, 'experiment': EXPERIMENT, 'send_status': send}
           #print(message)
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

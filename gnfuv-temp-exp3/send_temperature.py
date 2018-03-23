from kafka import KafkaProducer
import os
import time
import json
import socket
import Adafruit_DHT
import pandas as pd
import numpy
import statsmodels.formula.api as sm
import collections


KAFKA = os.getenv('KAFKA', '192.168.2.250:9092')
DELTA = float(os.getenv('DELTA', 1))
EXPERIMENT = float(os.getenv('EXP', 3))
LOGDIR = str(os.getenv('LOGDIR', '/tmp'))

#variables needed
windowsize=600
sliding_window_values = collections.deque(maxlen=windowsize)
threshold = 0.4
send='false'

gpio = 23
sensor = Adafruit_DHT.DHT11

def getTempAndHumidity():
    return Adafruit_DHT.read_retry(sensor, gpio)

def runmodel(sliding_window,values):
    #put sliding window in dataframe
    data = list(sliding_window)
    window_data=pd.DataFrame(data)
    window_data.columns= ['humidity','temperature']
    query='temperature ~ humidity'
    #model fitting
    result = sm.ols(formula=query, data=window_data).fit()
    #model parameters
    param_sensor=list(result.params)
    localpred_sens=param_sensor[0]+values[0]*param_sensor[1]
    actual_value=values[1]
    local_error=abs(localpred_sens-actual_value)
    return local_error,param_sensor

def savetext(message):
    str_filename = LOGDIR+'/'+str(socket.gethostname())+'_'+str(EXPERIMENT)+'.csv'
    with open(str_filename,'ab') as f:
        numpy.savetxt(f, [str(message)], fmt='%s')

def send():
    try:
       humidity, temperature = getTempAndHumidity()
       
       values=[humidity,temperature]
       #appending window
       sliding_window_values.append(values)
       
       if len(sliding_window_values_sens)>= windowsize:
           diff, parameters= runmodel(sliding_window_values,values)
           if diff>=threshold:
               send='true'
               message = {'device': socket.gethostname(), 'temperature': temperature, 'humidity': humidity, 'parameters': parameters, 'experiment': exp, 'send_status': send}
               savetext(message)
               print 'sending', message
               producer = KafkaProducer(bootstrap_servers=KAFKA, value_serializer=lambda v: json.dumps(v).encode('utf-8'))
               producer.send('sensor-reading', message)
               producer.flush()
           else:
               send='false'
               message = {'device': socket.gethostname(), 'temperature': temperature, 'humidity': humidity, 'parameters': parameters, 'experiment': exp, 'send_status': send}
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

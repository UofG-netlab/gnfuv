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
exp= float(os.getenv('EXP', 1))

#variables needed
windowsize=600
sliding_window_values = collections.deque(maxlen=windowsize)
threshold = 0.4
send='false'
param_con=[]


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
    
    angle = np.arccos(np.dot(param_sensor, param_con) / (np.linalg.norm(param_sensor) * np.linalg.norm(param_con)))
    w_diff=angle*(180/np.pi)
    delta_er= np.nan_to_num(w_diff)
    
    return delta_er,param_sensor

def savetext(message):
    str_filename = str(socket.gethostname())+'_'+str(exp)+'.csv'
    f=open(str_filename,'ab')
    row= str(message)
    numpy.savetxt(f, [row], fmt='%s')
    f.close()

def send():
    try:
       humidity, temperature = getTempAndHumidity()
       
       values=[humidity,temperature]
       #appending window
       sliding_window_values.append(values)
       
       if len(sliding_window_values_sens)>= windowsize:
           diff, parameters= runmodel(sliding_window_values,values)
           if diff>=threshold:
               param_con=parameters
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
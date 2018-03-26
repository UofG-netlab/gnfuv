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
from sklearn.metrics import r2_score


KAFKA = os.getenv('KAFKA', '192.168.2.250:9092')
DELTA = float(os.getenv('DELTA', 1))
EXPERIMENT = float(os.getenv('EXP', 6))
LOGDIR = str(os.getenv('LOGDIR', '/tmp'))

#variables needed
parameters_model=collections.deque(maxlen=2)
r2_prev=collections.deque(maxlen=2)
windowsize=30
sliding_window_values = collections.deque(maxlen=windowsize)

threshold = 0.1
difference=collections.deque(maxlen=2)
r2_old_push=collections.deque(maxlen=2)
send='false'

gpio = 23
sensor = Adafruit_DHT.DHT11

def getTempAndHumidity():
    #import random
    #hum= random.randint(1,101)
    #temp= random.randint(1,200) 
    #return hum,temp
    return Adafruit_DHT.read_retry(sensor, gpio)

def runmodel(sliding_window,values):
    #put sliding window in dataframe
    data = list(sliding_window)
    window_data=pd.DataFrame(data)
    window_data.columns= ['humidity','temperature']
    query='temperature ~ humidity'
    
    if len(parameters_model)==0:
        result = sm.ols(formula=query, data=window_data).fit()
        param_sensor=list(result.params)
        parameters_model.append(param_sensor)
        r2_prev.append(result.rsquared)
        r2_old_push.append(0)
        difference.append(0)
        send=True
    else:
        parameters_prev=parameters_model[-1]
        
        y_pred=[]
        for datapoint in range(len(window_data['temperature'])):
            pred=parameters_prev[0]+window_data.loc[datapoint,'humidity']*parameters_prev[1] 
            y_pred.append(pred)
        r2_old=r2_score(window_data['temperature'],y_pred)
        diff=abs(r2_prev[-1]-r2_old)
        r2_old_push.append(r2_old)
        difference.append(diff)
        
        if diff>=threshold:
            result = sm.ols(formula=query, data=window_data).fit()
            param_sensor=list(result.params)
            parameters_model.append(param_sensor)
            r2_prev.append(result.rsquared)
            send=True
        else:
            send=False
    
    return send
    

def savetext(message):
    str_filename = LOGDIR+'/'+str(socket.gethostname())+'_'+str(EXPERIMENT)+'.csv'
    with open(str_filename,'ab') as f:
        numpy.savetxt(f, [str(message)], fmt='%s')

def send():
    try:
       humidity, temperature = getTempAndHumidity()       
       values=[humidity,temperature]
       sliding_window_values.append(values)
       
       if len(sliding_window_values)>= windowsize:
           sendstatus = runmodel(sliding_window_values,values)
           if sendstatus==True:
               send='true'
               message = {'time': time.time(),'device': socket.gethostname(), 'temperature': temperature, 'humidity': humidity, 'parameters': parameters_model[-1], 'R2_current': r2_prev[-1],'R2_old': r2_old_push[-1] ,'difference': difference[-1], 'experiment': EXPERIMENT, 'send_status': send}
               #print(message)
               savetext(message)
               print 'sending', message
               producer = KafkaProducer(bootstrap_servers=KAFKA, value_serializer=lambda v: json.dumps(v).encode('utf-8'))
               producer.send('sensor-reading', message)
               producer.flush()
           else:
               send='false'
               message = {'time': time.time(),'device': socket.gethostname(), 'temperature': temperature, 'humidity': humidity, 'parameters': parameters_model[-1], 'R2_current': r2_prev[-1],'R2_old': r2_old_push[-1] ,'difference': difference[-1], 'experiment': EXPERIMENT, 'send_status': send}
               print(message)
               savetext(message)
               print 'sending', message
               producer = KafkaProducer(bootstrap_servers=KAFKA, value_serializer=lambda v: json.dumps(v).encode('utf-8'))
               producer.send('sensor-reading', message)
               producer.flush()
           
    except Exception as e:
       # print('error')
       print 'where r u kafka?', e

try:
    while True:
        send()
        time.sleep(DELTA)
except KeyboardInterrupt:
    pass

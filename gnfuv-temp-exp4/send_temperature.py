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
EXPERIMENT = float(os.getenv('EXP', 4))
LOGDIR = str(os.getenv('LOGDIR', '/tmp'))

#variables needed
parameters_model=collections.deque(maxlen=2)
windowsize=6
sliding_window_values = collections.deque(maxlen=windowsize)

threshold = collections.deque(maxlen=2)
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
    
    if len(threshold)==0:
        result = sm.ols(formula=query, data=window_data).fit()
        param_sensor=list(result.params)
        ypred= result.predict(window_data['humidity'])
        difference= ypred-window_data['temperature']
        diff=[]
        for val in difference:
            diff.append(val)
        error= numpy.mean(diff)
        threshold.append(error)
        parameters_model.append(param_sensor)
        send=True
    else:
        parameters_prev=parameters_model[-1]
        ypred_new=values[0]*parameters_prev[1]+parameters_prev[0]
        difference_pred=abs(ypred_new-values[1])
        if difference_pred>=threshold[-1]:
            result = sm.ols(formula=query, data=window_data).fit()
            param_sensor=list(result.params)
            #create prediction with new model
            ypred_newmodel=values[0]*param_sensor[1]+param_sensor[0]
            #compare both prediction
            difference_newpred=abs(ypred_new-ypred_newmodel)
            #use old avg error
            if difference_newpred >=threshold[-1]:
                ypred= result.predict(window_data['humidity'])
                difference= ypred-window_data['temperature']
                diff=[]
                for val in difference:
                    diff.append(val)
                error= numpy.mean(diff)
                threshold.append(error)
                parameters_model.append(param_sensor)
                send=True
            else:
                send=False
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
               message = {'device': socket.gethostname(), 'temperature': temperature, 'humidity': humidity, 'parameters': parameters_model[-1], 'experiment': EXPERIMENT, 'send_status': send}
               #print(message)
               savetext(message)
               print 'sending', message
               producer = KafkaProducer(bootstrap_servers=KAFKA, value_serializer=lambda v: json.dumps(v).encode('utf-8'))
               producer.send('sensor-reading', message)
               producer.flush()
           else:
               send='false'
               message = {'device': socket.gethostname(), 'temperature': temperature, 'humidity': humidity, 'parameters': parameters_model[-1], 'experiment': EXPERIMENT, 'send_status': send}
               #print(message)
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
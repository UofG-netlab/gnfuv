from kafka import KafkaProducer
import os
import time
import json
import socket
import Adafruit_DHT
import pandas as pd
import numpy
import statsmodels.api as sm
import collections


KAFKA = os.getenv('KAFKA', '192.168.2.250:9092')
DELTA = float(os.getenv('DELTA', 1))
EXPERIMENT = float(os.getenv('EXP', 4))
LOGDIR = str(os.getenv('LOGDIR', '/tmp'))
WINDOWSIZE = int(os.getenv('WIND', 30))

#variables needed
parameters_model=collections.deque(maxlen=2)
sliding_window_values = collections.deque(maxlen=WINDOWSIZE)
difference_prediction=collections.deque(maxlen=2)
difference_new_pred=collections.deque(maxlen=2)
parameters_model_new=collections.deque(maxlen=2)
model_recalc=collections.deque(maxlen=2)
threshold = collections.deque(maxlen=2)
send='false'

gpio = 23
sensor = Adafruit_DHT.DHT11

def getTempAndHumidity():
    #import random
    #hum= random.randint(1,10)
    #temp= random.randint(1,20) 
    #return hum,temp
    return Adafruit_DHT.read_retry(sensor, gpio)

def runmodel(sliding_window,values):
    #put sliding window in dataframe
    data = list(sliding_window)
    window_data=pd.DataFrame(data)
    window_data.columns= ['humidity','temperature']
    
    if len(parameters_model)==0:
        x = sm.add_constant(window_data['humidity'])
        result = sm.OLS(window_data['temperature'], x).fit()
        param_sensor=list(result.params)
        ypred= result.predict(x)
        difference= ypred-window_data['temperature']
        diff=[]
        for val in difference:
            diff.append(val)
        error= numpy.mean(diff)
        threshold.append(error)
        parameters_model.append(param_sensor)
        parameters_model_new.append(param_sensor)
        model_recalc.append(0)
        difference_prediction.append(0)
        difference_new_pred.append(0)
        send=True
    else:
        parameters_prev=parameters_model[-1]
        ypred_new=values[0]*parameters_prev[1]+parameters_prev[0]
        difference_pred=abs(ypred_new-values[1])
        difference_prediction.append(difference_pred)
        if difference_pred>=threshold[-1]:
            model_recalc.append(1)
            x = sm.add_constant(window_data['humidity'])
            result = sm.OLS(window_data['temperature'], x).fit()
            param_sensor=list(result.params)
            parameters_model_new.append(param_sensor)
            #create prediction with new model
            ypred_newmodel=values[0]*param_sensor[1]+param_sensor[0]
            #compare both prediction
            difference_newpred=abs(ypred_new-ypred_newmodel)
            difference_new_pred.append(difference_newpred)
            #use old avg error
            if difference_newpred >=threshold[-1]:
                ypred= result.predict(x)
                difference= ypred-window_data['temperature']
                diff=[]
                for val in difference:
                    diff.append(val)
                error= numpy.mean(diff)
                threshold.append(error)
                parameters_model.append(param_sensor)
                send=True
            else:
                model_recalc.append(0)
                send=False
        else:
            model_recalc.append(0)
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
       
       if len(sliding_window_values)>= WINDOWSIZE:
           sendstatus = runmodel(sliding_window_values,values)
           if sendstatus==True:
               send='true'
               message = {'time': time.time(),'device': socket.gethostname(), 'temperature': temperature, 'humidity': humidity, 'parameters': parameters_model[-1], 'Windowsize': WINDOWSIZE, 'Threshold': threshold[-1] ,'Model recalculation': model_recalc[-1],'Difference Prediction': difference_prediction[-1], 'Parameters new model': parameters_model_new[-1],'Difference Newprediction': difference_new_pred[-1],'experiment': EXPERIMENT, 'send_status': send}
               #print(message)
               savetext(message)
               print 'sending', message
               producer = KafkaProducer(bootstrap_servers=KAFKA, value_serializer=lambda v: json.dumps(v).encode('utf-8'))
               producer.send('sensor-reading', message)
               producer.flush()
           else:
               send='false'
               message = {'time': time.time(),'device': socket.gethostname(), 'temperature': temperature, 'humidity': humidity, 'parameters': parameters_model[-1], 'Windowsize': WINDOWSIZE, 'Threshold': threshold[-1] ,'Model recalculation': model_recalc[-1],'Difference Prediction': difference_prediction[-1], 'Parameters new model': parameters_model_new[-1],'Difference Newprediction': difference_new_pred[-1], 'experiment': EXPERIMENT, 'send_status': send}
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

'''
GNFUV Leader Container (aggregates sensor readings)
'''

from kafka import KafkaConsumer, KafkaProducer
import os
import time
import json
import threading
import sys
import socket

KAFKA = os.getenv('KAFKA', '192.168.2.250:9092')
DELTA_AVG = float(os.getenv('DELTA_AVG', 10))
DELTA_PRED = float(os.getenv('DELTA_PRED', 10))

# a better datastructure is needed in the future, maybe queues also for threading
history = {}

def get_predictions(history):
    # to be done...
    return {'device': socket.gethostname(), 'temperature': 42, 'light': 42, 'humidity': 42, 'excluded-sensors': ['gnfuv-2123']}

def get_averages(history):
    averages = []
    for sensor in history.iterkeys():
        avg = reduce(lambda x, y: x + y, [d['value'] for d in history[sensor]]) / len(history[sensor])
        averages.append({'device': socket.gethostname(), 'sensor': sensor, 'avg': avg})
    return averages

# run this in a separate thread
def AverageThread():
    global history
    while True:
        try:
            averages = get_averages(history)
            time.sleep(DELTA_AVG)
            producer = KafkaProducer(bootstrap_servers=KAFKA, value_serializer=lambda v: json.dumps(v).encode('utf-8'))
            for average in averages:
                producer.send('leader-data-avg', average)
            producer.flush()
        except Exception as e:
            print 'missing kafka?', e

def PredictionThread():
    global history
    while True:
        try:
            predictions = get_predictions(history)
            time.sleep(DELTA_PRED)
            producer = KafkaProducer(bootstrap_servers=KAFKA, value_serializer=lambda v: json.dumps(v).encode('utf-8'))
            producer.send('leader-data-pred', predictions)
            producer.flush()
        except Exception as e:
            print 'missing kafka?', e

def ConsumerThread():
    global history
    consumer = False
    while consumer == False:
        try:
            consumer = KafkaConsumer('sensor-reading', bootstrap_servers=KAFKA)
            time.sleep(1)
        except Exception as e:
            print 'missing kafka?', e
    for msg in consumer:
        if msg.topic  == 'sensor-reading':
            # we have msg.timestamp for a date/time
            message = json.loads(msg.value)
            for key in message.iterkeys():
                if key != 'device':
                    if key not in history:
                        # dict of sensor readings: {time, device, value)
                        history[key] = [{ 'time': msg.timestamp, 'device': message['device'], 'value': float(message[key])}]
                    else:
                        history[key].append({ 'time': msg.timestamp, 'device': message['device'], 'value': float(message[key])})
                    print 'reading from', message['device'], 'recorded', key, float(message[key])

t1 = threading.Thread(target=PredictionThread)
t2 = threading.Thread(target=AverageThread)
t3 = threading.Thread(target=ConsumerThread)


try:
    t1.daemon=True
    t2.daemon=True
    t3.daemon=True
    t1.start()
    t2.start()
    t3.start()
    while True: time.sleep(0.5)
except (KeyboardInterrupt, SystemExit):
    print 'the party is over'
    sys.exit()


#!/usr/local/bin/python

import RPi.GPIO as GPIO
from kafka import KafkaProducer
import time
import os
import socket
import json


GPIO.setmode(GPIO.BOARD)

KAFKA = os.getenv('KAFKA', '192.168.2.250:9092')
DELTA = float(os.getenv('DELTA', 3))

#define the pin that goes to the circuit
pin_to_circuit = 22

def rc_time (pin_to_circuit):
    count = 0
  
    #Output on the pin for 
    GPIO.setup(pin_to_circuit, GPIO.OUT)
    GPIO.output(pin_to_circuit, GPIO.LOW)

    #Change the pin back to input
    GPIO.setup(pin_to_circuit, GPIO.IN)
  
    #Count until the pin goes high
    while (GPIO.input(pin_to_circuit) == GPIO.LOW):
        count += 1

    return count

try:
    while True:
        try:
           light = rc_time(pin_to_circuit)
           message = {'device': socket.gethostname(), 'light': light}
           print 'sending', light
           producer = KafkaProducer(bootstrap_servers=KAFKA, value_serializer=lambda v: json.dumps(v).encode('utf-8'))
           producer.send('sensor-reading', message)
           producer.flush()
           time.sleep(DELTA)
        except Exception as e:
           print 'where r u kafka?'
except KeyboardInterrupt:
    pass
finally:
    GPIO.cleanup()


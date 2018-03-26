# Python Base Image from https://hub.docker.com/r/arm32v7/python/
FROM arm32v7/python:2.7.13-jessie

# Copy the Python Script to blink LED
COPY send_temperature.py ./

# Intall the rpi.gpio python module
RUN apt-get update
RUN apt-get install -y build-essential python-dev python-numpy python-pandas python-statsmodels python-scipy
RUN git clone https://github.com/adafruit/Adafruit_Python_DHT.git
RUN cd ./Adafruit_Python_DHT && python setup.py install
RUN pip install --no-cache-dir rpi.gpio
RUN pip install kafka-python

# Trigger Python script
CMD ["bash", "-c", "PYTHONPATH=/usr/lib/python2.7/dist-packages/ python ./send_temperature.py"]


# Python Base Image from https://hub.docker.com/r/arm32v7/python/
FROM richardson/gnfuv-temp-exp1:v6 

# Copy the Python Script to blink LED
COPY send_temperature.py ./

# add /usr/lib/pymodules/python2.7/ to PYTHONPATH for statsmodels.api
CMD ["bash", "-c", "PYTHONPATH=/usr/lib/python2.7/dist-packages/:/usr/lib/pymodules/python2.7/ python ./send_temperature.py"]

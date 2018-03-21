import RPi.GPIO as GPIO
import time
import signal


# Configure the PIN # 8
GPIO.setwarnings(False)
GPIO.setmode(GPIO.BOARD)
GPIO.setup(11, GPIO.OUT)
GPIO.setup(13, GPIO.OUT)

# Blink Interval
blink_interval = .5 #Time interval in Seconds

# Blinker Loop

def sigterm_handler(_signo, _stack_frame):
   # Raises SystemExit(0):
   GPIO.cleanup()

try:
   while True:
     GPIO.output(11, True)
     time.sleep(blink_interval)
     GPIO.output(11, False)
     GPIO.output(13, True)
     time.sleep(blink_interval)
     GPIO.output(13, False)
except KeyboardInterrupt:
   GPIO.cleanup()

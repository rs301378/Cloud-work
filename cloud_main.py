'''
@author: Aditya Verma, Rohit Sharma
Date:29/08/2021

'''
import paho.mqtt.client as mqtt
from cloud import *
from datetime import datetime


# main function
if __name__ == "__main__":
    print("Loaded MQTT configuration information.")
    print("Endpoint URL: " + mqtt_url)
    print("Root Cert: " + root_ca)
    print("Device Cert: " + public_crt)
    print("Private Key: " + private_key)
    
    # creating obejct of MQTT client
    client = mqtt.Client()
    
    # methods call
    funInitilise(client)
    subscribeClient(client)
   
    while True:
        now=datetime.now()  
        print(now)  
        dt = {'t_stmp' : int(datetime.timestamp(now)),
              't_utc' : now.strftime("%d/%m/%Y, %H:%M:%S"),
              'x' : "{:.3f}".format(random.uniform(-5,5)),
              'y' :"{:.3f}".format(random.uniform(-5,5)),
              'z' :"{:.3f}".format(random.uniform(-5,5)) 
        }

        # method call
        publishData(client, dt)
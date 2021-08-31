'''
@author: Aditya Verma, Rohit Sharma
Date:28/08/2021

'''
#import paho.mqtt.client as mqtt
import ssl, random
import time
import json
import sys
from datetime import datetime
from internet_check import *

mqtt_url = "a3qvnhplljfvjr-ats.iot.us-west-2.amazonaws.com"
root_ca = 'C:/Users/ROHIT/Desktop/scratch_N/certs/AmazonRootCA1.pem'
public_crt = 'C:/Users/ROHIT/Desktop/scratch_N/certs/gateway-certificate.pem.crt'
private_key = 'C:/Users/ROHIT/Desktop/scratch_N/certs/gateway-private.pem.key'

connflag = False
connbflag = False  #bad connection flag
pubflag= True

def onConnect(client, userdata, flags, response_code):
    global connflag
    global connbflag
    if response_code == 0:
        connflag = True
        print("Connected with status: {0}".format(response_code))
    else:
        print("Bad Connection", response_code)
        #connbflag = True

def onDisconnect(client, userdata, response_code):
    #logging.info("Disconnected reason " + response_code)
    client.connflag = False
    client.connbflag = True

def on_publish(client, userdata, mid):
    print(userdata + " -- " + mid)
    #client.disconnect()

def on_LedControl(client,obj,msg):
    print("LED Control"+msg.topic+"::"+str(msg.payload)+str(type(msg.payload)))
    cmd=json.loads(msg.payload)
    print("MAC:",cmd["MAC"])
    print("CMD:",cmd["CMD"])

def on_Job(client,obj,msg):
    global pubflag
    print(str(msg.payload))
    jobconfig = json.loads(msg.payload.decode('utf-8'))
       
    if 'execution' in jobconfig:
       
        jobid = jobconfig['execution']['jobId']
        operation = jobconfig['execution']['jobDocument']['operation']
        cmd=jobconfig['execution']['jobDocument']['command']
           
        jobstatustopic = "$aws/things/Test_gateway/jobs/"+ jobid + "/update"
       
        if operation=="publish" and cmd=="start":
            pubflag=True
        elif operation=="publish" and cmd=="stop":
            pubflag=False
            
        client.publish(jobstatustopic, json.dumps({ "status" : "SUCCEEDED"}),0)  

def on_General(client,obj,msg):
    print("GENERAL"+msg.topic+"::"+str(msg.payload))


def funInitilise(client):
    client.tls_set(root_ca,
                   certfile = public_crt,
                   keyfile = private_key,
                   cert_reqs = ssl.CERT_REQUIRED,
                   tls_version = ssl.PROTOCOL_TLSv1_2,
                   ciphers = None)

    client.on_connect = onConnect
    #client.on_disconnect = onDisconnect
    #client.on_publish = on_publish

# when the connection attempt failed it show "connection failed message"
    try:
        client.connect(mqtt_url, port = 8883, keepalive=60)
    except:
        print("Connection failed! Please try again...")
        exit(1)

    '''while not connflag and connbflag: #wait in loop
        print("In wait loop...")
        time.sleep(1)
    if connbflag:
        client.loop_stop()
        sys.exit()
    print("In main loop")
    client.loop_stop() #stop loop
    client.disconnect() #disconnect'''

def subscribeClient(client):
    client.message_callback_add("iot/led", on_LedControl)
    client.message_callback_add("iot/general", on_General)
    client.message_callback_add("$aws/things/Test_gateway/jobs/notify-next", on_Job)
    print("Connecting to AWS IoT Broker...")

    #client.subscribe("iot/#",0)
    client.subscribe("$aws/things/Test_gateway/jobs/notify-next",1)
    client.loop_start()

def publishData(client, dt):
    topic="thing/1100/data"
    name="BLE Gateway"
    sys_type="Gateway"
    dev_type="Beacon"
    dev_id="FF:00:00:FF:AA:BB"
    sensor="Accelerometer"
    
    time.sleep(5)
    #print(connflag) 
    if connflag == True and pubflag == True :
        
        t_utc = dt.get('t_utc')
        t_stmp = dt.get('t_stmp')
        x = dt.get('x')
        y = dt.get('y')
        z = dt.get('z')

        msg = {
            "Name": name,
            "Type":sys_type,
            "Device":dev_type,
            "DeviceID":dev_id,
            "TimestampUTC": t_utc,
            "Timestamp": t_stmp,
            "Sensor":sensor,
            "X-axis":x,
            "Y-axis":y,
            "Z-axis":z
        }
        #Internet connection handling along with publishing data
        try:
            requests.head('http://www.google.com/', timeout=3)
            data=json.dumps(msg)
            rt = client.publish(topic,data,qos=1)
            print("Publishing Data...", rt)
            return True

        except requests.ConnectionError as ex:
            print("Connection Lost! Please wait for some time...")
            return False
    else:
        print("waiting...")
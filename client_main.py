import paho.mqtt.client as mqtt
import ssl, random
import time
import datetime
import json

mqtt_url = "a3qvnhplljfvjr-ats.iot.us-west-2.amazonaws.com"
root_ca = 'C:/Users/ROHIT/Desktop/scratch_N/certs/AmazonRootCA1.pem'
public_crt = 'C:/Users/ROHIT/Desktop/scratch_N/certs/gateway-certificate.pem.crt'
private_key = 'C:/Users/ROHIT/Desktop/scratch_N/certs/gateway-private.pem.key'

connflag = False
pubflag= False

def on_connect(client, userdata, flags, response_code):
    global connflag
    connflag = True
    print("Connected with status: {0}".format(response_code))

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



if __name__ == "__main__":
    print("Loaded MQTT configuration information.")
    print("Endpoint URL: " + mqtt_url)
    print("Root Cert: " + root_ca)
    print("Device Cert: " + public_crt)
    print("Private Key: " + private_key)

    client = mqtt.Client()
    client.tls_set(root_ca,
                   certfile = public_crt,
                   keyfile = private_key,
                   cert_reqs = ssl.CERT_REQUIRED,
                   tls_version = ssl.PROTOCOL_TLSv1_2,
                   ciphers = None)

    client.on_connect = on_connect
    #client.on_publish = on_publish
    client.message_callback_add("iot/led", on_LedControl)
    client.message_callback_add("iot/general", on_General)
    client.message_callback_add("$aws/things/Test_gateway/jobs/notify-next", on_Job)
    print("Connecting to AWS IoT Broker...")
    client.connect(mqtt_url, port = 8883, keepalive=60)
    #client.subscribe("iot/#",0)
    client.subscribe("$aws/things/Test_gateway/jobs/notify-next",1)
    client.loop_start()
    topic="thing/1100/data"
    name="BLE Gateway"
    sys_type="Gateway"
    dev_type="Beacon"
    dev_id="FF:00:00:FF:AA:BB"
    sensor="Accelerometer"
    while True:
        time.sleep(5)
        #print(connflag) 
        if connflag == True and pubflag == True :
            now=datetime.now()
            t_stmp=int(datetime.timestamp(now))
            t_utc=now.strftime("%d/%m/%Y, %H:%M:%S")
            x="{:.3f}".format(random.uniform(-5,5))
            y="{:.3f}".format(random.uniform(-5,5))
            z="{:.3f}".format(random.uniform(-5,5))
            
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
            
            data=json.dumps(msg)
            client.publish(topic,data,qos=1)
            #print("Published: " + "%.2f" % ap_measurement )
        else:
            print("waiting...")
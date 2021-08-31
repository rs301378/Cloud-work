import json
from bluepy.btle import Scanner, DefaultDelegate , UUID, Peripheral
from time import sleep
import struct
import sys

def hextodec(value):
    return -(value & 0x8000) | (value & 0x7fff)
    
def LEDconfig(mac,config):
    led_service_uuid = UUID("00001523-1212-efde-1523-785feabcd123")
    led_char_uuid = UUID("00001525-1212-efde-1523-785feabcd123")
    
    #put if statement for scan clash and exception
    if SCAN_STATUS=='Inactive':
        p = Peripheral(mac, "random")
        led_srv=p.getServiceByUUID(led_service_uuid) # Service object for dev
        led_ch=led_srv.getCharacteristics(led_char_uuid)[0] # Charateristic object for dev
        if config=='True':
            led_ch.write(struct.pack('B', 0x01))
        if config=='False':
            led_ch.write(struct.pack('B', 0x00))
        p.disconnect()

def app_node(db):

    FLG=db.wait()
    print("NODE STARTED")
    global BT_STATUS
    while True:
        if N_STATUS=='Active':
            #BLE Section
            bt=subprocess.check_output(['hciconfig'])
            if b'UP' in bt:
                BT_STATUS='Active'
            else:
                BT_STATUS='Inactive'
            if BT_STATUS=='Active' and C_STATUS=='Active':
                SCAN_STATUS='Active'
                lescan=Scanner(0)
                devices=lescan.scan(3)
                for dev in devices:
                    man=dev.getValueText(255)
                    z=man[14:16] + man[12:14]
                    y=man[10:12] + man[8:10]
                    x=man[6:8] + man[4:6]
                    
                    x=hextodec(int(x, 16))*0.00245
				    y=hextodec(int(y, 16))*0.00245
				    z=hextodec(int(z, 16))*0.00245
                    payload={'TYPE':'Beacon'}
                    payload.update({'MAC':dev.addr,'MAC TYPE':dev.addrType,'RSSI':dev.rssi,'Accelerometer(x)':x,'Accelerometer(y)':y,'Accelerometer(z)':z})
                    for (adtype,desc,value) in dev.getScanData():
                        payload.update({desc:value})
                    if not q.full() and C_STATUS=='Active':
                        q.put(payload,block=True,timeout=2)
                SCAN_STATUS='Inactive'


        time.sleep(3)
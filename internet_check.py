'''
@author: Rohit Sharma
Date:30/08/2021

'''
import requests
import time

#function to check if the internet is available or not.
def check_internet():
    while True:
        try:
            requests.get('https://www.google.com/').status_code
            #print("Internet is Connected!")
            continue
            
        except:
            print("Connection Lost! Please wait for some time...")
            time.sleep(5)

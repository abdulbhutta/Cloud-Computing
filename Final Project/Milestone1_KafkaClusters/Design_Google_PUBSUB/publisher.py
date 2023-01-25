import os
import json
import time
import random 
import numpy as np
from google.cloud import pubsub_v1

#Set the environment variable to the credentials file
credentials_path = os.path.join('/Users/abdulbhutta/Desktop/Cloud Computing/Project_Milestone1/Google PUB:SUB/credentials.json')
os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = credentials_path
 
#Create a publisher client
publisher = pubsub_v1.PublisherClient()
topic_path = 'projects/project-milestone1/topics/smartMeter'

#device normal distributions profile used to generate random data
DEVICE_PROFILES = {
	"boston": {'temp': (51.3, 17.7), 'humd': (77.4, 18.7), 'pres': (1.019, 0.091) },
	"denver": {'temp': (49.5, 19.3), 'humd': (33.0, 13.9), 'pres': (1.512, 0.341) },
	"losang": {'temp': (63.9, 11.7), 'humd': (62.8, 21.8), 'pres': (1.215, 0.201) },
}
profileNames=["boston","denver","losang"];

#Key and value for the message
record_key=0
while(True):
  try: 
    profile_name = profileNames[random.randint(0, 2)];
    profile = DEVICE_PROFILES[profile_name]
    # get random values within a normal distribution of the value
    temp = max(0, np.random.normal(profile['temp'][0], profile['temp'][1]))
    humd = max(0, min(np.random.normal(profile['humd'][0], profile['humd'][1]), 100))
    pres = max(0, np.random.normal(profile['pres'][0], profile['pres'][1]))

    # create dictionary
    msg={"time": time.time(), "profile_name": profile_name, "temperature": temp,"humidity": humd, "pressure":pres}

      #randomly eliminate some measurements
    for i in range(3):
        if(random.randrange(0,10)<1):
            choice=random.randrange(0,3)
            if(choice==0):
                msg['temperature']=None
            elif (choice==1):
                msg['humidity']=None
            else:
                msg['pressure']=None

    record_key=record_key+1
    record_value=json.dumps(msg)
    publisher.publish(topic_path, key=str(record_key),data=record_value.encode('utf-8'))
    time.sleep(.5)

  #if keyboard interrupt, break
  except KeyboardInterrupt:
    break

  #Print produced record
  print("Produced record to topic: {} with key {} values: {}".format(topic_path, record_key, msg))
  


              
    



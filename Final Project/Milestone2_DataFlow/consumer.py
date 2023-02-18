import os
from google.cloud import pubsub_v1

#Set the environment variable to the credentials file
credentials_path = os.path.join('/Users/abdulbhutta/Desktop/Cloud Computing/Project_Milestone2/Design/credentials.json')
os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = credentials_path

#Create a subscriber client
subscriber = pubsub_v1.SubscriberClient()
subscription_path = 'projects/projectmilestone2/subscriptions/smartmeter_converted-sub'

#function to be called when a message is received
def callback(message):
  print("Consumed record with key {} and value {}".format(message.attributes.get('key'), message.data))

  #print(f"Received message with attributes: {}")      
  message.ack() #Acknowledge the message and remove from the topic

#The subscriber listening for messages from the topic
streaming_pull_future = subscriber.subscribe(subscription_path, callback=callback)
print(f"Started listening for messages on {subscription_path}")

#Listen for messages
with subscriber:
    try:
        #Listen for messages indefinitely
        streaming_pull_future.result()
    #if error occurs, cancel
    except TimeoutError:
        streaming_pull_future.cancel()
        streaming_pull_future.result()






from confluent_kafka import Producer, KafkaError
import json
import random 

# Read arguments and configurations and initialize
producer_conf = json.load(open('cred.json'))
producer = Producer(producer_conf)
topic= "testTopic2"

# Optional per-message on_delivery handler (triggered by poll() or flush())
# when a message has been successfully delivered or
# permanently failed delivery (after retries).
def acked(err, msg):
    if err is not None:
        print("Failed to deliver message: {}".format(err))
    else:
        print("Produced record to topic {} partition [{}] @ offset {}"
              .format(msg.topic(), msg.partition(), msg.offset()))

for n in range(100):
    print('Enter a key (String):',end='')
    record_key = input()
    print('Enter a value (String):',end='')
    record_value = input()
    print('Enter a partition:',end='')
    partition = int(input())
    
    if(partition<-1):
        break;
        
    print("Producing record: {}\t{}".format(record_key, record_value))
    producer.produce(topic, key=record_key, value=record_value,partition=partition, on_delivery=acked)
    # p.poll() serves delivery reports (on_delivery) from previous produce() calls.
    producer.poll(0)

producer.flush()
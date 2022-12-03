# Sample Kafka
Kafka is an open-source distributed event streaming platform, developed by the Apache Software Foundation.
The project involves setting up a mini-Kafka on our system, complete with the implementation of all basic principles of Kafka.
***
##Features:
*Publisher-Subscriber Architecture
*Multiple Producers (Dynamic)
*Multiple Consumers (Dynmaic)
*3 Brokers, one of which is a Leader, that create are responsible for creating and managing topics
*Multiple topics (Dynamic)
*Mini-Zookeeper to monitor the health of the brokers

***
##Functionalities:
*Can dynamically create producers and consumers
*Makes sure that there are always three brokers running 
*Uses leader algorithm to choose new leader when one of the brokers go down 
*Ensures partitioning 
*Consists of a zookeeper which monitors the heart-beat of the brokers 

***
###HOW TO RUN:
1. run the zookeeper.py on one terminal 
2. run the broker.py in another terminal (if more than 1 broker is required run broker2.py and broker3.py on separate terminals)
3. run the producer.py and consumer.py and two more terminals.

##Technologies used:
Python3: Socket Programming

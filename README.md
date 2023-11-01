# Java kafka producer and consumer
You will need 2 topics before running the application.

**Instructions**: 

If you have downloaded the kafka on your **local** system.

GoTo Folder  C:\kafka_2.12-2.3.0\

- Zookeeper start:
  .\bin\windows\zookeeper-server-start .\config\zookeeper.properties
- Starting kafka server
  .\bin\windows\kafka-server-start .\config\server.properties
- .\bin\windows\kafka-topics --create --topic {topicName} --bootstrap-server {bootStrapserverIpwithPort} --partitions 1 --replication-factor 1

Also make sure to replace the **topic name** and **bootstrap servers** value in java class.
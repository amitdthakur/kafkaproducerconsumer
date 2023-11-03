# Java kafka producer and consumer

You will need two(2) topics before running the application.

This will publish user data to input topic please refer model.User

**Instructions for Windows OS**:

If you have downloaded the kafka on your **Local** system.

Go to downloaded kafka Folder C:\${kafka_version_downloaded}\

- **Zookeeper start:**
  .\bin\windows\zookeeper-server-start .\config\zookeeper.properties
- **Starting kafka server:**
  .\bin\windows\kafka-server-start .\config\server.properties
- **Creation of topic:** .\bin\windows\kafka-topics --create --topic {topicName} --bootstrap-server
  {bootStrapserverIpwithPort} --partitions 1
  --replication-factor 1

Changes:

- Replace the **INPUT_TOPIC**,**TOPIC_NAME** from java class
- Change the values in consumer and producer properties file.

Future enhancements:

1. Apache kafka topics will be created in docker.
2. Move Input topic and output topic to property file.

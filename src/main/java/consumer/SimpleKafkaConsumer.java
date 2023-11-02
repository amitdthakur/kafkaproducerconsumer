package consumer;

import model.User;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import properties.PropertyLoader;

import java.time.Duration;
import java.util.Collections;

public class SimpleKafkaConsumer {
    private static final Logger LOGGER = LoggerFactory.getLogger(SimpleKafkaConsumer.class);
    private static final String TOPIC_NAME = "output-topic";

    public static void main(String[] args) {
        KafkaConsumer<String, User> consumer = getKafkaConsumer();
        //Kafka Consumer subscribes list of topics here.
        consumer.subscribe(Collections.singletonList(TOPIC_NAME));
        //print the topic name
        LOGGER.info("Subscribed to topic: {}", TOPIC_NAME);
        while (true) {
            ConsumerRecords<String, User> records = consumer.poll(Duration.ofMillis(100));
            for (ConsumerRecord<String, User> record : records)// print the offset,key and value for the consumer records.
                LOGGER.info("offset = {}, key = {}, value = {}", record.offset(), record.key(), record.value());
        }
    }

    private static KafkaConsumer<String, User> getKafkaConsumer() {
        return new KafkaConsumer<>(PropertyLoader.getConsumerProperties("kafkaConsumer.properties"));
    }
}
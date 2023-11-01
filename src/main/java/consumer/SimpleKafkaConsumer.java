package consumer;

import deserializer.CustomDeserializer;
import model.User;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.Collections;
import java.util.Properties;

public class SimpleKafkaConsumer {
    private static final Logger LOGGER = LoggerFactory.getLogger(SimpleKafkaConsumer.class);
    private static final String TOPIC_NAME = "output-topic";
    private static final String BOOTSTRAP_SERVERS = "localhost:9092";

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
        Properties properties = new Properties();
        properties.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, BOOTSTRAP_SERVERS);
        properties.put(ConsumerConfig.GROUP_ID_CONFIG, "test");
        properties.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "true");
        properties.put(ConsumerConfig.AUTO_COMMIT_INTERVAL_MS_CONFIG, "1000");
        properties.put(ConsumerConfig.SESSION_TIMEOUT_MS_CONFIG, "30000");
        properties.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, CustomDeserializer.class.getName());
        return new KafkaConsumer<>(properties);
    }
}
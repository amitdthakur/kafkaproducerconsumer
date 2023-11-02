package producer;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import model.User;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.kafka.connect.json.JsonSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;

public class SimpleKafkaProducer {
    private static final Logger LOGGER = LoggerFactory.getLogger(SimpleKafkaProducer.class);
    private static final String INPUT_TOPIC = "input-topic";
    private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();
    private static final String BOOTSTRAP_SERVERS = "localhost:9092";
    private static final int PERIOD = 4000;
    private static final int INITIAL_DELAY = 0;

    public static void main(String[] args) {
        KafkaProducer<String, JsonNode> kafkaProducer = getKafkaProducer();
        Timer timer = new Timer();
        //Run every two second
        timer.scheduleAtFixedRate(getTask(kafkaProducer), INITIAL_DELAY, PERIOD);
    }

    private static TimerTask getTask(KafkaProducer<String, JsonNode> kafkaProducer) {
        return new TimerTask() {
            public void run() {
                List<User> users = getUsers();
                for (User user : users) {
                    kafkaProducer.send(new ProducerRecord<>(INPUT_TOPIC, user.getAtmLocation(), convertToTree(user)));
                }
                LOGGER.info("Produced record count: {}", users.size());
            }
        };
    }

    private static JsonNode convertToTree(User user) {
        return OBJECT_MAPPER.valueToTree(user);
    }

    private static List<User> getUsers() {
        List<User> users = new ArrayList<>();
        users.add(User.builder().name("Amit").atmLocation("Ghansoli").accountNumber(123).amountToWithDraw(10).build());
        users.add(User.builder().name("Akshaya").atmLocation("Bhandup").accountNumber(124).amountToWithDraw(12).build());
        users.add(User.builder().name("Atharva").atmLocation("Ghansoli").accountNumber(125).amountToWithDraw(13).build());
        users.add(User.builder().name("Ninad").atmLocation("Ghansoli").accountNumber(126).amountToWithDraw(14).build());
        return users;
    }

    private static KafkaProducer<String, JsonNode> getKafkaProducer() {
        Properties properties = new Properties();
        //Assign localhost id
        properties.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, BOOTSTRAP_SERVERS);
        //Set acknowledgements for producer requests.
        properties.put(ProducerConfig.ACKS_CONFIG, "all");
        //If the request fails, the producer can automatically retry,
        properties.put(ProducerConfig.RETRIES_CONFIG, 0);
        //Specify buffer size in config
        properties.put(ProducerConfig.BATCH_SIZE_CONFIG, 16384);
        //Reduce the no of requests less than 0
        properties.put(ProducerConfig.LINGER_MS_CONFIG, 1);
        //The buffer.memory controls the total amount of memory available to the producer for buffering.
        properties.put(ProducerConfig.BUFFER_MEMORY_CONFIG, 33554432);
        properties.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, JsonSerializer.class.getName());
        return new KafkaProducer<>(properties);
    }
}
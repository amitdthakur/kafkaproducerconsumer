package task;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import model.User;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.TimerTask;

public class TimerTaskRecordProducer extends TimerTask {
    private static final Logger LOGGER = LoggerFactory.getLogger(TimerTaskRecordProducer.class);
    private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();
    private static final String INPUT_TOPIC = "input-topic";
    KafkaProducer<String, User> kafkaProducer;

    public TimerTaskRecordProducer(KafkaProducer<String, User> kafkaProducer) {
        this.kafkaProducer = kafkaProducer;
    }

    /**
     * The action to be performed by this timer task.
     */
    @Override
    public void run() {
        List<User> users = getUsers();
        for (User user : users) {
            kafkaProducer.send(new ProducerRecord<>(INPUT_TOPIC, user.getAtmLocation(), user));
        }
        LOGGER.info("Produced record count: {}", users.size());
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
}

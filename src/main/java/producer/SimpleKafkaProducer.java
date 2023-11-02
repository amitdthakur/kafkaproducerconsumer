package producer;

import com.fasterxml.jackson.databind.JsonNode;
import model.User;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import properties.PropertyLoader;
import task.TimerTaskRecordProducer;

import java.util.Timer;

public class SimpleKafkaProducer {
    private static final Logger LOGGER = LoggerFactory.getLogger(SimpleKafkaProducer.class);
    private static final int PERIOD_IN_MILLIS = 4000;
    private static final int INITIAL_DELAY = 0;

    public static void main(String[] args) {
        KafkaProducer<String, User> kafkaProducer = getKafkaProducer();
        Timer timer = new Timer();
        LOGGER.info("Producing messages");
        //Run every two second
        timer.scheduleAtFixedRate(new TimerTaskRecordProducer(kafkaProducer), INITIAL_DELAY, PERIOD_IN_MILLIS);
    }

    private static KafkaProducer<String, User> getKafkaProducer() {
        return new KafkaProducer<>(PropertyLoader.getProducerProperties("kafkaProducer.properties"));
    }
}
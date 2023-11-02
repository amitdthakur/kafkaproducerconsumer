package properties;

import org.apache.commons.configuration.ConfigurationException;
import org.apache.commons.configuration.PropertiesConfiguration;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.producer.ProducerConfig;

import java.util.Properties;

public class PropertyLoader {

    public static Properties getProducerProperties(String fileName) {
        PropertiesConfiguration config = new PropertiesConfiguration();
        try {
            config.load(fileName);
        } catch (ConfigurationException e) {
            throw new RuntimeException(e);
        }
        Properties properties = new Properties();
        //Assign localhost id
        properties.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, config.getString("BOOTSTRAP_SERVERS"));
        //Set acknowledgements for producer requests.
        properties.put(ProducerConfig.ACKS_CONFIG, config.getString("ACKS_CONFIG"));
        //If the request fails, the producer can automatically retry,
        properties.put(ProducerConfig.RETRIES_CONFIG, config.getInt("RETRIES_CONFIG"));
        //Specify buffer size in config
        properties.put(ProducerConfig.BATCH_SIZE_CONFIG, config.getInt("BATCH_SIZE_CONFIG"));
        //Reduce the no of requests less than 0
        properties.put(ProducerConfig.LINGER_MS_CONFIG, config.getInt("LINGER_MS_CONFIG"));
        //The buffer.memory controls the total amount of memory available to the producer for buffering.
        properties.put(ProducerConfig.BUFFER_MEMORY_CONFIG, config.getInt("BUFFER_MEMORY_CONFIG"));
        properties.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, config.getString("KEY_SERIALIZER_CLASS_CONFIG"));
        properties.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, config.getString("VALUE_SERIALIZER_CLASS_CONFIG"));
        return properties;
    }


    public static Properties getConsumerProperties(String fileName) {
        PropertiesConfiguration config = new PropertiesConfiguration();
        try {
            config.load(fileName);
        } catch (ConfigurationException e) {
            throw new RuntimeException(e);
        }
        Properties properties = new Properties();
        properties.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, config.getString("BOOTSTRAP_SERVERS"));
        properties.put(ConsumerConfig.GROUP_ID_CONFIG, config.getString("GROUP_ID_CONFIG"));
        properties.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, config.getString("ENABLE_AUTO_COMMIT_CONFIG"));
        properties.put(ConsumerConfig.AUTO_COMMIT_INTERVAL_MS_CONFIG, config.getString("AUTO_COMMIT_INTERVAL_MS_CONFIG"));
        properties.put(ConsumerConfig.SESSION_TIMEOUT_MS_CONFIG, config.getString("SESSION_TIMEOUT_MS_CONFIG"));
        properties.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, config.getString("KEY_DESERIALIZER_CLASS_CONFIG"));
        properties.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, config.getString("VALUE_DESERIALIZER_CLASS_CONFIG"));
        return properties;
    }

}

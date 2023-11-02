package deserializer;

import com.fasterxml.jackson.databind.ObjectMapper;
import model.User;
import org.apache.kafka.common.errors.SerializationException;
import org.apache.kafka.common.serialization.Deserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.charset.StandardCharsets;

public class CustomDeserializer implements Deserializer<User> {
    private static final Logger LOGGER = LoggerFactory.getLogger(CustomDeserializer.class);
    private static final ObjectMapper objectMapper = new ObjectMapper();

    @Override
    public User deserialize(String topic, byte[] data) {
        try {
            if (data == null) {
                LOGGER.info("Null received at deserializing");
                return null;
            }
            return objectMapper.readValue(new String(data, StandardCharsets.UTF_8), User.class);
        } catch (Exception e) {
            throw new SerializationException("Error when deserializing byte[] to MessageDto");
        }
    }
}
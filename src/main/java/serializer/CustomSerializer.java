package serializer;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import model.User;
import org.apache.kafka.common.serialization.Serializer;

public class CustomSerializer implements Serializer<User> {
    private static final ObjectMapper objectMapper = new ObjectMapper();

    /**
     * Convert {@code data} into a byte array.
     *
     * @param topic topic associated with data
     * @param user  typed data
     * @return serialized bytes
     */
    @Override
    public byte[] serialize(String topic, User user) {
        try {
            return objectMapper.writeValueAsBytes(user);
        } catch (JsonProcessingException e) {
            throw new RuntimeException(e);
        }
    }
}

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.protocol.Message;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.security.SecureRandom;
import java.util.Properties;
import java.util.Date;
import java.util.concurrent.ExecutionException;

public class Producer {
    private final KafkaProducer<String, MessageObject> producer;
    private final Logger logger = LoggerFactory.getLogger(Producer.class);
    private final String topic;

    private final String CHAR_LOWER = "abcdefghijklmnopqrstuvwxyz";
    private final String CHAR_UPPER = CHAR_LOWER.toUpperCase();
    private final String DATA = CHAR_LOWER + CHAR_UPPER;
    private final SecureRandom random = new SecureRandom();


    public Producer(String bootstrapServer, String topic) {
        this.topic = topic;
        Properties properties = setProperties(bootstrapServer);
        producer = new KafkaProducer<>(properties);

        logger.info("Producer initialized");
    }

    public void sendMessages(int numberOfMessage) throws ExecutionException, InterruptedException {
        ProducerRecord<String, MessageObject> record;

        for (int i = 0; i < numberOfMessage; i++) {
            MessageObject messageObject = getRandomMessage();
            logger.info(messageObject.toString());

            record = new ProducerRecord<>(this.topic, "message" + i, messageObject);
            producer.send(record, ((recordMetadata, e) -> {
                if (e != null) {
                    logger.error("Error while processing", e);
                }
            })).get();
        }
    }

    void close() {
        logger.info("Closing producer's connection");
        producer.close();
    }

    // fix length = 10 for testing
    private MessageObject getRandomMessage() {
        MessageObject messageObject = new MessageObject();
        messageObject.setMessage(generateRandomString(10));
        messageObject.setTime(new Date().getTime() / 1000L);

        return messageObject;
    }

    private String generateRandomString(int length) {
        if (length < 1) throw new IllegalArgumentException();

        StringBuilder sb = new StringBuilder(length);

        for (int i = 0; i < length; i++) {
            int id = random.nextInt(DATA.length());
            char randomChar = DATA.charAt(id);
            sb.append(randomChar);
        }

        return sb.toString();
    }

    private Properties setProperties(String bootstrapServer) {
        Properties properties = new Properties();
        properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServer);
        properties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, org.springframework.kafka.support.serializer.JsonSerializer.class.getName());

        return properties;
    }
}

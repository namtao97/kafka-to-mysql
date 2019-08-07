package kafka.producerClient;

import common.MessageObject;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;
import java.util.concurrent.ExecutionException;


public class Producer {

    private final KafkaProducer<String, MessageObject> producer;
    private final Logger logger = LoggerFactory.getLogger(Producer.class);
    private final String topic;


    public Producer(String bootstrapServer, String topic) {
        this.topic = topic;
        Properties properties = setProperties(bootstrapServer);
        producer = new KafkaProducer<>(properties);

        logger.info("Producer initialized");
    }


    public void sendMessages(int numberOfMessage) throws ExecutionException, InterruptedException {
        ProducerRecord<String, MessageObject> record;

        for (int i = 0; i < numberOfMessage; i++) {
            MessageObject messageObject = MessageObject.getRandomMessage(10);
            logger.info(messageObject.toString());

            record = new ProducerRecord<>(this.topic, "message" + i, messageObject);
            producer.send(record, ((recordMetadata, e) -> {
                if (e != null) {
                    logger.error("Error while processing", e);
                }
            })).get();
        }
    }


    public void close() {
        logger.info("Closing producer's connection");
        producer.close();
    }


    private Properties setProperties(String bootstrapServer) {
        Properties properties = new Properties();
        properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServer);
        properties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, org.springframework.kafka.support.serializer.JsonSerializer.class.getName());

        return properties;
    }
}

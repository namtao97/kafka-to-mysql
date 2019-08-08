package kafka.producerClient;

import common.MessageObject;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;
import java.util.concurrent.ExecutionException;


public class Producer {

    private final KafkaProducer<String, MessageObject> producer;
    private final Logger logger = LoggerFactory.getLogger(Producer.class);
    private final String topic;


    public Producer(Properties properties) {
        this.topic = properties.getProperty("topic");
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

}

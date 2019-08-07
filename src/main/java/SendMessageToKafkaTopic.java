import kafka.producerClient.Producer;

import java.util.concurrent.ExecutionException;


public class SendMessageToKafkaTopic {

    public static void main(String[] args) throws ExecutionException, InterruptedException {
        String bootstrapServer = "localhost:9092";
        String topic = "test";

        // a simple kafka.producerClient.Producer for testing Consumer
        Producer producer = new Producer(bootstrapServer, topic);
        producer.sendMessages(10); // producer send 10 random messages to kafka topic
        producer.close();
    }
}
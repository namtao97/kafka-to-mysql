import java.util.concurrent.ExecutionException;

public class SendMessageToKafkaTopic {
    public static void main(String[] args) throws ExecutionException, InterruptedException {
        String bootstrapServer = "localhost:9092";
        String topic = "test";

        Producer producer = new Producer(bootstrapServer, topic);
        producer.sendMessages(10); // producer send 5 random messages to kafka topic
        producer.close();
    }
}
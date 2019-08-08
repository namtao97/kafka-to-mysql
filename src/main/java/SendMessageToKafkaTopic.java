import kafka.producerClient.Producer;

import java.io.FileInputStream;
import java.io.IOException;
import java.util.Properties;
import java.util.concurrent.ExecutionException;


public class SendMessageToKafkaTopic {

    public static void main(String[] args) throws IOException, ExecutionException, InterruptedException {
        String configFile = "src\\main\\resources\\Producer.properties";
        Properties properties = new Properties();
        properties.load(new FileInputStream(configFile));

        Producer producer = new Producer(properties);
        producer.sendMessages(10); // producer send 10 random messages to kafka topic
        producer.close();
    }
}
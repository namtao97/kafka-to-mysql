import database.MySQLDatabase;
import kafka.consumerClient.Consumer;
import kafka.consumerClient.ConsumerGroup;
import kafka.consumerClient.ConsumerThread;

import java.io.FileInputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;


public class ReceiveMessageFromKafkaTopic {

    public static void main(String[] args) throws IOException {

        // create database connection poll
        String databaseConfig = "src\\main\\resources\\DB.properties";
        MySQLDatabase database = new MySQLDatabase(databaseConfig);

        String consumerConfig = "src\\main\\resources\\Consumer.properties";
        Properties consumerProperties = new Properties();
        consumerProperties.load(new FileInputStream(consumerConfig));

        // create consumer group
        ConsumerGroup consumerGroup = new ConsumerGroup(consumerProperties, database);

        // create list consumers to the group consumer
        List<Consumer> consumers = new ArrayList<>();
        for (int i = 0; i < Integer.parseInt(consumerProperties.getProperty("number.of.consumers")); i++) {
            consumers.add(new ConsumerThread(consumerProperties, database));
        }

        // assign the list of consumer to the group consumer
        consumerGroup.assignListConsumers(consumers);

        // start run all conumser of group
        consumerGroup.run();
    }
}
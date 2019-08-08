package kafka.consumerClient;

import database.Database;
import org.apache.kafka.clients.consumer.ConsumerRebalanceListener;
import org.apache.kafka.common.TopicPartition;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collection;


public class MyConsumerRebalanceListener implements ConsumerRebalanceListener {

    private Logger logger = LoggerFactory.getLogger(MyConsumerRebalanceListener.class);
    private Database database;
    private Consumer consumer;


    public MyConsumerRebalanceListener(Consumer consumer, Database database) {
        this.consumer = consumer;
        this.database = database;
    }


    @Override
    public void onPartitionsRevoked(Collection<TopicPartition> partitions) {
        try {
            for (TopicPartition partition : partitions) {
                this.database.saveOffset(partition.topic(), partition.partition(), consumer.position(partition));
            }
        } catch (Exception e) {
            logger.error("Database Exception", e);
            this.consumer.shutdown();
        }
    }


    @Override
    public void onPartitionsAssigned(Collection<TopicPartition> partitions) {
        try {
            for (TopicPartition partition : partitions) {
                this.consumer.seek(partition, this.database.getOffset(partition.topic(), partition.partition()));
            }
        } catch (Exception e) {
            logger.error("Database Exception", e);
            this.consumer.shutdown();
        }
    }
}
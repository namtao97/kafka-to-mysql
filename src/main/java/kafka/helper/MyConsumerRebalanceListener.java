package kafka.helper;

import common.MessageObject;
import org.apache.kafka.clients.consumer.ConsumerRebalanceListener;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.TopicPartition;

import java.util.Collection;


public class MyConsumerRebalanceListener implements ConsumerRebalanceListener {

    private OffsetManager offsetManager;
    private KafkaConsumer<String, MessageObject> consumer;


    public MyConsumerRebalanceListener(KafkaConsumer<String, MessageObject> consumer, OffsetManager offsetManager) {
        this.consumer = consumer;
        this.offsetManager = offsetManager;
    }


    @Override
    public void onPartitionsRevoked(Collection<TopicPartition> partitions) {
        for (TopicPartition partition : partitions) {
            offsetManager.saveOffsetInExternalStore(partition.topic(), partition.partition(), consumer.position(partition));
        }
    }


    @Override
    public void onPartitionsAssigned(Collection<TopicPartition> partitions) {
        for (TopicPartition partition : partitions) {
            consumer.seek(partition, offsetManager.readOffsetFromExternalStore(partition.topic(), partition.partition()));
        }
    }
}
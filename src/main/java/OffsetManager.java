import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedWriter;
import java.io.FileWriter;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.stream.Collectors;
import java.util.stream.Stream;


public class OffsetManager {

    private String storagePrefix;
    private Logger logger = LoggerFactory.getLogger(OffsetManager.class.getName());

    public OffsetManager(String storagePrefix) {
        this.storagePrefix = storagePrefix;
    }

    void saveOffsetInExternalStore(String topic, int partition, long offset) {
        try {
            FileWriter writer = new FileWriter(storageName(topic, partition), false);
            BufferedWriter bufferedWriter = new BufferedWriter(writer);
            bufferedWriter.write(offset + "");
            bufferedWriter.flush();
            bufferedWriter.close();
        } catch (Exception e) {
            logger.info("Created success storage-offset file for Topic=" + topic + " and Partition=" + partition);
        }
    }

    long readOffsetFromExternalStore(String topic, int partition) {
        try {
            Stream<String> stream = Files.lines(Paths.get(storageName(topic, partition)));
            return Long.parseLong(stream.collect(Collectors.toList()).get(0)) + 1;
        } catch (Exception e) {
            logger.info("There is no storage offset of Topic=" + topic + " and Partition=" + partition + ". The Offset is set to 0");
        }
        return 0;
    }

    private String storageName(String topic, int partition) {
        return storagePrefix + "\\" + topic + "-" + partition + ".txt";
    }
}
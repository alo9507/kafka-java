import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;

import java.util.Arrays;
import java.util.List;
import java.util.Properties;

public class KafkaConsumer2 {
    public static void main(String[] args) {
        Properties props = new Properties();
        props.put("bootstrap.servers","localhost:9092");
        props.put("key.deserializer","org.apache.kafka.common.serialization.StringDeserializer");
        props.put("value.deserializer","org.apache.kafka.common.serialization.StringDeserializer");
        props.put("group.id", "test");

        KafkaConsumer myConsumer = new KafkaConsumer(props);

        List<String> topics = Arrays.asList("new_topic");

        myConsumer.subscribe(topics);

        try {
            while(true) {
                ConsumerRecords<String, String> records = myConsumer.poll(100);
                for (ConsumerRecord<String, String> record : records) {
                    System.out.println(String.format("Topic %s, Partition: %d, Offset: %d, Key: %s, Value: %s",
                            record.topic(), record.partition(), record.offset(), record.key(), record.value().toUpperCase()
                    ));
                }
            }
        } catch (Exception e) {
            e.getMessage();
        } finally {
            myConsumer.close();
        }
    }
}

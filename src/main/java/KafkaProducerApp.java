import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.util.*;

public class KafkaProducerApp {
    public static void main(String[] args) {
        Properties props = new Properties();
        props.put("bootstrap.servers","localhost:9092");
        props.put("key.serializer","org.apache.kafka.common.serialization.StringSerializer");
        props.put("value.serializer","org.apache.kafka.common.serialization.StringSerializer");

        final KafkaProducer<String, String> myProducer = new KafkaProducer<String, String>(props);

        int delay = 100;
        int interval = 100;
        Timer timer = new Timer();

        final List<String> data = Arrays.asList("msg1","msg2", "msg3");
        final Random rand = new Random();
        final int upperbound = data.size() - 1;

        try {
            timer.scheduleAtFixedRate(new TimerTask() {
                public void run() {
                    myProducer.send(new ProducerRecord<String, String>("new_topic", data.get(rand.nextInt(upperbound))));
                }
            }, delay, interval);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}

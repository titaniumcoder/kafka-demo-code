package demo;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.time.Instant;
import java.util.Properties;

public class Producer {

    private final static String PRODUCER_TOPIC = "kafka-demo-event";

    public void falseTypes(Properties config) {
        var producer = new KafkaProducer<>(config);
        producer.send(new ProducerRecord<>(PRODUCER_TOPIC, "first-key", "value"));
        producer.send(new ProducerRecord<>(PRODUCER_TOPIC, "second-key", "other-value"));
        producer.close();
    }

    public void correctTypes(Properties config) {
        var producer = new KafkaProducer<>(config);
        producer.send(new ProducerRecord<>(PRODUCER_TOPIC, "user-1", new User("username1", "user1@nobody.org")));
        producer.send(new ProducerRecord<>(PRODUCER_TOPIC, "user-1", new User("username2", "user2@nobody.org")));

        producer.send(new ProducerRecord<>(PRODUCER_TOPIC, new Access("username1", "192.33.22.11", Instant.now())));
        producer.send(new ProducerRecord<>(PRODUCER_TOPIC, new Access("username1", "192.33.22.33", Instant.now().minusSeconds(3))));
        producer.send(new ProducerRecord<>(PRODUCER_TOPIC, new Access("username2", "191.33.22.11", Instant.now())));
        producer.send(new ProducerRecord<>(PRODUCER_TOPIC, new Access("username2", "191.33.22.33", Instant.now().minusSeconds(3600))));
        producer.flush();
        producer.close();
    }

    public static void main(String[] args) throws Exception {
        Properties config = ConfigLoader.loadConfig("/bootstrap.properties");
        new Producer().falseTypes(config);
        new Producer().correctTypes(config);
    }
}

package demo;

import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;

import java.time.Duration;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.CountDownLatch;

public class Consumer<T> {

    public void consume(Properties config, String topic, CountDownLatch latch) {
        var consumer = new KafkaConsumer<String, T>(config);
        consumer.subscribe(Set.of(topic));
        consumer.seekToBeginning(consumer.assignment());
        while (true) {
            ConsumerRecords<String, T> records = consumer.poll(Duration.ofMinutes(5));

            if (records.isEmpty()) {
                consumer.unsubscribe();
                consumer.close();
                latch.countDown();
                return;
            }

            records.forEach(record ->
                    System.out.println(record.partition() + "/" + record.offset() + ": " + record.key() + "=" + asString(record.value()))
            );

            consumer.commitAsync();
        }
    }

    private String asString(T t) {
        if (t instanceof User u) {
            return "User = " + u.getUsername() + " (" + u.getEmail() + ")";
        }
        if (t instanceof Access a) {
            return "Access = " + a.getUsername() + " (" + a.getIp() + " at " + a.getDatetime() + ")";
        }

        return "Unknown " + t;
    }

    public static void main(String[] args) throws Exception {
        Properties config = ConfigLoader.loadConfig("/bootstrap.properties");

        final CountDownLatch counter = new CountDownLatch(2);

        new Thread(() -> new Consumer<User>().consume(config, "kafka-demo-user", counter)).start();
        new Thread(() -> new Consumer<User>().consume(config, "kafka-demo-access", counter)).start();

        counter.await();

    }
}

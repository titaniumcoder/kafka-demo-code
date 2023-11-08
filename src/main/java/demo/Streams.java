package demo;

import io.confluent.kafka.streams.serdes.avro.GenericAvroSerde;
import org.apache.avro.Schema;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.kstream.Consumed;

import java.util.HashMap;
import java.util.Properties;
import java.util.concurrent.CountDownLatch;

public class Streams {

    private final static String PRODUCER_TOPIC = "kafka-demo-event";

    public Topology topology(Properties props) {
        StreamsBuilder builder = new StreamsBuilder();
        builder.stream(PRODUCER_TOPIC, Consumed.with(Serdes.String(), getGenericAvroSerde(props)))
                .peek((key, value) -> System.out.println(key + "=" + value.getSchema().getName()))
                .to((key, value, recordContext) -> topic(value.getSchema()));

        return builder.build();
    }

    private String topic(Schema schema) {
        return "kafka-demo-" + schema.getName().toLowerCase();
    }

    private static GenericAvroSerde getGenericAvroSerde(Properties props) {
        GenericAvroSerde serde = new GenericAvroSerde();
        HashMap<String, Object> config = new HashMap<>();
        props.forEach((key1, value1) -> config.put((String) key1, value1));
        serde.configure(config, false);
        return serde;
    }

    public void run(Properties config) {
        try (KafkaStreams streams = new KafkaStreams(topology(config), config)) {

            Runtime.getRuntime().addShutdownHook(new Thread(streams::close));

            final CountDownLatch latch = new CountDownLatch(1);

            streams.setStateListener((newState, oldState) -> {
                if (oldState == KafkaStreams.State.RUNNING && newState != KafkaStreams.State.RUNNING) {
                    latch.countDown();
                }
            });

            streams.start();

            try {
                latch.await();
            } catch (InterruptedException e) {
                throw new RuntimeException(e);
            }
        }
    }

    public static void main(String[] args) throws Exception {
        Properties config = ConfigLoader.loadConfig("/bootstrap.properties");

        new Streams().run(config);


    }
}

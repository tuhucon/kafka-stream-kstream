package com.example.kafkastreamhello;

import com.example.kafkastreamhello.transformer.CountCharacterValueTransformer;
import com.example.kafkastreamhello.transformer.CountValueTranformer;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.kstream.*;
import org.apache.kafka.streams.state.KeyValueBytesStoreSupplier;
import org.apache.kafka.streams.state.KeyValueStore;
import org.apache.kafka.streams.state.StoreBuilder;
import org.apache.kafka.streams.state.Stores;
import org.springframework.boot.CommandLineRunner;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.annotation.Bean;

import java.util.Properties;
import java.util.concurrent.CountDownLatch;

@SpringBootApplication
public class KafkaStreamHelloApplication implements CommandLineRunner {

    private final static String SRC_Topic = "src-topic";

    @Bean("src-topic")
    StoreBuilder<KeyValueStore<String, Integer>> srcTopicStoreBuilder() {
        KeyValueBytesStoreSupplier storeSupplier = Stores.inMemoryKeyValueStore(SRC_Topic);
        StoreBuilder<KeyValueStore<String, Integer>> srcTopicStoreBuilder = Stores.keyValueStoreBuilder(storeSupplier, Serdes.String(), Serdes.Integer());
        return srcTopicStoreBuilder;
    }


    public static void main(String[] args) {
        SpringApplication.run(KafkaStreamHelloApplication.class, args);
    }

    @Override
    public void run(String... args) throws Exception {
        Properties properties = new Properties();
        properties.put(StreamsConfig.APPLICATION_ID_CONFIG, "tuhucon");
        properties.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        properties.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass());
        properties.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass());


        KeyValueBytesStoreSupplier wordCountStoreSupplier = Stores.inMemoryKeyValueStore("tuhucon");
        StoreBuilder<KeyValueStore<String, Integer>> worCountStoreBuilder = Stores.keyValueStoreBuilder(wordCountStoreSupplier, Serdes.String(), Serdes.Integer());

        StreamsBuilder streamsBuilder = new StreamsBuilder();
        streamsBuilder.addStateStore(srcTopicStoreBuilder());
        streamsBuilder.addStateStore(worCountStoreBuilder);


        KStream<String, String> srcStream = streamsBuilder.stream(SRC_Topic, Consumed.with(Serdes.String(), Serdes.String()));

        KStream<String, String> newStream = srcStream.through("src-topic-partition-by-value",
                            Produced.with(Serdes.String(),
                                          Serdes.String(),
                                          (topic, key, value, numPartitions) -> Math.abs(value.hashCode() % numPartitions)));

        newStream
                .transformValues(() -> new CountValueTranformer(SRC_Topic), SRC_Topic);

        newStream
                .transformValues(() -> new CountCharacterValueTransformer("tuhucon"), "tuhucon")
                .foreach((k, v) -> System.out.println(v));

        Topology topo = streamsBuilder.build();
        System.out.println(topo.describe());

        KafkaStreams kafkaStreams = new KafkaStreams(topo, properties);

        CountDownLatch countDownLatch = new CountDownLatch(1);

        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            kafkaStreams.close();
            countDownLatch.countDown();
        }));

        try {
            kafkaStreams.start();
            countDownLatch.await();
        } catch (Exception ex) {
            kafkaStreams.close();
        }
    }
}

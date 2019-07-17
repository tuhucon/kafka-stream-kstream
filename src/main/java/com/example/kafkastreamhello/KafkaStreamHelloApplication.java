package com.example.kafkastreamhello;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.kstream.*;
import org.apache.kafka.streams.processor.ProcessorContext;
import org.apache.kafka.streams.processor.StreamPartitioner;
import org.apache.kafka.streams.state.KeyValueBytesStoreSupplier;
import org.apache.kafka.streams.state.KeyValueStore;
import org.apache.kafka.streams.state.StoreBuilder;
import org.apache.kafka.streams.state.Stores;
import org.apache.kafka.streams.state.internals.InMemoryKeyValueStore;
import org.apache.kafka.streams.state.internals.KeyValueStoreBuilder;
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

    @Data
    @NoArgsConstructor
    @AllArgsConstructor
    public static class ValueWithCount {
        String value;
        int count;
    }


    public static class CountValueTranformer implements ValueTransformer<String, ValueWithCount> {

        private ProcessorContext context;
        private KeyValueStore<String, Integer> store;
        private String storeName;

        public CountValueTranformer(String storeName) {
            this.storeName = storeName;
        }

        @Override
        public void init(ProcessorContext context) {
            this.context = context;
            this.store = (KeyValueStore) context.getStateStore(storeName);
        }

        @Override
        public ValueWithCount transform(String value) {
            System.out.println(Thread.currentThread().getId() + "------------");
            ValueWithCount valueWithCount = new ValueWithCount(value, 1);
            Integer currentCount = store.get(value);
            System.out.println("value = " + value + " - before count:" + currentCount);
            if (currentCount != null) {
                valueWithCount.setCount(currentCount + 1);
            }
            store.put(value, valueWithCount.getCount());
            return valueWithCount;
        }

        @Override
        public void close() {

        }
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

        StreamsBuilder streamsBuilder = new StreamsBuilder();
        streamsBuilder.addStateStore(srcTopicStoreBuilder());


        KStream<String, String> srcStream = streamsBuilder.stream(SRC_Topic, Consumed.with(Serdes.String(), Serdes.String()));

        srcStream.through("src-topic-partition-by-value",
                            Produced.with(Serdes.String(),
                                          Serdes.String(),
                                          (topic, key, value, numPartitions) -> Math.abs(value.hashCode() % numPartitions)))
                .transformValues(() -> new CountValueTranformer(SRC_Topic), SRC_Topic)
                .print(Printed.toSysOut());



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

package com.example.kafkastreamhello;

import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.kstream.*;
import org.springframework.boot.CommandLineRunner;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

import java.util.Properties;
import java.util.concurrent.CountDownLatch;

@SpringBootApplication
public class KafkaStreamHelloApplication implements CommandLineRunner {

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
        KStream<String, String> upperStream = streamsBuilder.stream("src-topic", Consumed.with(Serdes.String(), Serdes.String()))
                .mapValues(s -> {
                    if (s == null) return null;
                    return s.toUpperCase();
                });
        upperStream.foreach((k, v) -> System.out.println(k + ": " + v));
        upperStream.to("des-topic", Produced.with(Serdes.String(), Serdes.String()));
        upperStream.to("des-topic-1", Produced.with(Serdes.String(), Serdes.String()));

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

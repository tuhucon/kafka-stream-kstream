package com.example.kafkastreamhello.transformer;

import org.apache.kafka.streams.kstream.ValueTransformer;
import org.apache.kafka.streams.processor.ProcessorContext;
import org.apache.kafka.streams.state.KeyValueStore;

public class CountCharacterValueTransformer implements ValueTransformer<String, String> {

    private String storeName;
    private ProcessorContext context;
    private KeyValueStore<String, Integer> store;

    public CountCharacterValueTransformer(String storeName) {
        this.storeName = storeName;
    }

    @Override
    public void init(ProcessorContext context) {
        this.context = context;
        this.store = (KeyValueStore<String, Integer>) context.getStateStore(storeName);
    }

    @Override
    public String transform(String value) {
        if (value == null) {
            store.put(value, 0);
        } else {
            store.put(value, value.length());
        }

        Integer length = store.get(value);

        return value + ": " + length;
    }

    @Override
    public void close() {

    }
}

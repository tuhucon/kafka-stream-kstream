package com.example.kafkastreamhello.transformer;

import com.example.kafkastreamhello.data.ValueWithCount;
import org.apache.kafka.streams.kstream.ValueTransformer;
import org.apache.kafka.streams.processor.ProcessorContext;
import org.apache.kafka.streams.state.KeyValueStore;

public class CountValueTranformer implements ValueTransformer<String, ValueWithCount> {

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
        ValueWithCount valueWithCount = new ValueWithCount(value, 1);
        Integer currentCount = store.get(value);
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

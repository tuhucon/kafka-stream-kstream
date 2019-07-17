package com.example.kafkastreamhello;

import org.apache.kafka.streams.state.KeyValueStore;
import org.apache.kafka.streams.state.StoreBuilder;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Bean;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import java.util.HashMap;
import java.util.Map;

@RestController
public class HelloController {
    @Autowired
    StoreBuilder<KeyValueStore<String, Integer>> storeBuilder;

    @GetMapping("/")
    public Map<String, Integer> check(@RequestParam String value) {
        Map<String, Integer> result = new HashMap<>();
        result.put(value, storeBuilder.build().get(value));
        return result;
    }
}

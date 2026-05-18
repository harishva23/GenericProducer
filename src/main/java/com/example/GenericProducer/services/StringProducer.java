package com.example.GenericProducer.services;

import java.util.HashMap;
import java.util.Map;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;

import com.example.GenericProducer.KafkaClient.KafkaProducerClient;
import com.example.GenericProducer.enums.KafkaSerializerTypes;
import com.example.GenericProducer.pojo.Car;
import com.fasterxml.jackson.databind.ObjectMapper;

import jakarta.annotation.PostConstruct;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;

@Service
@RequiredArgsConstructor
@Slf4j
public class StringProducer {

    private final KafkaProducerClient kafkaProducerClient;
    private KafkaProducer<String, String> stringProducer;
    private final ObjectMapper objectMapper = new ObjectMapper();

    @Value("${schema.registry.url}")
    private String schemaRegistryUrl;

    @Value("${kafka.username}")
    private String username;

    @Value("${kafka.password}")
    private String password;

    @Value("${string.input.topic.name}")
    private String stringTopic;

    @PostConstruct
    private void initStringProducer() {
        stringProducer = kafkaProducerClient.getDefaultProducerClientWithoutPartitioner(
                username, password, schemaRegistryUrl,
                KafkaSerializerTypes.STRING_SERIALIZER,
                KafkaSerializerTypes.STRING_SERIALIZER
        );
    }

    public void produceCarString(Car car) {
        try {
            String carJson = objectMapper.writeValueAsString(car);
            Map<String, Object> carKeyMap = new HashMap<>();
            carKeyMap.put("carId", car.getCarId());
            String carKeyJson = objectMapper.writeValueAsString(carKeyMap);
            log.info("Producing string message: {}", carJson);

            ProducerRecord<String, String> producerRecord =
                    new ProducerRecord<>(stringTopic, carKeyJson, carJson);

            stringProducer.send(producerRecord, (metadata, exception) -> {
                if (exception != null) {
                    log.error("Error producing String message", exception);  
                }
            });
        } catch (Exception e) {
            log.error("Error producing String message", e);
        }
    }
}

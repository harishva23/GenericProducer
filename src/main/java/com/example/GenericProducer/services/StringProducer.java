package com.example.GenericProducer.services;

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

    private static final String STRING_TOPIC = "test-car-nested-string";

    private final KafkaProducerClient kafkaProducerClient;
    private KafkaProducer<String, String> stringProducer;

    @Value("${schema.registry.url}")
    private String schemaRegistryUrl;

    @Value("${kafka.username}")
    private String username;

    @Value("${kafka.password}")
    private String password;

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
            ObjectMapper objectMapper = new ObjectMapper();
            String carJson = objectMapper.writeValueAsString(car);
            log.info("Producing string message: {}", carJson);

            ProducerRecord<String, String> producerRecord =
                    new ProducerRecord<>(STRING_TOPIC, car.getCarId(), carJson);

            stringProducer.send(producerRecord, (metadata, exception) -> {
                if (exception == null) {
                    log.info("Produced String message topic={} partition={} offset={}",
                            metadata.topic(), metadata.partition(), metadata.offset());
                } else {
                    log.error("Error producing String message", exception);
                }
            });
        } catch (Exception e) {
            log.error("Error producing String message", e);
        }
    }
}

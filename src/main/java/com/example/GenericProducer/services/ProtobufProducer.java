package com.example.GenericProducer.services;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;

import com.example.GenericProducer.KafkaClient.KafkaProducerClient;
import com.example.GenericProducer.enums.KafkaSerializerTypes;
import com.example.GenericProducer.pojo.Car;
import com.example.GenericProducer.schema.CarProto;
import jakarta.annotation.PostConstruct;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;

@Service
@RequiredArgsConstructor
@Slf4j
public class ProtobufProducer {

    private static final String PROTO_TOPIC = "test-schema-car-protobuf";

    private final KafkaProducerClient kafkaProducerClient;
    private KafkaProducer<String, CarProto.Car> protoProducer;

    @Value("${schema.registry.url}")
    private String schemaRegistryUrl;

    @Value("${kafka.username}")
    private String username;

    @Value("${kafka.password}")
    private String password;

    @PostConstruct
    private void initProtoProducer(){
        protoProducer = kafkaProducerClient.getDefaultProducerClientWithoutPartitioner(
                username, password, schemaRegistryUrl,
                KafkaSerializerTypes.STRING_SERIALIZER,
                KafkaSerializerTypes.PROTOBUF_SERIALIZER
        );
    }

    public void produceCarProto(Car car) {
        try {
            // Direct conversion from POJO to Protobuf - NO JSON overhead!
            CarProto.Car protoCar = CarProto.Car.newBuilder()
                    .setCarId(car.getCarId())
                    .setCarNumber(car.getCarNumber())
                    .setSpeed(car.getSpeed())
                    .setLatitude(car.getLatitude())
                    .setLongitude(car.getLongitude())
                    .build();

            ProducerRecord<String, CarProto.Car> producerRecord = 
                    new ProducerRecord<>(PROTO_TOPIC, car.getCarId(), protoCar);
                    
            protoProducer.send(producerRecord, (metadata, exception) -> {
                if (exception == null) {
                    log.info("Produced Proto message topic={} partition={} offset={}",
                            metadata.topic(), metadata.partition(), metadata.offset());
                } else {
                    log.error("Error producing Proto message", exception);
                }
            });
        } catch (Exception e) {
            log.error("Error producing Proto message", e);
        }
    }
}
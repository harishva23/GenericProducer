package com.example.GenericProducer.services;

import java.io.IOException;
import java.util.Arrays;
import java.util.Optional;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;

import com.example.GenericProducer.KafkaClient.KafkaProducerClient;
import com.example.GenericProducer.KafkaClient.KarapaceClient;
import com.example.GenericProducer.enums.KafkaSerializerTypes;
import com.example.GenericProducer.pojo.Car;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.protobuf.Descriptors;
import com.google.protobuf.DynamicMessage;
import com.google.protobuf.util.JsonFormat;

import io.confluent.kafka.schemaregistry.ParsedSchema;
import io.confluent.kafka.schemaregistry.client.SchemaMetadata;
import io.confluent.kafka.schemaregistry.client.rest.entities.SchemaReference;
import io.confluent.kafka.schemaregistry.client.rest.exceptions.RestClientException;
import io.confluent.kafka.schemaregistry.protobuf.ProtobufSchema;
import jakarta.annotation.PostConstruct;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;

@Service
@RequiredArgsConstructor
@Slf4j
public class ProtobufProducer {

    private final KafkaProducerClient kafkaProducerClient;
    private final KarapaceClient schemaRegistryClient;
    private KafkaProducer<String, Object> protoProducer;
    private SchemaMetadata schemaMetadata;
    private Optional<ParsedSchema> parsedSchema;
    private ProtobufSchema protobufSchema;

    @Value("${schema.registry.url}")
    private String schemaRegistryUrl;

    @Value("${kafka.username}")
    private String username;

    @Value("${kafka.password}")
    private String password;

    @Value("${protobuf.input.topic.name}")
    private String protoTopic;

    private final ObjectMapper objectMapper = new ObjectMapper();

    @PostConstruct
    private void initProtoProducer() throws IOException, RestClientException{
        protoProducer = kafkaProducerClient.getDefaultProducerClientWithoutPartitioner(
                username, password, schemaRegistryUrl,
                KafkaSerializerTypes.STRING_SERIALIZER,
                KafkaSerializerTypes.PROTOBUF_SERIALIZER
        );
        if(protoTopic != null && !protoTopic.isEmpty()) {
            schemaMetadata = schemaRegistryClient.getClient().getLatestSchemaMetadata(protoTopic + "-value");
            parsedSchema = schemaRegistryClient.getClient()
                    .parseSchema("PROTOBUF", schemaMetadata.getSchema(), null);
            protobufSchema = (ProtobufSchema) parsedSchema.get();

        }

    }

    public void produceCarProto(Car car) {
        try {
            
            Descriptors.Descriptor descriptor = protobufSchema.toDescriptor();
            DynamicMessage.Builder builder = DynamicMessage.newBuilder(descriptor);
            
            String carJson = objectMapper.writeValueAsString(car);
            JsonFormat.parser().merge(carJson, builder);
            DynamicMessage protoCar = builder.build();
            ProducerRecord<String, Object> producerRecord = 
                    new ProducerRecord<>(protoTopic, car.getCarId(), protoCar);
                    
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
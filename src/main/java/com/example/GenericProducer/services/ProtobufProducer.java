package com.example.GenericProducer.services;

import java.util.Optional;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;

import com.example.GenericProducer.KafkaClient.KafkaProducerClient;
import com.example.GenericProducer.KafkaClient.KarapaceClient;
import com.example.GenericProducer.enums.KafkaSerializerTypes;
import com.example.GenericProducer.pojo.Car;
import com.example.GenericProducer.util.RandomCarDataGenerator;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.protobuf.Descriptors;
import com.google.protobuf.DynamicMessage;
import com.google.protobuf.InvalidProtocolBufferException;
import com.google.protobuf.util.JsonFormat;

import io.confluent.kafka.schemaregistry.ParsedSchema;
import io.confluent.kafka.schemaregistry.client.SchemaMetadata;
import io.confluent.kafka.schemaregistry.protobuf.ProtobufSchema;
import jakarta.annotation.PostConstruct;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;

@Service
@RequiredArgsConstructor
@Slf4j
public class ProtobufProducer {

    private static final String PROTO_TOPIC = "test-schema-car-protobuf";
    private static final String PROTO_SUBJECT = "test-schema-car-protobuf-value";

    private final KafkaProducerClient kafkaProducerClient;
    private final KarapaceClient schemaRegistryClient;
    private final RandomCarDataGenerator carDataGenerator;
    private KafkaProducer<String, DynamicMessage> avroProducer;

    @Value("${schema.registry.url}")
    private String schemaRegistryUrl;

    @Value("${kafka.username}")
    private String username;

    @Value("${kafka.password}")
    private String password;

    @PostConstruct
    private void initProtoProducer(){
        avroProducer = kafkaProducerClient.getDefaultProducerClientWithoutPartitioner(
                username, password, schemaRegistryUrl,
                KafkaSerializerTypes.STRING_SERIALIZER,
                KafkaSerializerTypes.PROTOBUF_SERIALIZER
        );
    }

    public void produceCarProto(Car car) {
        
        try {
            SchemaMetadata schemaMetadata = schemaRegistryClient.getClient().getLatestSchemaMetadata(PROTO_SUBJECT);
            log.info("Schema Metadata: {}",schemaMetadata.getSchema());
            Optional<ParsedSchema> parsedSchema = schemaRegistryClient.getClient()
                    .parseSchema("PROTOBUF", schemaMetadata.getSchema(), null);

            if (parsedSchema.isEmpty()) {
                log.error("Failed to parse AVRO schema for subject {}", PROTO_SUBJECT);
                return;
            }


            ObjectMapper objectMapper = new ObjectMapper();
            String carString = objectMapper.writeValueAsString(car);

            log.info("Car :{}",car);
            DynamicMessage finalValue = convertJsonToDynamicMessage(carString, parsedSchema.get());
            if(finalValue!=null){
                ProducerRecord<String, DynamicMessage> producerRecord = new ProducerRecord<>(PROTO_TOPIC, car.getCarId(), finalValue);
                avroProducer.send(producerRecord, (metadata, exception) -> {
                    if (exception == null) {
                        log.info("Produced Proto message topic={} partition={} offset={}",
                                metadata.topic(), metadata.partition(), metadata.offset());
                    } else {
                        log.error("Error producing Proto message", exception);
                    }
                });
            }
        } catch (Exception e) {
            log.error("Error producing Proto message", e);
        }
    }

    public static DynamicMessage convertJsonToDynamicMessage(String jsonPayload, ParsedSchema parsedSchema) {
        try {
            
            ProtobufSchema protobufSchema = (ProtobufSchema) parsedSchema;
            Descriptors.Descriptor descriptor = protobufSchema.toDescriptor();
            DynamicMessage.Builder builder = DynamicMessage.newBuilder(descriptor);
            JsonFormat.parser().merge(jsonPayload, builder);
            return builder.build();
        } catch (InvalidProtocolBufferException e) {
            log.error(e.getMessage());
            return null;
        } catch (Exception e) {
            log.error(e.getMessage());
            return null;
        }
    }
}

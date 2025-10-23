package com.example.GenericProducer.services;

import com.example.GenericProducer.KafkaClient.KafkaProducerClient;
import com.example.GenericProducer.KafkaClient.KarapaceClient;
import com.example.GenericProducer.enums.KafkaSerializerTypes;
import com.example.GenericProducer.pojo.Car;
import com.example.GenericProducer.util.RandomCarDataGenerator;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.protobuf.DynamicMessage;
import io.confluent.kafka.schemaregistry.ParsedSchema;
import io.confluent.kafka.schemaregistry.client.SchemaMetadata;
import io.confluent.kafka.schemaregistry.json.JsonSchema;
import io.confluent.kafka.schemaregistry.protobuf.ProtobufSchema;
import jakarta.annotation.PostConstruct;
import io.confluent.kafka.schemaregistry.avro.AvroSchema;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Service;


import java.util.Optional;



@RequiredArgsConstructor
@Slf4j
public class ProducerService {

    private static final String PROTOBUF_TOPIC = "test-schema-car-protobuf";
    private static final String PROTOBUF_SUBJECT = "test-schema-car-protobuf-value";

    private static final String AVRO_TOPIC = "test-schema-car-avro";
    private static final String AVRO_SUBJECT = "test-schema-car-avro-value";

    private static final String JSON_TOPIC = "test-schema-car-json";
    private static final String JSON_SUBJECT = "test-schema-car-json-value";

    private final KafkaProducerClient kafkaProducerClient;
    private final KarapaceClient schemaRegistryClient;
    private final RandomCarDataGenerator carDataGenerator;

    @Value("${schema.registry.url}")
    private String schemaRegistryUrl;

    @Value("${kafka.username}")
    private String username;

    @Value("${kafka.password}")
    private String password;

    private KafkaProducer<String, DynamicMessage> protobufProducer;
    private KafkaProducer<String, GenericRecord> avroProducer;
    private KafkaProducer<String, Object> jsonProducer;
    private final ObjectMapper objectMapper = new ObjectMapper();

    @PostConstruct
    public void init() {
        protobufProducer = kafkaProducerClient.getDefaultProducerClientWithoutPartitioner(
                username, password, schemaRegistryUrl,
                KafkaSerializerTypes.STRING_SERIALIZER,
                KafkaSerializerTypes.PROTOBUF_SERIALIZER
        );

        avroProducer = kafkaProducerClient.getDefaultProducerClientWithoutPartitioner(
                username, password, schemaRegistryUrl,
                KafkaSerializerTypes.STRING_SERIALIZER,
                KafkaSerializerTypes.AVRO_SERIALIZER
        );

        jsonProducer = kafkaProducerClient.getDefaultProducerClientWithoutPartitioner(username
            , password,
             schemaRegistryUrl,
              KafkaSerializerTypes.STRING_SERIALIZER, 
              KafkaSerializerTypes.JSON_SERIALIZER);
    }

    
    public void produceCarToBothFormats() {
        Car car = carDataGenerator.generateRandomCar(); // generate once

        produceCarJson(car);
    }

    private void produceProtobuf(Car car) {
        try {
            SchemaMetadata schemaMetadata = schemaRegistryClient.getClient().getLatestSchemaMetadata(PROTOBUF_SUBJECT);
            Optional<ParsedSchema> parsedSchema = schemaRegistryClient.getClient()
                    .parseSchema("PROTOBUF", schemaMetadata.getSchema(), null);

            if (parsedSchema.isEmpty()) {
                log.error("Failed to parse Protobuf schema for subject {}", PROTOBUF_SUBJECT);
                return;
            }

            ProtobufSchema protobufSchema = (ProtobufSchema) parsedSchema.get();
            DynamicMessage message = buildProtobufMessage(protobufSchema, car);

            ProducerRecord<String, DynamicMessage> producedRecord = new ProducerRecord<>(PROTOBUF_TOPIC, car.getCarId(), message);
            protobufProducer.send(producedRecord, (metadata, exception) -> {
                if (exception == null) {
                    log.info("Produced Protobuf message topic={} partition={} offset={}",
                            metadata.topic(), metadata.partition(), metadata.offset());
                } else {
                    log.error("Error producing Protobuf message", exception);
                }
            });

        } catch (Exception e) {
            log.error("Error producing Protobuf message", e);
        }
    }

    private void produceAvro(Car car) {
        try {
            SchemaMetadata schemaMetadata = schemaRegistryClient.getClient().getLatestSchemaMetadata(AVRO_SUBJECT);
            Optional<ParsedSchema> parsedSchema = schemaRegistryClient.getClient()
                    .parseSchema("AVRO", schemaMetadata.getSchema(), null);

            if (parsedSchema.isEmpty()) {
                log.error("Failed to parse Avro schema for subject {}", AVRO_SUBJECT);
                return;
            }

            AvroSchema avroSchema = (AvroSchema) parsedSchema.get();
            Schema schema = avroSchema.rawSchema();

            GenericRecord producedRecord = new GenericData.Record(schema);
            producedRecord.put("carId", car.getCarId());
            producedRecord.put("carNumber", car.getCarNumber());
            producedRecord.put("speed", car.getSpeed());
            producedRecord.put("latitude", car.getLatitude());
            producedRecord.put("longitude", car.getLongitude());

            ProducerRecord<String, GenericRecord> producerRecord = new ProducerRecord<>(AVRO_TOPIC, car.getCarId(), producedRecord);
            avroProducer.send(producerRecord, (metadata, exception) -> {
                if (exception == null) {
                    log.info("Produced Avro message topic={} partition={} offset={}",
                            metadata.topic(), metadata.partition(), metadata.offset());
                } else {
                    log.error("Error producing Avro message", exception);
                }
            });

        } catch (Exception e) {
            log.error("Error producing Avro message", e);
        }
    }

    public void produceCarJson(Car car) {
        
        try {
            SchemaMetadata schemaMetadata = schemaRegistryClient.getClient().getLatestSchemaMetadata(JSON_SUBJECT);
            Optional<ParsedSchema> parsedSchema = schemaRegistryClient.getClient()
                    .parseSchema("JSON", schemaMetadata.getSchema(), null);

            if (parsedSchema.isEmpty()) {
                log.error("Failed to parse JSON schema for subject {}", JSON_SUBJECT);
                return;
            }

            JsonSchema jsonSchema = (JsonSchema) parsedSchema.get();

            String carJson = objectMapper.writeValueAsString(car);
            
            JsonNode jsonNode = objectMapper.readTree(carJson);
            //jsonSchema.validate(jsonNode);
            log.info("JsonSchema: ",jsonSchema.canonicalString());
            ProducerRecord<String, Object> producerRecord = new ProducerRecord<>(
                    JSON_TOPIC, car.getCarId(), jsonNode
            );

            jsonProducer.send(producerRecord, (metadata, exception) -> {
                if (exception == null) {
                    log.info("Produced JSON message topic={} partition={} offset={}",
                            metadata.topic(), metadata.partition(), metadata.offset());
                } else {
                    log.error("Error producing JSON message", exception);
                }
            });

        } catch (Exception e) {
            log.error("Error producing JSON message", e);
        }
    }

    private DynamicMessage buildProtobufMessage(ProtobufSchema schema, Car car) {
        DynamicMessage.Builder builder = schema.newMessageBuilder();
        builder.setField(schema.toDescriptor().findFieldByName("carId"), car.getCarId());
        builder.setField(schema.toDescriptor().findFieldByName("carNumber"), car.getCarNumber());
        builder.setField(schema.toDescriptor().findFieldByName("speed"), car.getSpeed());
        builder.setField(schema.toDescriptor().findFieldByName("latitude"), car.getLatitude());
        builder.setField(schema.toDescriptor().findFieldByName("longitude"), car.getLongitude());
        return builder.build();
    }
}

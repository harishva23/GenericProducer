package com.example.GenericProducer.services;

import java.io.IOException;
import java.util.Optional;

import org.apache.avro.AvroTypeException;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.Decoder;
import org.apache.avro.io.DecoderFactory;
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

import io.confluent.kafka.schemaregistry.ParsedSchema;
import io.confluent.kafka.schemaregistry.client.SchemaMetadata;
import jakarta.annotation.PostConstruct;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;


@Service
@RequiredArgsConstructor
@Slf4j
public class AvroProducer {

    private static final String AVRO_TOPIC = "test-schema-car-avro";
    private static final String AVRO_SUBJECT = "test-schema-car-avro-value";

    private final KafkaProducerClient kafkaProducerClient;
    private final KarapaceClient schemaRegistryClient;
    private final RandomCarDataGenerator carDataGenerator;
    private KafkaProducer<String, GenericRecord> avroProducer;

    @Value("${schema.registry.url}")
    private String schemaRegistryUrl;

    @Value("${kafka.username}")
    private String username;

    @Value("${kafka.password}")
    private String password;

    @PostConstruct
    private void initAvroProducer(){
        avroProducer = kafkaProducerClient.getDefaultProducerClientWithoutPartitioner(
                username, password, schemaRegistryUrl,
                KafkaSerializerTypes.STRING_SERIALIZER,
                KafkaSerializerTypes.AVRO_SERIALIZER
        );
    }

    public void produceCarAvro(Car car) throws AvroTypeException {
        
        try {
            SchemaMetadata schemaMetadata = schemaRegistryClient.getClient().getLatestSchemaMetadata(AVRO_SUBJECT);
            log.info("Schema Metadata: {}",schemaMetadata.getSchema());
            Optional<ParsedSchema> parsedSchema = schemaRegistryClient.getClient()
                    .parseSchema("AVRO", schemaMetadata.getSchema(), null);

            if (parsedSchema.isEmpty()) {
                log.error("Failed to parse AVRO schema for subject {}", AVRO_SUBJECT);
                return;
            }


            ObjectMapper objectMapper = new ObjectMapper();
            String carString = objectMapper.writeValueAsString(car);

            log.info("Car :{}",car);
            GenericRecord finalValue = convertJsonToGenericRecord(carString, parsedSchema.get());
            if(finalValue!=null){
                ProducerRecord<String, GenericRecord> producerRecord = new ProducerRecord<>(AVRO_TOPIC, car.getCarId(), finalValue);
                avroProducer.send(producerRecord, (metadata, exception) -> {
                    if (exception == null) {
                        log.info("Produced Avro message topic={} partition={} offset={}",
                                metadata.topic(), metadata.partition(), metadata.offset());
                    } else {
                        log.error("Error producing Avro message", exception);
                    }
                });
            }
        } catch (Exception e) {
            log.error("Error producing JSON message", e);
        }
    }

    public static GenericRecord convertJsonToGenericRecord(String jsonPayload, ParsedSchema schema) {
        try {
            Schema avroSchema = new Schema.Parser().parse(schema.canonicalString());
            GenericDatumReader<GenericRecord> reader = new GenericDatumReader<>(avroSchema);
            Decoder decoder = DecoderFactory.get().jsonDecoder(avroSchema, jsonPayload);
            return reader.read(null, decoder);
        } catch (AvroTypeException e) {
            log.error("Schema validation failed: {}", e.getMessage());
        } catch (IOException e) {
            log.error(e.toString());
        } catch (Exception e) {
           log.error(e.getMessage());
        } 
        return null;
    }
}

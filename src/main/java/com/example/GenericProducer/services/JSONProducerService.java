package com.example.GenericProducer.services;

import java.util.HashMap;
import java.util.Map;
import java.util.Optional;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Service;

import com.example.GenericProducer.KafkaClient.KafkaProducerClient;
import com.example.GenericProducer.KafkaClient.KarapaceClient;
import com.example.GenericProducer.enums.KafkaSerializerTypes;
import com.example.GenericProducer.pojo.Car;
import com.example.GenericProducer.util.RandomCarDataGenerator;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;

import io.confluent.kafka.schemaregistry.ParsedSchema;
import io.confluent.kafka.schemaregistry.client.SchemaMetadata;
import io.confluent.kafka.schemaregistry.json.JsonSchema;
import io.confluent.kafka.schemaregistry.json.JsonSchemaUtils;
import io.confluent.kafka.serializers.AbstractKafkaSchemaSerDeConfig;
import io.confluent.kafka.serializers.json.KafkaJsonSchemaSerializer;
import io.confluent.kafka.serializers.json.KafkaJsonSchemaSerializerConfig;
import jakarta.annotation.PostConstruct;
import lombok.Cleanup;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;

@Service
@RequiredArgsConstructor
@Slf4j
public class JSONProducerService {

    private static final String JSON_TOPIC = "test-schema-car-json";
    private static final String JSON_SUBJECT = "test-schema-car-json-value";

    private final KafkaProducerClient kafkaProducerClient;
    private final KarapaceClient schemaRegistryClient;
    
    private KafkaProducer<String, Object> jsonProducer;

    @Value("${schema.registry.url}")
    private String schemaRegistryUrl;

    @Value("${kafka.username}")
    private String username;

    @Value("${kafka.password}")
    private String password;

    @PostConstruct
    private void initJSONProducer(){
        jsonProducer =  kafkaProducerClient.getDefaultProducerClientWithoutPartitioner(username
            , password,
             schemaRegistryUrl,
              KafkaSerializerTypes.STRING_SERIALIZER, 
              KafkaSerializerTypes.BYTE_SERIALIZER);
    }

    public void produceCarJson(Car car) {
        
        try {
            SchemaMetadata schemaMetadata = schemaRegistryClient.getClient().getLatestSchemaMetadata(JSON_SUBJECT);
            log.info("Schema Metadata: {}",schemaMetadata.getSchema());
            Optional<ParsedSchema> parsedSchema = schemaRegistryClient.getClient()
                    .parseSchema("JSON", schemaMetadata.getSchema(), null);

            if (parsedSchema.isEmpty()) {
                log.error("Failed to parse JSON schema for subject {}", JSON_SUBJECT);
                return;
            }

            JsonSchema jsonSchema = (JsonSchema) parsedSchema.get();
            log.info("JsonSchema: {}",jsonSchema);
            ObjectMapper objectMapper = new ObjectMapper();
            String carJson = objectMapper.writeValueAsString(car);
            
            JsonNode jsonNode = objectMapper.readTree(carJson);
            //jsonSchema.validate(jsonNode);
            

            KafkaJsonSchemaSerializer<JsonNode> kafkaJsonSchemaSerializer = getJsonNodeKafkaJsonSchemaSerializer();
            byte[] finalMessage = kafkaJsonSchemaSerializer.serialize(JSON_TOPIC, JsonSchemaUtils.envelope(jsonSchema, jsonNode));
            ProducerRecord<String, Object> producerRecord = new ProducerRecord<>(
                    JSON_TOPIC, car.getCarId(), finalMessage
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

    private KafkaJsonSchemaSerializer<JsonNode> getJsonNodeKafkaJsonSchemaSerializer() {
        Map<String, Object> jsonSerializerProps = new HashMap<>();
        jsonSerializerProps.put(AbstractKafkaSchemaSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG, schemaRegistryUrl);
        jsonSerializerProps.put(AbstractKafkaSchemaSerDeConfig.AUTO_REGISTER_SCHEMAS, false);
        jsonSerializerProps.put(KafkaJsonSchemaSerializerConfig.FAIL_INVALID_SCHEMA, true);
        jsonSerializerProps.put(KafkaJsonSchemaSerializerConfig.FAIL_UNKNOWN_PROPERTIES, true);
        jsonSerializerProps.put(AbstractKafkaSchemaSerDeConfig.NORMALIZE_SCHEMAS, true);

        @Cleanup KafkaJsonSchemaSerializer<JsonNode> kafkaJsonSchemaSerializer = new KafkaJsonSchemaSerializer<>();
        kafkaJsonSchemaSerializer.configure(jsonSerializerProps, false);
        return kafkaJsonSchemaSerializer;
    }

    
}

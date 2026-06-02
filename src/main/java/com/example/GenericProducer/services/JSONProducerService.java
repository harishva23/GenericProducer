package com.example.GenericProducer.services;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;

import com.example.GenericProducer.KafkaClient.KafkaProducerClient;
import com.example.GenericProducer.KafkaClient.KarapaceClient;
import com.example.GenericProducer.enums.KafkaSerializerTypes;
import com.example.GenericProducer.pojo.Car;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;

import io.confluent.kafka.schemaregistry.ParsedSchema;
import io.confluent.kafka.schemaregistry.client.SchemaMetadata;
import io.confluent.kafka.schemaregistry.client.rest.exceptions.RestClientException;
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

    

    private final KafkaProducerClient kafkaProducerClient;
    private final KarapaceClient schemaRegistryClient;
    private KafkaJsonSchemaSerializer<JsonNode> kafkaJsonSchemaSerializer;
    private KafkaProducer<String, Object> jsonProducer;
    private SchemaMetadata schemaMetadata;
    private Optional<ParsedSchema> parsedSchema;
    private JsonSchema jsonSchema;

    @Value("${schema.registry.url}")
    private String schemaRegistryUrl;

    @Value("${kafka.username}")
    private String username;

    @Value("${kafka.password}")
    private String password;

    @Value("${json.input.topic.name}")
    private String jsonTopic;

    @PostConstruct
    private void initJSONProducer() throws IOException, RestClientException{
        jsonProducer =  kafkaProducerClient.getDefaultProducerClientWithoutPartitioner(username
            , password,
             schemaRegistryUrl,
              KafkaSerializerTypes.STRING_SERIALIZER, 
              KafkaSerializerTypes.BYTE_SERIALIZER);
            kafkaJsonSchemaSerializer = getJsonNodeKafkaJsonSchemaSerializer();
            if(jsonTopic != null && !jsonTopic.isEmpty()) {
                schemaMetadata = schemaRegistryClient.getClient().getLatestSchemaMetadata(jsonTopic + "-value");
                parsedSchema = schemaRegistryClient.getClient()
                    .parseSchema("JSON", schemaMetadata.getSchema(), null);
                jsonSchema = (JsonSchema) parsedSchema.get();
            }
    }

    public void produceCarJson(Car car) {
        
        try {
            
            ObjectMapper objectMapper = new ObjectMapper();
            String carJson = objectMapper.writeValueAsString(car);
            
            JsonNode jsonNode = objectMapper.readTree(carJson);
            
            byte[] finalMessage = kafkaJsonSchemaSerializer.serialize(jsonTopic, JsonSchemaUtils.envelope(jsonSchema, jsonNode));
            ProducerRecord<String, Object> producerRecord = new ProducerRecord<>(
                    jsonTopic, car.getCarId(), finalMessage
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
        jsonSerializerProps.put("basic.auth.credentials.source", "USER_INFO");
        jsonSerializerProps.put("basic.auth.user.info", username + ":" + password);
        jsonSerializerProps.put(AbstractKafkaSchemaSerDeConfig.AUTO_REGISTER_SCHEMAS, false);
        jsonSerializerProps.put(KafkaJsonSchemaSerializerConfig.FAIL_INVALID_SCHEMA, true);
        jsonSerializerProps.put(KafkaJsonSchemaSerializerConfig.FAIL_UNKNOWN_PROPERTIES, true);
        jsonSerializerProps.put(AbstractKafkaSchemaSerDeConfig.NORMALIZE_SCHEMAS, true);

        kafkaJsonSchemaSerializer = new KafkaJsonSchemaSerializer<>();
        kafkaJsonSchemaSerializer.configure(jsonSerializerProps, false);
        return kafkaJsonSchemaSerializer;
    }
}

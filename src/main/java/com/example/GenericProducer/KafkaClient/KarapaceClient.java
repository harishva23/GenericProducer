package com.example.GenericProducer.KafkaClient;

import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

import io.confluent.kafka.schemaregistry.SchemaProvider;
import io.confluent.kafka.schemaregistry.avro.AvroSchemaProvider;
import io.confluent.kafka.schemaregistry.client.CachedSchemaRegistryClient;
import io.confluent.kafka.schemaregistry.json.JsonSchemaProvider;
import io.confluent.kafka.schemaregistry.protobuf.ProtobufSchemaProvider;

@Component
public class KarapaceClient {

    private final CachedSchemaRegistryClient client;

    public KarapaceClient(@Value("${schema.registry.url}") String schemaRegistryUrl, @Value("${kafka.username}") String username, @Value("${kafka.password}") String password) {
        
        List<SchemaProvider> providers = Arrays.asList(new AvroSchemaProvider(), new JsonSchemaProvider(), new ProtobufSchemaProvider());
        Map<String, String> configs = new HashMap<>();
        configs.put("basic.auth.credentials.source", "USER_INFO");
        configs.put("basic.auth.user.info", username + ":" + password);
        client = new CachedSchemaRegistryClient(schemaRegistryUrl, 100, providers, configs, null);
    }

    public CachedSchemaRegistryClient getClient() {
        return  client;
    }
}

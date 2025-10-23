package com.example.GenericProducer.KafkaClient;

import java.util.Arrays;
import java.util.List;

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

    public KarapaceClient(@Value("${schema.registry.url}") String schemaRegistryUrl) {
        
        List<SchemaProvider> providers = Arrays.asList(new AvroSchemaProvider(), new JsonSchemaProvider(), new ProtobufSchemaProvider());
        client = new CachedSchemaRegistryClient(schemaRegistryUrl, 100, providers, null, null);
    
    }

    public CachedSchemaRegistryClient getClient() {
        return  client;
    }
}

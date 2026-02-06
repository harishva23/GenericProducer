package com.example.GenericProducer.KafkaClient;

import java.util.Properties;

import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.config.SaslConfigs;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;
import com.example.GenericProducer.enums.KafkaSerializerTypes;

import io.confluent.kafka.serializers.AbstractKafkaSchemaSerDeConfig;
import io.confluent.kafka.serializers.json.KafkaJsonSchemaSerializerConfig;

@Component
public class KafkaProducerClient {

    public static final String FALSE = "false";
    private static final String CLIENT_ID = "car-producer";
    private static final String SECURITY_PROTOCOL = "SASL_PLAINTEXT";
    private static final String SASL_MECHANISM = "SCRAM-SHA-512";
    private static final String SASL_JAAS_CONFIG = "org.apache.kafka.common.security.scram.ScramLoginModule required username=\"%s\" password=\"%s\";";
    private final String bootstrapServerUrl;


    public KafkaProducerClient(@Value("${kafka.bootstrap.server.url}") String bootstrapServerUrl) {
        this.bootstrapServerUrl = bootstrapServerUrl;
    }
    public <K, V> KafkaProducer<K, V> getDefaultProducerClientWithoutPartitioner(String username, String password, @Value("${schema.registry.url}") String schemaRegistryUrl, KafkaSerializerTypes keySerializer, KafkaSerializerTypes valueSerializer) {
        Properties props = new Properties();
        props.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, keySerializer.getClassName());
        props.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, valueSerializer.getClassName());
        props.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServerUrl);
        props.setProperty(ProducerConfig.CLIENT_ID_CONFIG, CLIENT_ID);
        props.setProperty(ProducerConfig.RETRIES_CONFIG, "0");
        props.setProperty(ProducerConfig.MAX_BLOCK_MS_CONFIG, "3000");
        props.setProperty(AdminClientConfig.SECURITY_PROTOCOL_CONFIG, SECURITY_PROTOCOL);
        props.setProperty(SaslConfigs.SASL_MECHANISM, SASL_MECHANISM);
        props.setProperty(SaslConfigs.SASL_JAAS_CONFIG, String.format(SASL_JAAS_CONFIG, username, password));
        props.setProperty(AbstractKafkaSchemaSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG, schemaRegistryUrl);
        props.setProperty("basic.auth.credentials.source", "USER_INFO");
        props.setProperty("basic.auth.user.info", username + ":" + password);
        props.setProperty(AbstractKafkaSchemaSerDeConfig.AUTO_REGISTER_SCHEMAS, FALSE);
        props.setProperty(AbstractKafkaSchemaSerDeConfig.LATEST_COMPATIBILITY_STRICT, "true");
        props.setProperty(AbstractKafkaSchemaSerDeConfig.VALUE_SUBJECT_NAME_STRATEGY, 
             "io.confluent.kafka.serializers.subject.TopicNameStrategy");
        if(valueSerializer.equals(KafkaSerializerTypes.BYTE_SERIALIZER)){
                props.setProperty(KafkaJsonSchemaSerializerConfig.FAIL_INVALID_SCHEMA, "true");
                props.setProperty(KafkaJsonSchemaSerializerConfig.FAIL_UNKNOWN_PROPERTIES, "true");
        }
        
        return new KafkaProducer<>(props);
    }
}

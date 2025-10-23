package com.example.GenericProducer.enums;

import lombok.Getter;

@Getter
public enum KafkaSerializerTypes {

    STRING_SERIALIZER("org.apache.kafka.common.serialization.StringSerializer"), AVRO_SERIALIZER("io.confluent.kafka.serializers.KafkaAvroSerializer"), JSON_SERIALIZER("io.confluent.kafka.serializers.json.KafkaJsonSchemaSerializer"), PROTOBUF_SERIALIZER("io.confluent.kafka.serializers.protobuf.KafkaProtobufSerializer"), BYTE_SERIALIZER("org.apache.kafka.common.serialization.ByteArraySerializer");

    private final String className;

    KafkaSerializerTypes(String className) {
        this.className = className;
    }
}

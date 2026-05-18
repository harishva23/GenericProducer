package com.example.GenericProducer.services;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicInteger;

import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.ConfigurableApplicationContext;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Service;

import com.example.GenericProducer.pojo.Car;
import com.example.GenericProducer.util.RandomCarDataGenerator;

import lombok.Cleanup;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;

@Service
@Slf4j
@RequiredArgsConstructor
public class ProducerScheduleService {

    @Value("${string.input.topic.name}")
    private String stringTopic;

    @Value("${json.input.topic.name}")
    private String jsonTopic;

    @Value("${avro.input.topic.name}")
    private String avroTopic;

    @Value("${protobuf.input.topic.name}")
    private String protoTopic;

    private final RandomCarDataGenerator carDataGenerator;
    private final ProtobufProducer protobufProducer;
    private final AvroProducer avroProducer;
    private final JSONProducerService jsonProducerService;
    private final StringProducer stringProducer;
    private final AtomicInteger messageCount = new AtomicInteger(0);  // Add counter
    private static final Integer MAX_MESSAGES = 1000000;  // Max messages to send
    private final ConfigurableApplicationContext context; 

    @Scheduled(fixedRate = 1)
    public void produceCarToBothFormats() {
        if (messageCount.get() >= MAX_MESSAGES) {
            log.info("Reached maximum message count. Shutting down...");
            context.close();  // Close the Spring application context
            return;
        }
        
        Car car = carDataGenerator.generateRandomCar();
        @Cleanup
        ExecutorService executorService = Executors.newVirtualThreadPerTaskExecutor();
        if(stringTopic != null && !stringTopic.isEmpty()) {
            executorService.submit(() -> stringProducer.produceCarString(car));
        }
        if(jsonTopic != null && !jsonTopic.isEmpty()) {
            executorService.submit(() -> jsonProducerService.produceCarJson(car));
        }
        if(avroTopic != null && !avroTopic.isEmpty()) {
            executorService.submit(() -> avroProducer.produceCarAvro(car));
        }
        if(protoTopic != null && !protoTopic.isEmpty()) {
            executorService.submit(() -> protobufProducer.produceCarProto(car));
        }

        int count = messageCount.incrementAndGet();
        log.info("Sent message {} of {}", count, MAX_MESSAGES);
    }
}

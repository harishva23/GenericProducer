package com.example.GenericProducer.services;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicInteger;

import org.springframework.context.ConfigurableApplicationContext;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Service;

import com.example.GenericProducer.pojo.Car;
import com.example.GenericProducer.util.RandomCarDataGenerator;

import lombok.Cleanup;
import lombok.RequiredArgsConstructor;

@Service
@RequiredArgsConstructor
public class ProducerScheduleService {

    private final RandomCarDataGenerator carDataGenerator;
    private final ProtobufProducer protobufProducer;
    private final AvroProducer avroProducer;
    private final JSONProducerService jsonProducerService;
    private final StringProducer stringProducer;
    private final AtomicInteger messageCount = new AtomicInteger(0);  // Add counter
    private static final Integer MAX_MESSAGES = 100;  // Max messages to send
    private final ConfigurableApplicationContext context; 

    @Scheduled(fixedRate = 10000)
    public void produceCarToBothFormats() {
        if (messageCount.get() >= MAX_MESSAGES) {
            System.out.println("Reached maximum message count. Shutting down...");
            context.close();  // Close the Spring application context
            return;
        }
        
        Car car = carDataGenerator.generateRandomCar();
        @Cleanup
        ExecutorService executorService = Executors.newVirtualThreadPerTaskExecutor();
        //executorService.submit(()->jsonProducerService.produceCarJson(car));
        //executorService.submit(()->avroProducer.produceCarAvro(car));
        //executorService.submit(()->protobufProducer.produceCarProto(car));
        executorService.submit(()->stringProducer.produceCarString(car));

        int count = messageCount.incrementAndGet();
        System.out.println("Sent message " + count + " of " + MAX_MESSAGES);
    }
}

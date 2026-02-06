package com.example.GenericProducer.services;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

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
    @Scheduled(fixedRate = 5000)
    public void produceCarToBothFormats() {
        Car car = carDataGenerator.generateRandomCar(); // generate once
        @Cleanup
        ExecutorService executorService = Executors.newVirtualThreadPerTaskExecutor();
        //executorService.submit(()->jsonProducerService.produceCarJson(car));
        //executorService.submit(()->avroProducer.produceCarAvro(car));
        executorService.submit(()->protobufProducer.produceCarProto(car));
    }
}

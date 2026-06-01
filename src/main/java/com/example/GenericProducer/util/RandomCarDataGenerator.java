package com.example.GenericProducer.util;

import com.example.GenericProducer.pojo.Car;
import com.example.GenericProducer.pojo.Location;

import jakarta.annotation.PostConstruct;
import lombok.NoArgsConstructor;
import lombok.extern.slf4j.Slf4j;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.concurrent.atomic.AtomicInteger;

import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

@Component
@NoArgsConstructor
@Slf4j
public class RandomCarDataGenerator {

    @Value("${num.car.keys}")
    private int numCarKeys;

    @Value("${num.bike.keys}")
    private int numBikeKeys;

    @Value("${sequential.keys.or.random}")
    private String randomSequentialKeys;

    private static final Random random = new Random();
    private static final List<String> carIdList =new ArrayList<>();
    private static final Map<String, String> carNumberMap = new HashMap<>();
    private static final AtomicInteger carIdCounter = new AtomicInteger(0);
    private static final AtomicInteger packetNumberCounter = new AtomicInteger(0);

    

    @PostConstruct
    public void initializeCarData() {
        for(int i=1;i<=numCarKeys;i++) {
            carIdList.add("car-" + i);
            carNumberMap.put("car-" + i, "CAR");
        }
        // for(int i=1;i<=numBikeKeys;i++) {
        //     carIdList.add("bike-" + i);
        //     carNumberMap.put("bike-" + i, "BIKE");
        // }
    }

    public Car generateRandomCar() {
        Car car = new Car();
        log.info("randomOrSequentialKeys: {}", randomSequentialKeys);
        if("sequential".equalsIgnoreCase(randomSequentialKeys)) {
            String carId = "car-" + carIdCounter.incrementAndGet();
            car.setCarId(carId);
            car.setCarName("CAR");
        } else {
            String carId = carIdList.get(random.nextInt(carIdList.size()));
            car.setCarId(carId);
            car.setCarName(carNumberMap.get(carId));
        }
        car.setSpeed(generateRandomSpeed());
        car.setLocation(new Location(generateRandomLatitude(), generateRandomLongitude()));
        car.setPacketNumberString("packet-" + packetNumberCounter.incrementAndGet());
        car.setPacketNumber(packetNumberCounter.get());
        car.setIsActive(random.nextBoolean());
        return car;
    }

    private static double generateRandomLatitude() {
        // Range: -90 to 90
        return -90 + (180 * random.nextDouble());
    }

    private static double generateRandomLongitude() {
        // Range: -180 to 180
        return -180 + (360 * random.nextDouble());
    }

    private static double generateRandomSpeed() {
        // Random speed between 0 and 300 km/h
        return (random.nextDouble() * 50) + 250;
    }
}

package com.example.GenericProducer.util;

import com.example.GenericProducer.pojo.Car;
import com.example.GenericProducer.pojo.Location;

import jakarta.annotation.PostConstruct;
import lombok.NoArgsConstructor;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.concurrent.atomic.AtomicInteger;

import org.springframework.stereotype.Component;

@Component
@NoArgsConstructor
public class RandomCarDataGenerator {

    private static final Random random = new Random();
    private static final List<String> carIdList =new ArrayList<>();
    private static final Map<String, String> carNumberMap = new HashMap<>();
    private static final AtomicInteger carIdCounter = new AtomicInteger(0);

    @PostConstruct
    public void initializeCarData() {
        for(int i=1;i<1000000;i++) {
            carIdList.add("car-" + i);
            carNumberMap.put("car-" + i, "CAR");
        }
        for(int i=1;i<1000000;i++) {
            carIdList.add("bike-" + i);
            carNumberMap.put("bike-" + i, "BIKE");
        }
    }

    public Car generateRandomCar() {
        Car car = new Car();
        String carId = carIdList.get(random.nextInt(carIdList.size()));
        car.setCarId(carId);
        car.setCarName(carNumberMap.get(carId));
        car.setSpeed(generateRandomSpeed());
        car.setLocation(new Location(generateRandomLatitude(), generateRandomLongitude()));
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
        return (random.nextDouble() * 250);
    }
}

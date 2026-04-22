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

import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

@Component
@NoArgsConstructor
public class RandomCarDataGenerator {

    @Value("${CAR_ID}")
    private  String carIDString;

    @Value("${CAR_NUMBER}")
    private  String carNumberString;

    private static final Random random = new Random();
    private static final List<String> carIdList =new ArrayList<>();
    private static final Map<String, String> carNumberMap = new HashMap<>();

    @PostConstruct
    public void initializeCarData() {
        for(int i=1;i<100000;i++) {
            carIdList.add("car-" + i);
            carNumberMap.put("car-" + i, "CAR");
        }
        // for(int i=0;i<1;i++) {
        //     carIdList.add("bike-" + i);
        //     carNumberMap.put("bike-" + i, "BIKE");
        // }
    }

    public Car generateRandomCar() {
        Car car = new Car();
        String carId = carIdList.get(random.nextInt(carIdList.size()));
        String carNumber = carNumberMap.get(carId);
        car.setCarId(carId);
        car.setCarName(carNumber);
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
        return (random.nextDouble() * 180);
    }
}

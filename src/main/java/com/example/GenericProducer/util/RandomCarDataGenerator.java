package com.example.GenericProducer.util;

import com.example.GenericProducer.pojo.Car;
import com.example.GenericProducer.pojo.Location;

import lombok.NoArgsConstructor;

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

    public Car generateRandomCar() {
        Car car = new Car();
        String carId = carIDString;
        String carNumber = carNumberString;

        if (carId == null || carId.isEmpty()) {
            carId = "default-car-id";
        }
        if (carNumber == null || carNumber.isEmpty()) {
            carNumber = "default-car-number";
        }
        car.setCarId(carId);
        car.setCarName(carNumberString);
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
        // Random speed between 0 and 180 km/h
        return random.nextDouble() * 180;
    }
}

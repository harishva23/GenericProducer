package com.example.GenericProducer.pojo;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@AllArgsConstructor
@NoArgsConstructor
public class Car {

    private String carId;

    private String carNumber;

    private double speed;

    private double latitude;

    private double longitude;

    //private String extraAttribute;

}

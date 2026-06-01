package com.example.GenericProducer.pojo;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@AllArgsConstructor
@NoArgsConstructor
public class Car {

    private String carId;

    private String carName;

    private String packetNumberString;

    private Integer packetNumber;

    private double speed;

    private Location location;

    private Boolean isActive;

}

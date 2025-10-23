package com.example.GenericProducer;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.scheduling.annotation.EnableScheduling;

@SpringBootApplication
@EnableScheduling
public class GenericProducerApplication {

	public static void main(String[] args) {
		SpringApplication.run(GenericProducerApplication.class, args);
	}

}

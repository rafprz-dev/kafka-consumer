package com.example.kafkaconsumer.model;

import lombok.Data;

import java.time.LocalDate;

@Data
public class ExampleDto {
    private Long id;
    private String message;
    private int count;
    private LocalDate date;
}

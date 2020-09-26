package com.rickie;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

@SpringBootApplication
public class HBaseApp
{
    public static void main( String[] args )
    {
        System.out.println("hello ...");
        SpringApplication.run(HBaseApp.class, args);
    }
}

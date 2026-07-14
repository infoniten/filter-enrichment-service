package com.example.filterenrichment;

import com.example.filterenrichment.config.FilterEnrichmentProperties;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.context.properties.EnableConfigurationProperties;

@SpringBootApplication
@EnableConfigurationProperties(FilterEnrichmentProperties.class)
public class FilterEnrichmentApplication {

    public static void main(String[] args) {
        SpringApplication.run(FilterEnrichmentApplication.class, args);
    }
}

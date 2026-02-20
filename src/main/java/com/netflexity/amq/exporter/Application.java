package com.netflexity.amq.exporter;

import lombok.extern.slf4j.Slf4j;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.scheduling.annotation.EnableScheduling;

/**
 * Main application class for the Anypoint Metrics Prometheus Exporter.
 * 
 * This Spring Boot application implements the complete Netflexity Metrics Toolkit (MTK)
 * functionality, ported from MuleSoft/DataWeave to Java Spring Boot with Prometheus metrics.
 * 
 * MTK Modules:
 * - CloudHub App Inventory (CH1 & CH2)
 * - Dashboard Statistics
 * - API Manager metrics
 * - API Analytics
 * - Metering/Usage (24 meter types)
 * - Optimization Reports
 * - Platform, Business, SDLC, and Alerts metrics
 * 
 * Features:
 * - Prometheus metrics at /actuator/prometheus
 * - Health checks at /actuator/health
 * - Scheduled collectors with configurable intervals
 * - Multi-environment auto-discovery
 * - Connected App OAuth2 authentication
 * 
 * @author Netflexity
 * @version 1.0.0
 */
@SpringBootApplication
@EnableScheduling
@Slf4j
public class Application {

    public static void main(String[] args) {
        log.info("Starting Anypoint Metrics Prometheus Exporter (MTK port)...");
        SpringApplication.run(Application.class, args);
        log.info("Anypoint Metrics Prometheus Exporter started successfully!");
    }
}
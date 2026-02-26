package com.netflexity.amq.exporter.collector;

import com.netflexity.anypoint.common.client.AnypointAuthClient;
import com.netflexity.anypoint.common.config.AnypointConfig;
import com.netflexity.anypoint.common.config.ExporterConfig;
import io.micrometer.core.instrument.Gauge;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Timer;
import lombok.extern.slf4j.Slf4j;
import org.springframework.boot.context.event.ApplicationReadyEvent;
import org.springframework.context.event.EventListener;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;
import org.springframework.web.reactive.function.client.WebClient;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import java.time.LocalDate;
import java.time.format.DateTimeFormatter;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Collects Metering/Usage metrics and registers them with Micrometer.
 * 
 * Covers MTK Module 5: Metering/Usage
 * - POST /metering/usage/api/v1/meters:search
 * - 24 meter types including: mule_flow_count, mule_message_count, total_data_throughput, 
 *   api_manager_api_count, anypoint_mq_requests, etc.
 * 
 * Metrics:
 * - anypoint_metering_{meter_name} (gauge for each meter type)
 */
@Component
@Slf4j
public class MeteringUsageCollector {

    private final WebClient webClient;
    private final AnypointAuthClient authClient;
    private final AnypointConfig anypointConfig;
    private final MeterRegistry meterRegistry;
    private final ExporterConfig.ExporterMetrics exporterMetrics;

    // Metrics storage
    private final ConcurrentHashMap<String, AtomicLong> meteringMetrics = new ConcurrentHashMap<>();

    // All known meter types from MTK
    private static final List<String> METER_TYPES = Arrays.asList(
            "mule_flow_count",
            "mule_message_count", 
            "total_data_throughput",
            "api_manager_api_count",
            "api_manager_policy_count",
            "anypoint_mq_requests",
            "anypoint_mq_messages_delivered",
            "anypoint_mq_messages_received",
            "object_store_requests",
            "object_store_keys",
            "design_center_api_specification_count",
            "design_center_fragment_count",
            "design_center_mule_application_count",
            "exchange_assets",
            "cloudhub_networking_ips_assigned",
            "cloudhub_networking_vpn_megabits",
            "cloudhub_networking_dlb_megabits",
            "monitoring_api_requests",
            "monitoring_custom_metrics_count",
            "monitoring_custom_dashboards_count",
            "visualizer_requests",
            "partner_manager_transactions",
            "trading_partner_count",
            "maps_count"
    );

    public MeteringUsageCollector(WebClient webClient,
                                 AnypointAuthClient authClient,
                                 AnypointConfig anypointConfig,
                                 MeterRegistry meterRegistry,
                                 ExporterConfig.ExporterMetrics exporterMetrics) {
        this.webClient = webClient;
        this.authClient = authClient;
        this.anypointConfig = anypointConfig;
        this.meterRegistry = meterRegistry;
        this.exporterMetrics = exporterMetrics;
        
        log.info("Initialized MeteringUsageCollector for {} meter types",
                METER_TYPES.size());
    }

    /**
     * Initialize metrics collection after application startup
     */
    @EventListener(ApplicationReadyEvent.class)
    public void initializeMetrics() {
        if (!anypointConfig.getScrape().isEnabled()) {
            log.info("Metering/Usage collection is disabled");
            return;
        }
        
        log.info("Starting initial Metering/Usage collection...");
        collectMetrics();
    }

    /**
     * Scheduled metrics collection - every 30 minutes for metering data
     */
    @Scheduled(fixedDelayString = "${anypoint.scrape.metering-usage-interval-seconds:1800}000", 
               initialDelayString = "${anypoint.scrape.metering-usage-interval-seconds:1800}000")
    public void scheduledCollection() {
        if (!anypointConfig.getScrape().isEnabled()) {
            return;
        }
        
        log.debug("Starting scheduled Metering/Usage collection");
        collectMetrics();
    }

    /**
     * Main metrics collection method
     */
    private void collectMetrics() {
        Timer.Sample sample = exporterMetrics.startScrapeTimer();
        
        try {
            authClient.getAccessToken()
                    .flatMapMany(token -> 
                            Flux.fromIterable(anypointConfig.getEnvironments())
                                    .flatMap(environment -> 
                                            collectEnvironmentMetrics(environment, token.getAccessToken())
                                    )
                    )
                    .doOnComplete(() -> {
                        exporterMetrics.recordScrapeTime(sample);
                        log.info("Metering/Usage collection completed successfully");
                    })
                    .doOnError(error -> {
                        exporterMetrics.incrementErrorCounter("metering_usage_collection_failed");
                        log.error("Metering/Usage collection failed: {}", error.getMessage(), error);
                    })
                    .subscribe();
            
        } catch (Exception e) {
            exporterMetrics.incrementErrorCounter("metering_usage_unexpected_error");
            log.error("Unexpected error during Metering/Usage collection: {}", e.getMessage(), e);
        }
    }

    /**
     * Collect metering metrics for a specific environment
     */
    private Mono<Void> collectEnvironmentMetrics(AnypointConfig.Environment environment, String accessToken) {
        log.debug("Collecting Metering/Usage for environment {} ({})", environment.getName(), environment.getId());
        
        return Flux.fromIterable(METER_TYPES)
                .flatMap(meterType -> 
                        collectMeterTypeUsage(environment, meterType, accessToken)
                                .onErrorResume(error -> {
                                    exporterMetrics.incrementErrorCounter("meter_type_failed");
                                    log.warn("Failed to collect meter type {} for environment {}: {}", 
                                            meterType, environment.getName(), error.getMessage());
                                    return Mono.empty();
                                })
                )
                .then()
                .doOnSuccess(v -> log.debug("Completed Metering/Usage for environment {}", environment.getName()))
                .onErrorResume(throwable -> {
                    exporterMetrics.incrementErrorCounter("metering_usage_environment_failed");
                    log.warn("Failed to collect Metering/Usage for environment {}: {}", environment.getName(), throwable.getMessage());
                    return Mono.empty();
                });
    }

    /**
     * Collect usage for a specific meter type
     */
    private Mono<Void> collectMeterTypeUsage(AnypointConfig.Environment environment, String meterType, String accessToken) {
        String uri = anypointConfig.getBaseUrl() + "/metering/usage/api/v1/meters:search";
        
        // Query current month usage
        LocalDate now = LocalDate.now();
        LocalDate startOfMonth = now.withDayOfMonth(1);
        
        MeteringSearchRequest request = new MeteringSearchRequest();
        request.setOrganizationId(anypointConfig.getOrganizationId());
        request.setEnvironmentId(environment.getId());
        request.setMeterName(meterType);
        request.setStartDate(startOfMonth.format(DateTimeFormatter.ISO_LOCAL_DATE));
        request.setEndDate(now.format(DateTimeFormatter.ISO_LOCAL_DATE));
        request.setAggregation("total");
        
        return webClient.post()
                .uri(uri)
                .header("Authorization", "Bearer " + accessToken)
                .bodyValue(request)
                .retrieve()
                .bodyToMono(MeteringSearchResponse.class)
                .doOnNext(response -> updateMeteringMetrics(environment.getName(), meterType, response))
                .then()
                .onErrorResume(error -> {
                    log.debug("Failed to collect meter {} for environment {}: {}", 
                            meterType, environment.getName(), error.getMessage());
                    return Mono.empty();
                });
    }

    /**
     * Update metering metrics from search response
     */
    private void updateMeteringMetrics(String environmentName, String meterType, MeteringSearchResponse response) {
        if (response.getData() == null || response.getData().isEmpty()) {
            return;
        }
        
        // Sum up all usage values for this meter type
        double totalUsage = response.getData().stream()
                .mapToDouble(usage -> usage.getValue() != null ? usage.getValue() : 0.0)
                .sum();
        
        String metricName = "anypoint_metering_" + meterType;
        String metricKey = createMetricKey(meterType, environmentName);
        
        updateGaugeMetric(metricName, metricKey, totalUsage, Map.of(
                "meter_type", meterType,
                "environment", environmentName,
                "unit", getUnitForMeterType(meterType)
        ));
        
        log.debug("Updated metering metric {}: {} {} for environment {}", 
                meterType, totalUsage, getUnitForMeterType(meterType), environmentName);
    }

    /**
     * Get the unit for a specific meter type
     */
    private String getUnitForMeterType(String meterType) {
        switch (meterType) {
            case "total_data_throughput":
            case "cloudhub_networking_vpn_megabits":
            case "cloudhub_networking_dlb_megabits":
                return "megabytes";
            case "mule_flow_count":
            case "mule_message_count":
            case "anypoint_mq_requests":
            case "anypoint_mq_messages_delivered":
            case "anypoint_mq_messages_received":
            case "object_store_requests":
            case "monitoring_api_requests":
            case "visualizer_requests":
            case "partner_manager_transactions":
                return "count";
            case "api_manager_api_count":
            case "api_manager_policy_count":
            case "object_store_keys":
            case "design_center_api_specification_count":
            case "design_center_fragment_count":
            case "design_center_mule_application_count":
            case "exchange_assets":
            case "cloudhub_networking_ips_assigned":
            case "monitoring_custom_metrics_count":
            case "monitoring_custom_dashboards_count":
            case "trading_partner_count":
            case "maps_count":
                return "items";
            default:
                return "units";
        }
    }

    /**
     * Create a unique key for a metric
     */
    private String createMetricKey(String meterType, String environment) {
        return meterType + "_" + environment;
    }

    /**
     * Update or create a gauge metric
     */
    private void updateGaugeMetric(String metricName, String key, double value, Map<String, String> tags) {
        meteringMetrics.computeIfAbsent(key, k -> {
            AtomicLong atomicValue = new AtomicLong((long) (value * 1000)); // Store with precision
            
            // Build gauge with tags
            Gauge.Builder<AtomicLong> builder = Gauge.builder(metricName, atomicValue, atomic -> atomic.get() / 1000.0);
                    
            tags.forEach(builder::tag);
            
            builder.register(meterRegistry);
            return atomicValue;
        }).set((long) (value * 1000));
    }

    // Data Transfer Objects for Metering API
    public static class MeteringSearchRequest {
        private String organizationId;
        private String environmentId;
        private String meterName;
        private String startDate;
        private String endDate;
        private String aggregation;
        
        // Getters and setters
        public String getOrganizationId() { return organizationId; }
        public void setOrganizationId(String organizationId) { this.organizationId = organizationId; }
        
        public String getEnvironmentId() { return environmentId; }
        public void setEnvironmentId(String environmentId) { this.environmentId = environmentId; }
        
        public String getMeterName() { return meterName; }
        public void setMeterName(String meterName) { this.meterName = meterName; }
        
        public String getStartDate() { return startDate; }
        public void setStartDate(String startDate) { this.startDate = startDate; }
        
        public String getEndDate() { return endDate; }
        public void setEndDate(String endDate) { this.endDate = endDate; }
        
        public String getAggregation() { return aggregation; }
        public void setAggregation(String aggregation) { this.aggregation = aggregation; }
    }
    
    public static class MeteringSearchResponse {
        private List<MeteringUsageData> data;
        private Integer totalCount;
        
        public List<MeteringUsageData> getData() { 
            return data != null ? data : java.util.Collections.emptyList(); 
        }
        public void setData(List<MeteringUsageData> data) { this.data = data; }
        
        public Integer getTotalCount() { return totalCount; }
        public void setTotalCount(Integer totalCount) { this.totalCount = totalCount; }
    }
    
    public static class MeteringUsageData {
        private String meterName;
        private String organizationId;
        private String environmentId;
        private String date;
        private Double value;
        private String unit;
        
        public String getMeterName() { return meterName; }
        public void setMeterName(String meterName) { this.meterName = meterName; }
        
        public String getOrganizationId() { return organizationId; }
        public void setOrganizationId(String organizationId) { this.organizationId = organizationId; }
        
        public String getEnvironmentId() { return environmentId; }
        public void setEnvironmentId(String environmentId) { this.environmentId = environmentId; }
        
        public String getDate() { return date; }
        public void setDate(String date) { this.date = date; }
        
        public Double getValue() { return value; }
        public void setValue(Double value) { this.value = value; }
        
        public String getUnit() { return unit; }
        public void setUnit(String unit) { this.unit = unit; }
    }
}
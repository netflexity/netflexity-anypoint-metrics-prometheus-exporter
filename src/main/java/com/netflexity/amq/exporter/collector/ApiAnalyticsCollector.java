package com.netflexity.amq.exporter.collector;

import com.netflexity.amq.exporter.client.AnypointAuthClient;
import com.netflexity.amq.exporter.config.AnypointConfig;
import com.netflexity.amq.exporter.config.ExporterConfig;
import io.micrometer.core.instrument.Counter;
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

import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Collects API Analytics metrics and registers them with Micrometer.
 * 
 * Covers MTK Module 4: API Analytics
 * - GET /analytics/1.0/{orgId}/environments/{envId}/query
 * 
 * Metrics:
 * - anypoint_api_requests_total (counter)
 * - anypoint_api_policy_violations_total (counter)
 */
@Component
@Slf4j
public class ApiAnalyticsCollector {

    private final WebClient webClient;
    private final AnypointAuthClient authClient;
    private final AnypointConfig anypointConfig;
    private final MeterRegistry meterRegistry;
    private final ExporterConfig.ExporterMetrics exporterMetrics;

    // Counter storage for proper Prometheus counter behavior
    private final ConcurrentHashMap<String, Counter> requestCounters = new ConcurrentHashMap<>();
    private final ConcurrentHashMap<String, Counter> violationCounters = new ConcurrentHashMap<>();
    private final ConcurrentHashMap<String, Double> lastValues = new ConcurrentHashMap<>();

    public ApiAnalyticsCollector(WebClient webClient,
                               AnypointAuthClient authClient,
                               AnypointConfig anypointConfig,
                               MeterRegistry meterRegistry,
                               ExporterConfig.ExporterMetrics exporterMetrics) {
        this.webClient = webClient;
        this.authClient = authClient;
        this.anypointConfig = anypointConfig;
        this.meterRegistry = meterRegistry;
        this.exporterMetrics = exporterMetrics;
        
        log.info("Initialized ApiAnalyticsCollector for {} environments",
                anypointConfig.getEnvironments().size());
    }

    /**
     * Initialize metrics collection after application startup
     */
    @EventListener(ApplicationReadyEvent.class)
    public void initializeMetrics() {
        if (!anypointConfig.getScrape().isEnabled()) {
            log.info("API Analytics collection is disabled");
            return;
        }
        
        log.info("Starting initial API Analytics collection...");
        collectMetrics();
    }

    /**
     * Scheduled metrics collection - every 5 minutes for API Analytics
     */
    @Scheduled(fixedDelayString = "${anypoint.scrape.api-analytics-interval-seconds:300}000", 
               initialDelayString = "${anypoint.scrape.api-analytics-interval-seconds:300}000")
    public void scheduledCollection() {
        if (!anypointConfig.getScrape().isEnabled()) {
            return;
        }
        
        log.debug("Starting scheduled API Analytics collection");
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
                        log.info("API Analytics collection completed successfully");
                    })
                    .doOnError(error -> {
                        exporterMetrics.incrementErrorCounter("api_analytics_collection_failed");
                        log.error("API Analytics collection failed: {}", error.getMessage(), error);
                    })
                    .subscribe();
            
        } catch (Exception e) {
            exporterMetrics.incrementErrorCounter("api_analytics_unexpected_error");
            log.error("Unexpected error during API Analytics collection: {}", e.getMessage(), e);
        }
    }

    /**
     * Collect API Analytics metrics for a specific environment
     */
    private Mono<Void> collectEnvironmentMetrics(AnypointConfig.Environment environment, String accessToken) {
        log.debug("Collecting API Analytics for environment {} ({})", environment.getName(), environment.getId());
        
        // Collect both request counts and policy violations
        Mono<Void> requestMetrics = collectApiRequestCounts(environment, accessToken);
        Mono<Void> violationMetrics = collectPolicyViolations(environment, accessToken);
        
        return Mono.when(requestMetrics, violationMetrics)
                .doOnSuccess(v -> log.debug("Completed API Analytics for environment {}", environment.getName()))
                .onErrorResume(throwable -> {
                    exporterMetrics.incrementErrorCounter("api_analytics_environment_failed");
                    log.warn("Failed to collect API Analytics for environment {}: {}", environment.getName(), throwable.getMessage());
                    return Mono.empty();
                });
    }

    /**
     * Collect API request count analytics
     */
    private Mono<Void> collectApiRequestCounts(AnypointConfig.Environment environment, String accessToken) {
        String uri = anypointConfig.getBaseUrl() + "/analytics/1.0/" + 
                anypointConfig.getOrganizationId() + "/environments/" + environment.getId() + "/query";
        
        // Query for API request counts in the last hour
        LocalDateTime endTime = LocalDateTime.now();
        LocalDateTime startTime = endTime.minusHours(1);
        
        ApiAnalyticsQuery query = new ApiAnalyticsQuery();
        query.setMetrics(List.of("request_count"));
        query.setDimensions(List.of("api_name", "api_version", "method"));
        query.setStartDate(startTime.format(DateTimeFormatter.ISO_LOCAL_DATE_TIME));
        query.setEndDate(endTime.format(DateTimeFormatter.ISO_LOCAL_DATE_TIME));
        query.setFormat("json");
        
        return webClient.post()
                .uri(uri)
                .header("Authorization", "Bearer " + accessToken)
                .bodyValue(query)
                .retrieve()
                .bodyToMono(ApiAnalyticsResponse.class)
                .doOnNext(response -> updateRequestCountMetrics(environment.getName(), response))
                .then()
                .onErrorResume(error -> {
                    exporterMetrics.incrementErrorCounter("api_request_counts_failed");
                    log.warn("Failed to collect API request counts for environment {}: {}", environment.getName(), error.getMessage());
                    return Mono.empty();
                });
    }

    /**
     * Collect API policy violation analytics
     */
    private Mono<Void> collectPolicyViolations(AnypointConfig.Environment environment, String accessToken) {
        String uri = anypointConfig.getBaseUrl() + "/analytics/1.0/" + 
                anypointConfig.getOrganizationId() + "/environments/" + environment.getId() + "/query";
        
        // Query for policy violations in the last hour
        LocalDateTime endTime = LocalDateTime.now();
        LocalDateTime startTime = endTime.minusHours(1);
        
        ApiAnalyticsQuery query = new ApiAnalyticsQuery();
        query.setMetrics(List.of("policy_violation_count"));
        query.setDimensions(List.of("api_name", "api_version", "policy_name", "violation_type"));
        query.setStartDate(startTime.format(DateTimeFormatter.ISO_LOCAL_DATE_TIME));
        query.setEndDate(endTime.format(DateTimeFormatter.ISO_LOCAL_DATE_TIME));
        query.setFormat("json");
        
        return webClient.post()
                .uri(uri)
                .header("Authorization", "Bearer " + accessToken)
                .bodyValue(query)
                .retrieve()
                .bodyToMono(ApiAnalyticsResponse.class)
                .doOnNext(response -> updatePolicyViolationMetrics(environment.getName(), response))
                .then()
                .onErrorResume(error -> {
                    exporterMetrics.incrementErrorCounter("policy_violations_failed");
                    log.warn("Failed to collect policy violations for environment {}: {}", environment.getName(), error.getMessage());
                    return Mono.empty();
                });
    }

    /**
     * Update request count metrics from analytics response
     */
    private void updateRequestCountMetrics(String environmentName, ApiAnalyticsResponse response) {
        if (response.getData() == null) {
            return;
        }
        
        for (ApiAnalyticsDataPoint dataPoint : response.getData()) {
            String apiName = getValueFromDimensions(dataPoint.getDimensions(), "api_name", "unknown");
            String apiVersion = getValueFromDimensions(dataPoint.getDimensions(), "api_version", "unknown");
            String method = getValueFromDimensions(dataPoint.getDimensions(), "method", "unknown");
            
            Double requestCount = getValueFromMetrics(dataPoint.getMetrics(), "request_count");
            if (requestCount != null && requestCount > 0) {
                String counterKey = createCounterKey("requests", apiName, apiVersion, method, environmentName);
                
                // Get or create counter
                Counter counter = requestCounters.computeIfAbsent(counterKey, key -> 
                        Counter.builder("anypoint_api_requests_total")
                                .tags("api_name", apiName,
                                      "api_version", apiVersion,
                                      "method", method,
                                      "environment", environmentName)
                                .register(meterRegistry)
                );
                
                // Increment by the difference from last time (to maintain counter semantics)
                Double lastValue = lastValues.getOrDefault(counterKey, 0.0);
                double increment = requestCount - lastValue;
                if (increment > 0) {
                    counter.increment(increment);
                    lastValues.put(counterKey, requestCount);
                }
            }
        }
        
        log.debug("Updated API request count metrics for environment {}: {} data points", environmentName, response.getData().size());
    }

    /**
     * Update policy violation metrics from analytics response
     */
    private void updatePolicyViolationMetrics(String environmentName, ApiAnalyticsResponse response) {
        if (response.getData() == null) {
            return;
        }
        
        for (ApiAnalyticsDataPoint dataPoint : response.getData()) {
            String apiName = getValueFromDimensions(dataPoint.getDimensions(), "api_name", "unknown");
            String apiVersion = getValueFromDimensions(dataPoint.getDimensions(), "api_version", "unknown");
            String policyName = getValueFromDimensions(dataPoint.getDimensions(), "policy_name", "unknown");
            String violationType = getValueFromDimensions(dataPoint.getDimensions(), "violation_type", "unknown");
            
            Double violationCount = getValueFromMetrics(dataPoint.getMetrics(), "policy_violation_count");
            if (violationCount != null && violationCount > 0) {
                String counterKey = createCounterKey("violations", apiName, apiVersion, policyName, environmentName);
                
                // Get or create counter
                Counter counter = violationCounters.computeIfAbsent(counterKey, key -> 
                        Counter.builder("anypoint_api_policy_violations_total")
                                .tags("api_name", apiName,
                                      "api_version", apiVersion,
                                      "policy_name", policyName,
                                      "violation_type", violationType,
                                      "environment", environmentName)
                                .register(meterRegistry)
                );
                
                // Increment by the difference from last time
                Double lastValue = lastValues.getOrDefault(counterKey, 0.0);
                double increment = violationCount - lastValue;
                if (increment > 0) {
                    counter.increment(increment);
                    lastValues.put(counterKey, violationCount);
                }
            }
        }
        
        log.debug("Updated policy violation metrics for environment {}: {} data points", environmentName, response.getData().size());
    }

    /**
     * Get value from dimensions map
     */
    private String getValueFromDimensions(Map<String, Object> dimensions, String key, String defaultValue) {
        Object value = dimensions != null ? dimensions.get(key) : null;
        return value != null ? value.toString() : defaultValue;
    }

    /**
     * Get numeric value from metrics map
     */
    private Double getValueFromMetrics(Map<String, Object> metrics, String key) {
        Object value = metrics != null ? metrics.get(key) : null;
        if (value instanceof Number) {
            return ((Number) value).doubleValue();
        }
        return null;
    }

    /**
     * Create a unique key for counters
     */
    private String createCounterKey(String type, String... parts) {
        return type + "_" + String.join("_", parts);
    }

    // Data Transfer Objects for API Analytics
    public static class ApiAnalyticsQuery {
        private List<String> metrics;
        private List<String> dimensions;
        private String startDate;
        private String endDate;
        private String format;
        
        // Getters and setters
        public List<String> getMetrics() { return metrics; }
        public void setMetrics(List<String> metrics) { this.metrics = metrics; }
        
        public List<String> getDimensions() { return dimensions; }
        public void setDimensions(List<String> dimensions) { this.dimensions = dimensions; }
        
        public String getStartDate() { return startDate; }
        public void setStartDate(String startDate) { this.startDate = startDate; }
        
        public String getEndDate() { return endDate; }
        public void setEndDate(String endDate) { this.endDate = endDate; }
        
        public String getFormat() { return format; }
        public void setFormat(String format) { this.format = format; }
    }
    
    public static class ApiAnalyticsResponse {
        private List<ApiAnalyticsDataPoint> data;
        private Integer totalHits;
        
        public List<ApiAnalyticsDataPoint> getData() { return data; }
        public void setData(List<ApiAnalyticsDataPoint> data) { this.data = data; }
        
        public Integer getTotalHits() { return totalHits; }
        public void setTotalHits(Integer totalHits) { this.totalHits = totalHits; }
    }
    
    public static class ApiAnalyticsDataPoint {
        private Map<String, Object> dimensions;
        private Map<String, Object> metrics;
        
        public Map<String, Object> getDimensions() { return dimensions; }
        public void setDimensions(Map<String, Object> dimensions) { this.dimensions = dimensions; }
        
        public Map<String, Object> getMetrics() { return metrics; }
        public void setMetrics(Map<String, Object> metrics) { this.metrics = metrics; }
    }
}
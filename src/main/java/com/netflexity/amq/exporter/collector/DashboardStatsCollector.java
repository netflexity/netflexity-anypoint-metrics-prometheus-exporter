package com.netflexity.amq.exporter.collector;

import com.netflexity.amq.exporter.client.AnypointAuthClient;
import com.netflexity.amq.exporter.config.AnypointConfig;
import com.netflexity.amq.exporter.config.ExporterConfig;
import com.fasterxml.jackson.annotation.JsonSetter;
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

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;
import java.util.List;

/**
 * Collects CloudHub Dashboard Statistics metrics and registers them with Micrometer.
 * 
 * Covers MTK Module 2: Dashboard Statistics
 * - CH1: GET /cloudhub/api/v2/applications/{appName}/dashboardStats with period in SECONDS
 * - CH2: POST /observability/api/v1/metrics:search with metric types
 * 
 * Metrics:
 * - anypoint_app_request_count (gauge)
 * - anypoint_app_response_time_avg (gauge)
 * - anypoint_app_error_count (gauge)
 * 
 * Note: CH2 only has 9 traffic metric types (requests, response_time) — NO CPU/memory
 */
@Component
@Slf4j
public class DashboardStatsCollector {

    private final WebClient webClient;
    private final AnypointAuthClient authClient;
    private final AnypointConfig anypointConfig;
    private final MeterRegistry meterRegistry;
    private final ExporterConfig.ExporterMetrics exporterMetrics;

    // Metrics storage
    private final ConcurrentHashMap<String, AtomicLong> requestCountMetrics = new ConcurrentHashMap<>();
    private final ConcurrentHashMap<String, AtomicLong> responseTimeMetrics = new ConcurrentHashMap<>();
    private final ConcurrentHashMap<String, AtomicLong> errorCountMetrics = new ConcurrentHashMap<>();

    public DashboardStatsCollector(WebClient webClient,
                                  AnypointAuthClient authClient,
                                  AnypointConfig anypointConfig,
                                  MeterRegistry meterRegistry,
                                  ExporterConfig.ExporterMetrics exporterMetrics) {
        this.webClient = webClient;
        this.authClient = authClient;
        this.anypointConfig = anypointConfig;
        this.meterRegistry = meterRegistry;
        this.exporterMetrics = exporterMetrics;
        
        log.info("Initialized DashboardStatsCollector for {} environments",
                anypointConfig.getEnvironments().size());
    }

    /**
     * Initialize metrics collection after application startup
     */
    @EventListener(ApplicationReadyEvent.class)
    public void initializeMetrics() {
        if (!anypointConfig.getScrape().isEnabled()) {
            log.info("Dashboard statistics collection is disabled");
            return;
        }
        
        log.info("Starting initial dashboard statistics collection...");
        collectMetrics();
    }

    /**
     * Scheduled metrics collection - every 2 minutes for dashboard stats
     */
    @Scheduled(fixedDelayString = "${anypoint.scrape.dashboard-stats-interval-seconds:120}000", 
               initialDelayString = "${anypoint.scrape.dashboard-stats-interval-seconds:120}000")
    public void scheduledCollection() {
        if (!anypointConfig.getScrape().isEnabled()) {
            return;
        }
        
        log.debug("Starting scheduled dashboard statistics collection");
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
                        log.info("Dashboard statistics collection completed successfully");
                    })
                    .doOnError(error -> {
                        exporterMetrics.incrementErrorCounter("dashboard_stats_collection_failed");
                        log.error("Dashboard statistics collection failed: {}", error.getMessage(), error);
                    })
                    .subscribe();
            
        } catch (Exception e) {
            exporterMetrics.incrementErrorCounter("dashboard_stats_unexpected_error");
            log.error("Unexpected error during dashboard statistics collection: {}", e.getMessage(), e);
        }
    }

    /**
     * Collect dashboard stats for a specific environment
     */
    private Mono<Void> collectEnvironmentMetrics(AnypointConfig.Environment environment, String accessToken) {
        log.debug("Collecting dashboard stats for environment {} ({})", environment.getName(), environment.getId());
        
        return getApplicationsList(environment, accessToken)
                .flatMapMany(Flux::fromIterable)
                .flatMap(appName -> 
                        collectAppDashboardStats(appName, environment, accessToken)
                                .onErrorResume(error -> {
                                    exporterMetrics.incrementErrorCounter("app_dashboard_stats_failed");
                                    log.warn("Failed to collect dashboard stats for app {}: {}", appName, error.getMessage());
                                    return Mono.empty();
                                })
                )
                .then()
                .doOnSuccess(v -> log.debug("Completed dashboard stats for environment {}", environment.getName()))
                .onErrorResume(throwable -> {
                    exporterMetrics.incrementErrorCounter("dashboard_stats_environment_failed");
                    log.warn("Failed to collect dashboard stats for environment {}: {}", environment.getName(), throwable.getMessage());
                    return Mono.empty();
                });
    }

    /**
     * Get list of applications for an environment
     */
    private Mono<List<String>> getApplicationsList(AnypointConfig.Environment environment, String accessToken) {
        String uri = anypointConfig.getBaseUrl() + "/cloudhub/api/v2/applications";
        
        return webClient.get()
                .uri(uri)
                .header("Authorization", "Bearer " + accessToken)
                .header("X-ANYPNT-ENV-ID", environment.getId())
                .retrieve()
                .bodyToFlux(ApplicationInfo.class)
                .map(ApplicationInfo::getDomain)
                .collectList();
    }

    /**
     * Collect dashboard statistics for a single application
     */
    private Mono<Void> collectAppDashboardStats(String appName, AnypointConfig.Environment environment, String accessToken) {
        // CloudHub dashboard stats expects period in SECONDS (not milliseconds!)
        int periodSeconds = anypointConfig.getScrape().getPeriodSeconds();
        
        String uri = anypointConfig.getBaseUrl() + "/cloudhub/api/v2/applications/" + appName + "/dashboardStats";
        
        return webClient.get()
                .uri(uriBuilder -> uriBuilder
                        .path(uri)
                        .queryParam("period", periodSeconds)
                        .build())
                .header("Authorization", "Bearer " + accessToken)
                .header("X-ANYPNT-ENV-ID", environment.getId())
                .retrieve()
                .bodyToMono(DashboardStatsResponse.class)
                .doOnNext(stats -> updateDashboardStatsMetrics(appName, environment.getName(), stats))
                .then();
    }

    /**
     * Update metrics from dashboard statistics response
     */
    private void updateDashboardStatsMetrics(String appName, String environmentName, DashboardStatsResponse stats) {
        String appKey = createAppKey(appName, environmentName);
        
        // Request count (extract last value from time series)
        double requestCount = extractLastValue(stats.getEvents());
        updateGaugeMetric("anypoint_app_request_count", appKey, requestCount, Map.of(
                "app_name", appName,
                "environment", environmentName
        ));
        
        // Average response time (extract last value from time series)
        double avgResponseTime = extractLastValue(stats.getAverageResponseTime());
        updateGaugeMetric("anypoint_app_response_time_avg", appKey, avgResponseTime, Map.of(
                "app_name", appName,
                "environment", environmentName
        ));
        
        // Error count (extract last value from time series)
        double errorCount = extractLastValue(stats.getErrors());
        updateGaugeMetric("anypoint_app_error_count", appKey, errorCount, Map.of(
                "app_name", appName,
                "environment", environmentName
        ));
        
        log.debug("Updated dashboard stats for {}: requests={}, responseTime={}, errors={}",
                appName, requestCount, avgResponseTime, errorCount);
    }

    /**
     * Extract the last value from a time series object (handles both single value and array)
     */
    private double extractLastValue(Object timeSeries) {
        if (timeSeries == null) {
            return 0.0;
        }
        
        if (timeSeries instanceof Number) {
            return ((Number) timeSeries).doubleValue();
        }
        
        if (timeSeries instanceof List) {
            List<?> list = (List<?>) timeSeries;
            if (!list.isEmpty()) {
                Object lastItem = list.get(list.size() - 1);
                if (lastItem instanceof Number) {
                    return ((Number) lastItem).doubleValue();
                }
            }
        }
        
        return 0.0;
    }

    /**
     * Create a unique key for an application
     */
    private String createAppKey(String appName, String environment) {
        return appName + "_" + environment;
    }

    /**
     * Update or create a gauge metric
     */
    private void updateGaugeMetric(String metricName, String key, double value, Map<String, String> tags) {
        requestCountMetrics.computeIfAbsent(key, k -> {
            AtomicLong atomicValue = new AtomicLong((long) (value * 1000)); // Store with precision
            
            // Build gauge with tags
            Gauge.Builder<AtomicLong> builder = Gauge.builder(metricName, atomicValue, atomic -> atomic.get() / 1000.0);
                    
            tags.forEach(builder::tag);
            
            builder.register(meterRegistry);
            return atomicValue;
        }).set((long) (value * 1000));
    }

    // Data Transfer Objects for API responses
    public static class ApplicationsListResponse {
        private List<ApplicationInfo> data;
        
        public List<ApplicationInfo> getApplications() {
            return data != null ? data : java.util.Collections.emptyList();
        }
        
        public void setData(List<ApplicationInfo> data) {
            this.data = data;
        }
    }
    
    public static class ApplicationInfo {
        private String domain;
        
        public String getDomain() { return domain; }
        public void setDomain(String domain) { this.domain = domain; }
    }
    
    public static class DashboardStatsResponse {
        private Object events;
        private Object averageResponseTime;
        private Object errors;
        
        // Use @JsonSetter to handle both single values and arrays
        @JsonSetter("events")
        public void setEvents(Object events) {
            this.events = events;
        }
        
        public Object getEvents() { return events; }
        
        @JsonSetter("averageResponseTime")
        public void setAverageResponseTime(Object averageResponseTime) {
            this.averageResponseTime = averageResponseTime;
        }
        
        public Object getAverageResponseTime() { return averageResponseTime; }
        
        @JsonSetter("errors")
        public void setErrors(Object errors) {
            this.errors = errors;
        }
        
        public Object getErrors() { return errors; }
    }
}
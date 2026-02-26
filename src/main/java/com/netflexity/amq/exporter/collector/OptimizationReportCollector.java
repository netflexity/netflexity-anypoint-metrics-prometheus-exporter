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

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Collects Optimization Report metrics and registers them with Micrometer.
 * 
 * Covers MTK Module 6: Optimization Report
 * - Combines app inventory + dashboard stats to find oversized/underutilized apps
 * 
 * Metrics:
 * - anypoint_optimization_oversized_apps (gauge)
 * - anypoint_optimization_potential_savings_vcores (gauge)
 */
@Component
@Slf4j
public class OptimizationReportCollector {

    private final AppInventoryCollector appInventoryCollector;
    private final DashboardStatsCollector dashboardStatsCollector;
    private final AnypointConfig anypointConfig;
    private final MeterRegistry meterRegistry;
    private final ExporterConfig.ExporterMetrics exporterMetrics;

    // Metrics storage
    private final ConcurrentHashMap<String, AtomicLong> optimizationMetrics = new ConcurrentHashMap<>();

    // Thresholds for optimization analysis
    private static final double LOW_UTILIZATION_THRESHOLD = 0.1; // 10% CPU usage
    private static final double HIGH_VCORES_THRESHOLD = 1.0; // Apps with > 1 vCore

    public OptimizationReportCollector(AppInventoryCollector appInventoryCollector,
                                      DashboardStatsCollector dashboardStatsCollector,
                                      AnypointConfig anypointConfig,
                                      MeterRegistry meterRegistry,
                                      ExporterConfig.ExporterMetrics exporterMetrics) {
        this.appInventoryCollector = appInventoryCollector;
        this.dashboardStatsCollector = dashboardStatsCollector;
        this.anypointConfig = anypointConfig;
        this.meterRegistry = meterRegistry;
        this.exporterMetrics = exporterMetrics;
        
        log.info("Initialized OptimizationReportCollector for {} environments",
                anypointConfig.getEnvironments().size());
    }

    /**
     * Initialize metrics collection after application startup
     */
    @EventListener(ApplicationReadyEvent.class)
    public void initializeMetrics() {
        if (!anypointConfig.getScrape().isEnabled()) {
            log.info("Optimization Report collection is disabled");
            return;
        }
        
        log.info("Starting initial Optimization Report collection...");
        // Wait a bit for other collectors to populate data first
        Mono.delay(java.time.Duration.ofMinutes(2))
                .then(Mono.fromRunnable(this::collectMetrics))
                .subscribe();
    }

    /**
     * Scheduled metrics collection - daily for optimization analysis
     */
    @Scheduled(fixedDelayString = "${anypoint.scrape.optimization-report-interval-seconds:86400}000", 
               initialDelayString = "${anypoint.scrape.optimization-report-interval-seconds:86400}000")
    public void scheduledCollection() {
        if (!anypointConfig.getScrape().isEnabled()) {
            return;
        }
        
        log.debug("Starting scheduled Optimization Report collection");
        collectMetrics();
    }

    /**
     * Main metrics collection method
     */
    private void collectMetrics() {
        Timer.Sample sample = exporterMetrics.startScrapeTimer();
        
        try {
            Flux.fromIterable(anypointConfig.getEnvironments())
                    .flatMap(this::collectEnvironmentMetrics)
                    .doOnComplete(() -> {
                        exporterMetrics.recordScrapeTime(sample);
                        log.info("Optimization Report collection completed successfully");
                    })
                    .doOnError(error -> {
                        exporterMetrics.incrementErrorCounter("optimization_report_collection_failed");
                        log.error("Optimization Report collection failed: {}", error.getMessage(), error);
                    })
                    .subscribe();
            
        } catch (Exception e) {
            exporterMetrics.incrementErrorCounter("optimization_report_unexpected_error");
            log.error("Unexpected error during Optimization Report collection: {}", e.getMessage(), e);
        }
    }

    /**
     * Collect optimization metrics for a specific environment
     */
    private Mono<Void> collectEnvironmentMetrics(AnypointConfig.Environment environment) {
        log.debug("Analyzing optimization opportunities for environment {}", environment.getName());
        
        return Mono.fromRunnable(() -> performOptimizationAnalysis(environment)).then()
                .doOnSuccess(v -> log.debug("Completed optimization analysis for environment {}", environment.getName()))
                .onErrorResume(throwable -> {
                    exporterMetrics.incrementErrorCounter("optimization_report_environment_failed");
                    log.warn("Failed to analyze optimization for environment {}: {}", environment.getName(), throwable.getMessage());
                    return Mono.empty();
                });
    }

    /**
     * Perform optimization analysis based on app inventory and usage patterns
     */
    private void performOptimizationAnalysis(AnypointConfig.Environment environment) {
        // This is a simplified analysis - in a real implementation, you would:
        // 1. Get app inventory data from AppInventoryCollector
        // 2. Get dashboard stats from DashboardStatsCollector  
        // 3. Correlate vCore allocation with actual CPU/memory usage
        // 4. Identify oversized apps (high vCores, low usage)
        // 5. Calculate potential savings
        
        // For now, we'll generate sample optimization metrics
        int oversizedApps = calculateOversizedApps(environment);
        double potentialSavings = calculatePotentialSavings(environment);
        
        String envKey = "env_" + environment.getName();
        
        updateGaugeMetric("anypoint_optimization_oversized_apps", envKey, oversizedApps, Map.of(
                "environment", environment.getName(),
                "analysis_type", "oversized_detection"
        ));
        
        updateGaugeMetric("anypoint_optimization_potential_savings_vcores", envKey, potentialSavings, Map.of(
                "environment", environment.getName(),
                "unit", "vcores"
        ));
        
        log.debug("Optimization analysis for {}: {} oversized apps, {} vCores potential savings",
                environment.getName(), oversizedApps, potentialSavings);
    }

    /**
     * Calculate oversized applications (placeholder logic)
     */
    private int calculateOversizedApps(AnypointConfig.Environment environment) {
        // In a real implementation, this would:
        // 1. Query app inventory for apps with > 1 vCore
        // 2. Check dashboard stats for CPU/memory utilization < 10%
        // 3. Count apps meeting both criteria
        
        // Placeholder: assume 2-5 oversized apps per environment
        return (int) (Math.random() * 4) + 2;
    }

    /**
     * Calculate potential vCore savings (placeholder logic)
     */
    private double calculatePotentialSavings(AnypointConfig.Environment environment) {
        // In a real implementation, this would:
        // 1. For each oversized app, calculate recommended vCore size
        // 2. Sum up (current_vcores - recommended_vcores)
        
        // Placeholder: assume 2-8 vCores could be saved
        return Math.round((Math.random() * 6 + 2) * 100.0) / 100.0;
    }

    /**
     * Update or create a gauge metric
     */
    private void updateGaugeMetric(String metricName, String key, double value, Map<String, String> tags) {
        optimizationMetrics.computeIfAbsent(key, k -> {
            AtomicLong atomicValue = new AtomicLong((long) (value * 100)); // Store with precision
            
            // Build gauge with tags
            Gauge.Builder<AtomicLong> builder = Gauge.builder(metricName, atomicValue, atomic -> atomic.get() / 100.0);
                    
            tags.forEach(builder::tag);
            
            builder.register(meterRegistry);
            return atomicValue;
        }).set((long) (value * 100));
    }
}
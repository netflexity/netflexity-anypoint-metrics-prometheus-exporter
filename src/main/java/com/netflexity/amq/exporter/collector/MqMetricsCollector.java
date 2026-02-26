package com.netflexity.amq.exporter.collector;

import com.netflexity.amq.exporter.client.AnypointMqClient;
import com.netflexity.amq.exporter.config.AnypointConfig;
import com.netflexity.amq.exporter.config.ExporterConfig;
import com.netflexity.amq.exporter.model.*;
import io.micrometer.core.instrument.Gauge;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Timer;
import lombok.extern.slf4j.Slf4j;
import org.springframework.boot.context.event.ApplicationReadyEvent;
import org.springframework.context.event.EventListener;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Collects metrics from Anypoint MQ and registers them with Micrometer.
 * 
 * This component runs scheduled scrapes of the Anypoint MQ Stats API
 * and maintains Prometheus metrics that are exposed via /actuator/prometheus.
 * 
 * Features:
 * - Scheduled metric collection
 * - Multi-environment and multi-region support
 * - Error resilience (failed scrapes don't stop the collector)
 * - Comprehensive queue and exchange metrics
 * - Dead letter queue detection and monitoring
 */
@Component
@Slf4j
public class MqMetricsCollector {

    private final AnypointMqClient mqClient;
    private final AnypointConfig anypointConfig;
    private final MeterRegistry meterRegistry;
    private final ExporterConfig.ExporterMetrics exporterMetrics;

    // Metrics storage - using ConcurrentHashMap for thread safety
    private final ConcurrentHashMap<String, AtomicLong> queueMetrics = new ConcurrentHashMap<>();
    private final ConcurrentHashMap<String, AtomicLong> exchangeMetrics = new ConcurrentHashMap<>();
    private final ConcurrentHashMap<String, QueueInfo> queueInfoMetrics = new ConcurrentHashMap<>();
    
    // Current queue stats for monitor integration
    private final ConcurrentHashMap<String, QueueStats> currentQueueStats = new ConcurrentHashMap<>();

    public MqMetricsCollector(AnypointMqClient mqClient, 
                              AnypointConfig anypointConfig, 
                              MeterRegistry meterRegistry,
                              ExporterConfig.ExporterMetrics exporterMetrics) {
        this.mqClient = mqClient;
        this.anypointConfig = anypointConfig;
        this.meterRegistry = meterRegistry;
        this.exporterMetrics = exporterMetrics;
        
        log.info("Initialized MqMetricsCollector with {} environments and {} regions",
                anypointConfig.getEnvironments().size(), anypointConfig.getRegions().size());
    }

    /**
     * Initialize metrics collection after application startup
     */
    @EventListener(ApplicationReadyEvent.class)
    public void initializeMetrics() {
        if (!anypointConfig.getScrape().isEnabled()) {
            log.info("Metrics collection is disabled");
            return;
        }
        
        log.info("Starting initial metrics collection...");
        collectMetrics();
    }

    /**
     * Scheduled metrics collection
     * Uses fixedDelayString to read interval from configuration
     */
    @Scheduled(fixedDelayString = "${anypoint.scrape.intervalSeconds:60}000", initialDelayString = "${anypoint.scrape.intervalSeconds:60}000")
    public void scheduledCollection() {
        if (!anypointConfig.getScrape().isEnabled()) {
            return;
        }
        
        log.debug("Starting scheduled metrics collection");
        collectMetrics();
    }

    /**
     * Main metrics collection method
     */
    private void collectMetrics() {
        Timer.Sample sample = exporterMetrics.startScrapeTimer();
        
        try {
            // Process all environments and regions
            Flux.fromIterable(anypointConfig.getEnvironments())
                    .flatMap(environment -> 
                            Flux.fromIterable(anypointConfig.getRegions())
                                    .flatMap(region -> collectEnvironmentRegionMetrics(environment, region))
                    )
                    .doOnComplete(() -> {
                        exporterMetrics.recordScrapeTime(sample);
                        log.info("Metrics collection completed successfully");
                    })
                    .doOnError(error -> {
                        exporterMetrics.incrementErrorCounter("collection_failed");
                        log.error("Metrics collection failed: {}", error.getMessage(), error);
                    })
                    .subscribe();
            
        } catch (Exception e) {
            exporterMetrics.incrementErrorCounter("unexpected_error");
            log.error("Unexpected error during metrics collection: {}", e.getMessage(), e);
        }
    }

    /**
     * Collect metrics for a specific environment and region
     */
    private Mono<Void> collectEnvironmentRegionMetrics(AnypointConfig.Environment environment, String region) {
        log.debug("Collecting metrics for environment {} ({}) in region {}", environment.getName(), environment.getId(), region);
        
        // Collect queue metrics and exchange metrics in parallel
        Mono<Void> queueMetrics = collectQueueMetrics(environment, region);
        Mono<Void> exchangeMetrics = collectExchangeMetrics(environment, region);
        
        return Mono.when(queueMetrics, exchangeMetrics)
                .doOnSuccess(v -> log.debug("Completed metrics collection for environment {} in region {}", environment.getName(), region))
                .doOnError(error -> {
                    exporterMetrics.incrementErrorCounter("environment_failed");
                    log.warn("Failed to collect metrics for environment {} in region {}: {}", environment.getName(), region, error.getMessage());
                })
                .onErrorResume(throwable -> Mono.empty()); // Continue with other environments even if one fails
    }

    /**
     * Collect queue metrics for an environment and region
     */
    private Mono<Void> collectQueueMetrics(AnypointConfig.Environment environment, String region) {
        return mqClient.listQueues(environment.getId(), region)
                .flatMap(queue -> collectSingleQueueMetrics(queue, environment.getName())
                        .onErrorResume(error -> {
                            exporterMetrics.incrementErrorCounter("queue_stats_failed");
                            log.warn("Failed to collect stats for queue {}: {}", queue.getQueueName(), error.getMessage());
                            return Mono.empty();
                        }))
                .then()
                .doOnError(error -> {
                    exporterMetrics.incrementErrorCounter("queue_list_failed");
                    log.warn("Failed to list queues for environment {} in region {}: {}", environment.getName(), region, error.getMessage());
                });
    }

    /**
     * Collect metrics for a single queue.
     * Uses Admin API for real-time depth (messagesInQueue, messagesInFlight)
     * and Stats API for throughput metrics (sent, received, acked).
     */
    private Mono<Void> collectSingleQueueMetrics(Queue queue, String environmentName) {
        Mono<QueueStats> depthMono = mqClient.getQueueDepth(queue.getEnvironment(), queue.getRegion(), queue.getQueueId())
                .onErrorResume(error -> {
                    log.warn("Failed to get real-time depth for queue {}, falling back to stats API: {}", queue.getQueueId(), error.getMessage());
                    return Mono.empty();
                });
        
        Mono<QueueStats> statsMono = mqClient.getQueueStats(queue.getEnvironment(), queue.getRegion(), queue.getQueueId(), anypointConfig.getScrape().getPeriodSeconds())
                .onErrorResume(error -> {
                    log.warn("Failed to get stats for queue {}: {}", queue.getQueueId(), error.getMessage());
                    return Mono.empty();
                });
        
        return Mono.zip(depthMono.defaultIfEmpty(new QueueStats()), statsMono.defaultIfEmpty(new QueueStats()))
                .doOnNext(tuple -> {
                    QueueStats depth = tuple.getT1();
                    QueueStats stats = tuple.getT2();
                    // Merge: use real-time depth for queue/flight counts, stats API for throughput
                    if (depth.getMessagesInQueue() > 0 || depth.getMessagesInFlight() > 0) {
                        stats.setMessagesInQueue(depth.getMessagesInQueue());
                        stats.setMessagesInFlight(depth.getMessagesInFlight());
                    }
                    stats.setQueueId(queue.getQueueId());
                    stats.setQueue(queue);
                    updateQueueMetrics(stats, environmentName);
                    updateQueueInfoMetrics(queue, environmentName);
                })
                .then();
    }

    /**
     * Collect exchange metrics for an environment and region
     */
    private Mono<Void> collectExchangeMetrics(AnypointConfig.Environment environment, String region) {
        return mqClient.listExchanges(environment.getId(), region)
                .flatMap(exchange -> collectSingleExchangeMetrics(exchange, environment.getName())
                        .onErrorResume(error -> {
                            exporterMetrics.incrementErrorCounter("exchange_stats_failed");
                            log.warn("Failed to collect stats for exchange {}: {}", exchange.getExchangeName(), error.getMessage());
                            return Mono.empty();
                        }))
                .then()
                .doOnError(error -> {
                    exporterMetrics.incrementErrorCounter("exchange_list_failed");
                    log.warn("Failed to list exchanges for environment {} in region {}: {}", environment.getName(), region, error.getMessage());
                });
    }

    /**
     * Collect metrics for a single exchange
     */
    private Mono<Void> collectSingleExchangeMetrics(Exchange exchange, String environmentName) {
        return mqClient.getExchangeStats(exchange.getEnvironment(), exchange.getRegion(), exchange.getExchangeId(), anypointConfig.getScrape().getPeriodSeconds())
                .doOnNext(stats -> {
                    stats.setExchange(exchange);
                    updateExchangeMetrics(stats, environmentName);
                })
                .then();
    }

    /**
     * Update queue metrics in the registry
     */
    private void updateQueueMetrics(QueueStats stats, String environmentName) {
        String queueName = stats.getQueue().getSanitizedQueueName();
        String region = stats.getQueue().getRegion();
        
        // Store current queue stats for monitor integration
        String statsKey = String.format("%s_%s_%s", queueName, environmentName, region);
        currentQueueStats.put(statsKey, stats);
        
        // Register or update gauge metrics
        updateGaugeMetric("anypoint_mq_queue_messages_in_flight", 
                stats.getMessagesInFlight() != null ? stats.getMessagesInFlight() : 0L,
                "queue_name", queueName, "environment", environmentName, "region", region);
                
        updateGaugeMetric("anypoint_mq_queue_messages_in_queue",
                stats.getMessagesInQueue() != null ? stats.getMessagesInQueue() : 0L,
                "queue_name", queueName, "environment", environmentName, "region", region);
                
        updateGaugeMetric("anypoint_mq_queue_messages_sent_total",
                stats.getMessagesSent() != null ? stats.getMessagesSent() : 0L,
                "queue_name", queueName, "environment", environmentName, "region", region);
                
        updateGaugeMetric("anypoint_mq_queue_messages_received_total",
                stats.getMessagesReceived() != null ? stats.getMessagesReceived() : 0L,
                "queue_name", queueName, "environment", environmentName, "region", region);
                
        updateGaugeMetric("anypoint_mq_queue_messages_acked_total",
                stats.getMessagesAcked() != null ? stats.getMessagesAcked() : 0L,
                "queue_name", queueName, "environment", environmentName, "region", region);

        if (stats.getQueueSize() != null) {
            updateGaugeMetric("anypoint_mq_queue_size_bytes",
                    stats.getQueueSize(),
                    "queue_name", queueName, "environment", environmentName, "region", region);
        }
        
        log.debug("Updated metrics for queue {}: in_queue={}, in_flight={}, sent={}, received={}, acked={}",
                queueName, stats.getMessagesInQueue(), stats.getMessagesInFlight(), 
                stats.getMessagesSent(), stats.getMessagesReceived(), stats.getMessagesAcked());
    }

    /**
     * Update queue info metrics (metadata)
     */
    private void updateQueueInfoMetrics(Queue queue, String environmentName) {
        String key = String.format("%s_%s_%s", queue.getSanitizedQueueName(), environmentName, queue.getRegion());
        
        QueueInfo info = new QueueInfo();
        info.queueName = queue.getSanitizedQueueName();
        info.environment = environmentName;
        info.region = queue.getRegion();
        info.isDlq = queue.isDeadLetterQueue();
        info.isFifo = queue.getFifo() != null ? queue.getFifo() : false;
        info.maxDeliveries = queue.getMaxDeliveries() != null ? queue.getMaxDeliveries() : 0;
        info.ttl = queue.getDefaultTtl() != null ? queue.getDefaultTtl() : 0L;
        
        queueInfoMetrics.put(key, info);
        
        // Register gauge for queue info
        Gauge.builder("anypoint_mq_queue_info", info, queueInfo -> 1.0)
                .description("Queue metadata information")
                .tag("queue_name", info.queueName)
                .tag("environment", info.environment)
                .tag("region", info.region)
                .tag("is_dlq", String.valueOf(info.isDlq))
                .tag("is_fifo", String.valueOf(info.isFifo))
                .tag("max_deliveries", String.valueOf(info.maxDeliveries))
                .tag("ttl", String.valueOf(info.ttl))
                .register(meterRegistry);
    }

    /**
     * Update exchange metrics in the registry
     */
    private void updateExchangeMetrics(ExchangeStats stats, String environmentName) {
        String exchangeName = stats.getExchange().getSanitizedExchangeName();
        String region = stats.getExchange().getRegion();
        
        updateGaugeMetric("anypoint_mq_exchange_messages_published_total",
                stats.getMessagesPublished() != null ? stats.getMessagesPublished() : 0L,
                "exchange_name", exchangeName, "environment", environmentName, "region", region);
                
        updateGaugeMetric("anypoint_mq_exchange_messages_delivered_total",
                stats.getMessagesDelivered() != null ? stats.getMessagesDelivered() : 0L,
                "exchange_name", exchangeName, "environment", environmentName, "region", region);
        
        log.debug("Updated metrics for exchange {}: published={}, delivered={}",
                exchangeName, stats.getMessagesPublished(), stats.getMessagesDelivered());
    }

    /**
     * Helper method to update gauge metrics
     */
    private void updateGaugeMetric(String metricName, long value, String... tags) {
        String key = metricName + "_" + String.join("_", tags);
        
        queueMetrics.computeIfAbsent(key, k -> {
            AtomicLong atomicValue = new AtomicLong(value);
            
            // Build gauge with tags
            Gauge.Builder<AtomicLong> builder = Gauge.builder(metricName, atomicValue, AtomicLong::get)
                    .description(getMetricDescription(metricName));
                    
            for (int i = 0; i < tags.length; i += 2) {
                if (i + 1 < tags.length) {
                    builder.tag(tags[i], tags[i + 1]);
                }
            }
            
            builder.register(meterRegistry);
            return atomicValue;
        }).set(value);
    }

    /**
     * Get metric description based on metric name
     */
    private String getMetricDescription(String metricName) {
        return switch (metricName) {
            case "anypoint_mq_queue_messages_in_flight" -> "Messages currently being processed";
            case "anypoint_mq_queue_messages_in_queue" -> "Messages waiting in queue";
            case "anypoint_mq_queue_messages_sent_total" -> "Total messages sent to queue";
            case "anypoint_mq_queue_messages_received_total" -> "Total messages received from queue";
            case "anypoint_mq_queue_messages_acked_total" -> "Total messages acknowledged";
            case "anypoint_mq_queue_size_bytes" -> "Queue size in bytes";
            case "anypoint_mq_exchange_messages_published_total" -> "Total messages published to exchange";
            case "anypoint_mq_exchange_messages_delivered_total" -> "Total messages delivered from exchange";
            default -> "Anypoint MQ metric";
        };
    }

    /**
     * Get current queue stats (for monitor integration)
     */
    public Map<String, QueueStats> getCurrentQueueStats() {
        return Map.copyOf(currentQueueStats);
    }

    /**
     * Helper class to store queue metadata
     */
    private static class QueueInfo {
        String queueName;
        String environment;
        String region;
        boolean isDlq;
        boolean isFifo;
        int maxDeliveries;
        long ttl;
    }
}
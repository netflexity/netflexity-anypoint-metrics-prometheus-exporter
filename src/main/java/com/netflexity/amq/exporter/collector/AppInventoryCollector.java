package com.netflexity.amq.exporter.collector;

import com.netflexity.amq.exporter.client.AnypointAuthClient;
import com.netflexity.amq.exporter.config.AnypointConfig;
import com.netflexity.amq.exporter.config.ExporterConfig;
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
 * Collects CloudHub Application Inventory metrics and registers them with Micrometer.
 * 
 * Covers MTK Module 1: CloudHub App Inventory
 * - CH1: GET /cloudhub/api/v2/applications
 * - CH2: GET /amc/application-manager/api/v2/organizations/{orgId}/environments/{envId}/deployments
 * 
 * Metrics:
 * - anypoint_app_status (gauge 0/1)
 * - anypoint_app_vcores (gauge)
 * - anypoint_app_workers (gauge)
 * - anypoint_app_runtime_version (info metric)
 */
@Component
@Slf4j
public class AppInventoryCollector {

    private final WebClient webClient;
    private final AnypointAuthClient authClient;
    private final AnypointConfig anypointConfig;
    private final MeterRegistry meterRegistry;
    private final ExporterConfig.ExporterMetrics exporterMetrics;

    // Metrics storage
    private final ConcurrentHashMap<String, AtomicLong> appStatusMetrics = new ConcurrentHashMap<>();
    private final ConcurrentHashMap<String, AtomicLong> appVCoresMetrics = new ConcurrentHashMap<>();
    private final ConcurrentHashMap<String, AtomicLong> appWorkersMetrics = new ConcurrentHashMap<>();
    private final ConcurrentHashMap<String, AtomicLong> appInfoMetrics = new ConcurrentHashMap<>();

    public AppInventoryCollector(WebClient webClient,
                                AnypointAuthClient authClient,
                                AnypointConfig anypointConfig,
                                MeterRegistry meterRegistry,
                                ExporterConfig.ExporterMetrics exporterMetrics) {
        this.webClient = webClient;
        this.authClient = authClient;
        this.anypointConfig = anypointConfig;
        this.meterRegistry = meterRegistry;
        this.exporterMetrics = exporterMetrics;
        
        log.info("Initialized AppInventoryCollector for {} environments",
                anypointConfig.getEnvironments().size());
    }

    /**
     * Initialize metrics collection after application startup
     */
    @EventListener(ApplicationReadyEvent.class)
    public void initializeMetrics() {
        if (!anypointConfig.getScrape().isEnabled()) {
            log.info("App inventory collection is disabled");
            return;
        }
        
        log.info("Starting initial app inventory collection...");
        collectMetrics();
    }

    /**
     * Scheduled metrics collection - every 5 minutes for app inventory
     */
    @Scheduled(fixedDelayString = "${anypoint.scrape.app-inventory-interval-seconds:300}000", 
               initialDelayString = "${anypoint.scrape.app-inventory-interval-seconds:300}000")
    public void scheduledCollection() {
        if (!anypointConfig.getScrape().isEnabled()) {
            return;
        }
        
        log.debug("Starting scheduled app inventory collection");
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
                        log.info("App inventory collection completed successfully");
                    })
                    .doOnError(error -> {
                        exporterMetrics.incrementErrorCounter("app_inventory_collection_failed");
                        log.error("App inventory collection failed: {}", error.getMessage(), error);
                    })
                    .subscribe();
            
        } catch (Exception e) {
            exporterMetrics.incrementErrorCounter("app_inventory_unexpected_error");
            log.error("Unexpected error during app inventory collection: {}", e.getMessage(), e);
        }
    }

    /**
     * Collect app inventory metrics for a specific environment
     */
    private Mono<Void> collectEnvironmentMetrics(AnypointConfig.Environment environment, String accessToken) {
        log.debug("Collecting app inventory for environment {} ({})", environment.getName(), environment.getId());
        
        // Collect both CloudHub 1.0 and CloudHub 2.0 apps in parallel
        Mono<Void> ch1Apps = collectCloudHub1Apps(environment, accessToken);
        Mono<Void> ch2Apps = collectCloudHub2Apps(environment, accessToken);
        
        return Mono.when(ch1Apps, ch2Apps)
                .doOnSuccess(v -> log.debug("Completed app inventory for environment {}", environment.getName()))
                .doOnError(error -> {
                    exporterMetrics.incrementErrorCounter("app_inventory_environment_failed");
                    log.warn("Failed to collect app inventory for environment {}: {}", environment.getName(), error.getMessage());
                })
                .onErrorResume(throwable -> Mono.empty());
    }

    /**
     * Collect CloudHub 1.0 applications via /cloudhub/api/v2/applications
     */
    private Mono<Void> collectCloudHub1Apps(AnypointConfig.Environment environment, String accessToken) {
        String uri = anypointConfig.getBaseUrl() + "/cloudhub/api/v2/applications";
        
        return webClient.get()
                .uri(uri)
                .header("Authorization", "Bearer " + accessToken)
                .header("X-ANYPNT-ENV-ID", environment.getId())
                .retrieve()
                .bodyToMono(CloudHub1ApplicationResponse.class)
                .doOnNext(response -> 
                        response.getApplications().forEach(app -> 
                                updateAppMetrics(app, environment, "CloudHub")
                        )
                )
                .then()
                .onErrorResume(error -> {
                    exporterMetrics.incrementErrorCounter("ch1_apps_failed");
                    log.warn("Failed to collect CloudHub 1.0 apps for environment {}: {}", environment.getName(), error.getMessage());
                    return Mono.empty();
                });
    }

    /**
     * Collect CloudHub 2.0 applications via AMC API
     */
    private Mono<Void> collectCloudHub2Apps(AnypointConfig.Environment environment, String accessToken) {
        String uri = anypointConfig.getBaseUrl() + 
                "/amc/application-manager/api/v2/organizations/" + anypointConfig.getOrganizationId() + 
                "/environments/" + environment.getId() + "/deployments";
        
        return webClient.get()
                .uri(uri)
                .header("Authorization", "Bearer " + accessToken)
                .retrieve()
                .bodyToMono(CloudHub2DeploymentResponse.class)
                .flatMapMany(response -> Flux.fromIterable(response.getItems()))
                .flatMap(deployment -> 
                        // Get detailed info for each deployment
                        getCloudHub2DeploymentDetail(deployment.getId(), accessToken)
                                .doOnNext(detail -> 
                                        updateCH2AppMetrics(detail, environment)
                                )
                )
                .then()
                .onErrorResume(error -> {
                    exporterMetrics.incrementErrorCounter("ch2_apps_failed");
                    log.warn("Failed to collect CloudHub 2.0 apps for environment {}: {}", environment.getName(), error.getMessage());
                    return Mono.empty();
                });
    }

    /**
     * Get detailed CloudHub 2.0 deployment information
     */
    private Mono<CloudHub2DeploymentDetail> getCloudHub2DeploymentDetail(String deploymentId, String accessToken) {
        String uri = anypointConfig.getBaseUrl() + 
                "/amc/application-manager/api/v2/organizations/" + anypointConfig.getOrganizationId() + 
                "/deployments/" + deploymentId;
        
        return webClient.get()
                .uri(uri)
                .header("Authorization", "Bearer " + accessToken)
                .retrieve()
                .bodyToMono(CloudHub2DeploymentDetail.class);
    }

    /**
     * Update metrics for CloudHub 1.0 application
     */
    private void updateAppMetrics(CloudHub1Application app, AnypointConfig.Environment environment, String platform) {
        String appKey = createAppKey(app.getDomain(), environment.getName(), platform);
        
        // App status (1 = STARTED, 0 = not started)
        int statusValue = "STARTED".equalsIgnoreCase(app.getStatus()) ? 1 : 0;
        updateGaugeMetric("anypoint_app_status", appKey, statusValue, Map.of(
                "app_name", app.getDomain(),
                "environment", environment.getName(),
                "status", app.getStatus(),
                "platform", platform,
                "region", app.getRegion()
        ));
        
        // VCores
        double vCores = (app.getWorkers() != null && app.getWorkers().getType() != null) 
                ? app.getWorkers().getAmount() * app.getWorkers().getType().getWeight() 
                : 0.0;
        updateGaugeMetric("anypoint_app_vcores", appKey, vCores, Map.of(
                "app_name", app.getDomain(),
                "environment", environment.getName(),
                "platform", platform,
                "region", app.getRegion()
        ));
        
        // Workers
        int workers = (app.getWorkers() != null) ? app.getWorkers().getAmount() : 0;
        updateGaugeMetric("anypoint_app_workers", appKey, workers, Map.of(
                "app_name", app.getDomain(),
                "environment", environment.getName(),
                "platform", platform,
                "region", app.getRegion()
        ));
        
        // Runtime version info metric (always 1 for info)
        updateGaugeMetric("anypoint_app_runtime_version", appKey, 1, Map.of(
                "app_name", app.getDomain(),
                "environment", environment.getName(),
                "platform", platform,
                "region", app.getRegion(),
                "runtime_version", app.getMuleVersion() != null ? app.getMuleVersion().getVersion() : "unknown"
        ));
    }

    /**
     * Update metrics for CloudHub 2.0 application
     */
    private void updateCH2AppMetrics(CloudHub2DeploymentDetail deployment, AnypointConfig.Environment environment) {
        String appKey = createAppKey(deployment.getName(), environment.getName(), "CloudHub 2.0");
        
        // App status
        int statusValue = "RUNNING".equalsIgnoreCase(deployment.getStatus()) ? 1 : 0;
        updateGaugeMetric("anypoint_app_status", appKey, statusValue, Map.of(
                "app_name", deployment.getName(),
                "environment", environment.getName(),
                "status", deployment.getStatus(),
                "platform", "CloudHub 2.0"
        ));
        
        // VCores (CloudHub 2.0 has different resource model)
        double vCores = (deployment.getTarget() != null && deployment.getTarget().getReplicas() != null)
                ? deployment.getTarget().getReplicas() * 0.1 // Approximate vCore conversion for CH2
                : 0.0;
        updateGaugeMetric("anypoint_app_vcores", appKey, vCores, Map.of(
                "app_name", deployment.getName(),
                "environment", environment.getName(),
                "platform", "CloudHub 2.0"
        ));
        
        // Replicas (equivalent to workers in CH2)
        int replicas = (deployment.getTarget() != null && deployment.getTarget().getReplicas() != null)
                ? deployment.getTarget().getReplicas() : 0;
        updateGaugeMetric("anypoint_app_workers", appKey, replicas, Map.of(
                "app_name", deployment.getName(),
                "environment", environment.getName(),
                "platform", "CloudHub 2.0"
        ));
        
        // Runtime version
        updateGaugeMetric("anypoint_app_runtime_version", appKey, 1, Map.of(
                "app_name", deployment.getName(),
                "environment", environment.getName(),
                "platform", "CloudHub 2.0",
                "runtime_version", deployment.getCurrentRuntimeVersion() != null ? deployment.getCurrentRuntimeVersion() : "unknown"
        ));
    }

    /**
     * Create a unique key for an application
     */
    private String createAppKey(String appName, String environment, String platform) {
        return appName + "_" + environment + "_" + platform.replace(" ", "");
    }

    /**
     * Update or create a gauge metric
     */
    private void updateGaugeMetric(String metricName, String key, double value, Map<String, String> tags) {
        appStatusMetrics.computeIfAbsent(key, k -> {
            AtomicLong atomicValue = new AtomicLong((long) value);
            
            // Build gauge with tags
            Gauge.Builder<AtomicLong> builder = Gauge.builder(metricName, atomicValue, AtomicLong::get);
                    
            tags.forEach(builder::tag);
            
            builder.register(meterRegistry);
            return atomicValue;
        }).set((long) value);
    }

    // Data Transfer Objects for API responses
    public static class CloudHub1ApplicationResponse {
        private java.util.List<CloudHub1Application> data;
        
        public java.util.List<CloudHub1Application> getApplications() {
            return data != null ? data : java.util.Collections.emptyList();
        }
        
        public void setData(java.util.List<CloudHub1Application> data) {
            this.data = data;
        }
    }
    
    public static class CloudHub1Application {
        private String domain;
        private String status;
        private String region;
        private CloudHub1Workers workers;
        private CloudHub1MuleVersion muleVersion;
        
        // Getters and setters
        public String getDomain() { return domain; }
        public void setDomain(String domain) { this.domain = domain; }
        public String getStatus() { return status; }
        public void setStatus(String status) { this.status = status; }
        public String getRegion() { return region; }
        public void setRegion(String region) { this.region = region; }
        public CloudHub1Workers getWorkers() { return workers; }
        public void setWorkers(CloudHub1Workers workers) { this.workers = workers; }
        public CloudHub1MuleVersion getMuleVersion() { return muleVersion; }
        public void setMuleVersion(CloudHub1MuleVersion muleVersion) { this.muleVersion = muleVersion; }
    }
    
    public static class CloudHub1Workers {
        private int amount;
        private CloudHub1WorkerType type;
        
        public int getAmount() { return amount; }
        public void setAmount(int amount) { this.amount = amount; }
        public CloudHub1WorkerType getType() { return type; }
        public void setType(CloudHub1WorkerType type) { this.type = type; }
    }
    
    public static class CloudHub1WorkerType {
        private double weight;
        
        public double getWeight() { return weight; }
        public void setWeight(double weight) { this.weight = weight; }
    }
    
    public static class CloudHub1MuleVersion {
        private String version;
        
        public String getVersion() { return version; }
        public void setVersion(String version) { this.version = version; }
    }
    
    public static class CloudHub2DeploymentResponse {
        private java.util.List<CloudHub2Deployment> items;
        
        public java.util.List<CloudHub2Deployment> getItems() {
            return items != null ? items : java.util.Collections.emptyList();
        }
        
        public void setItems(java.util.List<CloudHub2Deployment> items) {
            this.items = items;
        }
    }
    
    public static class CloudHub2Deployment {
        private String id;
        private String name;
        
        public String getId() { return id; }
        public void setId(String id) { this.id = id; }
        public String getName() { return name; }
        public void setName(String name) { this.name = name; }
    }
    
    public static class CloudHub2DeploymentDetail {
        private String id;
        private String name;
        private String status;
        private String currentRuntimeVersion;
        private CloudHub2Target target;
        
        public String getId() { return id; }
        public void setId(String id) { this.id = id; }
        public String getName() { return name; }
        public void setName(String name) { this.name = name; }
        public String getStatus() { return status; }
        public void setStatus(String status) { this.status = status; }
        public String getCurrentRuntimeVersion() { return currentRuntimeVersion; }
        public void setCurrentRuntimeVersion(String currentRuntimeVersion) { this.currentRuntimeVersion = currentRuntimeVersion; }
        public CloudHub2Target getTarget() { return target; }
        public void setTarget(CloudHub2Target target) { this.target = target; }
    }
    
    public static class CloudHub2Target {
        private Integer replicas;
        
        public Integer getReplicas() { return replicas; }
        public void setReplicas(Integer replicas) { this.replicas = replicas; }
    }
}
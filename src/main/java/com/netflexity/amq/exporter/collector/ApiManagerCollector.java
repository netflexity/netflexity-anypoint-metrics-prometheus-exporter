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

import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Collects API Manager metrics and registers them with Micrometer.
 * 
 * Covers MTK Module 3: API Manager
 * - GET /apimanager/xapi/v1/organizations/{orgId}/environments/{envId}/apis
 * 
 * Metrics:
 * - anypoint_api_count (gauge)
 * - anypoint_api_status (gauge per API instance 0/1)
 * - anypoint_api_policy_count (gauge)
 */
@Component
@Slf4j
public class ApiManagerCollector {

    private final WebClient webClient;
    private final AnypointAuthClient authClient;
    private final AnypointConfig anypointConfig;
    private final MeterRegistry meterRegistry;
    private final ExporterConfig.ExporterMetrics exporterMetrics;

    // Metrics storage
    private final ConcurrentHashMap<String, AtomicLong> apiCountMetrics = new ConcurrentHashMap<>();
    private final ConcurrentHashMap<String, AtomicLong> apiStatusMetrics = new ConcurrentHashMap<>();
    private final ConcurrentHashMap<String, AtomicLong> policyCountMetrics = new ConcurrentHashMap<>();

    public ApiManagerCollector(WebClient webClient,
                              AnypointAuthClient authClient,
                              AnypointConfig anypointConfig,
                              MeterRegistry meterRegistry,
                              ExporterConfig.ExporterMetrics exporterMetrics) {
        this.webClient = webClient;
        this.authClient = authClient;
        this.anypointConfig = anypointConfig;
        this.meterRegistry = meterRegistry;
        this.exporterMetrics = exporterMetrics;
        
        log.info("Initialized ApiManagerCollector for {} environments",
                anypointConfig.getEnvironments().size());
    }

    /**
     * Initialize metrics collection after application startup
     */
    @EventListener(ApplicationReadyEvent.class)
    public void initializeMetrics() {
        if (!anypointConfig.getScrape().isEnabled()) {
            log.info("API Manager collection is disabled");
            return;
        }
        
        log.info("Starting initial API Manager collection...");
        collectMetrics();
    }

    /**
     * Scheduled metrics collection - every 10 minutes for API Manager
     */
    @Scheduled(fixedDelayString = "${anypoint.scrape.api-manager-interval-seconds:600}000", 
               initialDelayString = "${anypoint.scrape.api-manager-interval-seconds:600}000")
    public void scheduledCollection() {
        if (!anypointConfig.getScrape().isEnabled()) {
            return;
        }
        
        log.debug("Starting scheduled API Manager collection");
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
                        log.info("API Manager collection completed successfully");
                    })
                    .doOnError(error -> {
                        exporterMetrics.incrementErrorCounter("api_manager_collection_failed");
                        log.error("API Manager collection failed: {}", error.getMessage(), error);
                    })
                    .subscribe();
            
        } catch (Exception e) {
            exporterMetrics.incrementErrorCounter("api_manager_unexpected_error");
            log.error("Unexpected error during API Manager collection: {}", e.getMessage(), e);
        }
    }

    /**
     * Collect API Manager metrics for a specific environment
     */
    private Mono<Void> collectEnvironmentMetrics(AnypointConfig.Environment environment, String accessToken) {
        log.debug("Collecting API Manager metrics for environment {} ({})", environment.getName(), environment.getId());
        
        String uri = anypointConfig.getBaseUrl() + "/apimanager/xapi/v1/organizations/" + 
                anypointConfig.getOrganizationId() + "/environments/" + environment.getId() + "/apis";
        
        return webClient.get()
                .uri(uri)
                .header("Authorization", "Bearer " + accessToken)
                .retrieve()
                .bodyToMono(ApiManagerResponse.class)
                .doOnNext(response -> updateApiManagerMetrics(environment.getName(), response))
                .then()
                .doOnSuccess(v -> log.debug("Completed API Manager collection for environment {}", environment.getName()))
                .onErrorResume(throwable -> {
                    exporterMetrics.incrementErrorCounter("api_manager_environment_failed");
                    log.warn("Failed to collect API Manager metrics for environment {}: {}", environment.getName(), throwable.getMessage());
                    return Mono.empty();
                });
    }

    /**
     * Update API Manager metrics from response
     */
    private void updateApiManagerMetrics(String environmentName, ApiManagerResponse response) {
        List<ApiInstance> apis = response.getApis();
        
        // Total API count per environment
        String envKey = "env_" + environmentName;
        updateGaugeMetric("anypoint_api_count", envKey, apis.size(), Map.of(
                "environment", environmentName
        ));
        
        // Individual API status and policy counts
        for (ApiInstance api : apis) {
            String apiKey = createApiKey(api.getId(), environmentName);
            
            // API status (1 = active, 0 = inactive)
            int statusValue = "active".equalsIgnoreCase(api.getStatus()) ? 1 : 0;
            updateGaugeMetric("anypoint_api_status", apiKey, statusValue, Map.of(
                    "api_id", String.valueOf(api.getId()),
                    "api_name", api.getInstanceLabel() != null ? api.getInstanceLabel() : "unknown",
                    "environment", environmentName,
                    "status", api.getStatus() != null ? api.getStatus() : "unknown"
            ));
            
            // Policy count per API
            int policyCount = (api.getPolicies() != null) ? api.getPolicies().size() : 0;
            updateGaugeMetric("anypoint_api_policy_count", apiKey, policyCount, Map.of(
                    "api_id", String.valueOf(api.getId()),
                    "api_name", api.getInstanceLabel() != null ? api.getInstanceLabel() : "unknown",
                    "environment", environmentName
            ));
        }
        
        log.debug("Updated API Manager metrics for environment {}: {} APIs total", environmentName, apis.size());
    }

    /**
     * Create a unique key for an API
     */
    private String createApiKey(Long apiId, String environment) {
        return "api_" + apiId + "_" + environment;
    }

    /**
     * Update or create a gauge metric
     */
    private void updateGaugeMetric(String metricName, String key, double value, Map<String, String> tags) {
        // Choose the appropriate storage map based on metric name
        ConcurrentHashMap<String, AtomicLong> storageMap;
        if (metricName.contains("count")) {
            if (metricName.contains("policy")) {
                storageMap = policyCountMetrics;
            } else {
                storageMap = apiCountMetrics;
            }
        } else {
            storageMap = apiStatusMetrics;
        }
        
        storageMap.computeIfAbsent(key, k -> {
            AtomicLong atomicValue = new AtomicLong((long) value);
            
            // Build gauge with tags
            Gauge.Builder<AtomicLong> builder = Gauge.builder(metricName, atomicValue, AtomicLong::get);
                    
            tags.forEach(builder::tag);
            
            builder.register(meterRegistry);
            return atomicValue;
        }).set((long) value);
    }

    // Data Transfer Objects for API responses
    public static class ApiManagerResponse {
        private List<ApiInstance> apis;
        private Integer total;
        
        public List<ApiInstance> getApis() {
            return apis != null ? apis : java.util.Collections.emptyList();
        }
        
        public void setApis(List<ApiInstance> apis) {
            this.apis = apis;
        }
        
        public Integer getTotal() { return total; }
        public void setTotal(Integer total) { this.total = total; }
    }
    
    public static class ApiInstance {
        private Long id;
        private String instanceLabel;
        private String status;
        private List<ApiPolicy> policies;
        private ApiSpec spec;
        
        public Long getId() { return id; }
        public void setId(Long id) { this.id = id; }
        
        public String getInstanceLabel() { return instanceLabel; }
        public void setInstanceLabel(String instanceLabel) { this.instanceLabel = instanceLabel; }
        
        public String getStatus() { return status; }
        public void setStatus(String status) { this.status = status; }
        
        public List<ApiPolicy> getPolicies() { return policies; }
        public void setPolicies(List<ApiPolicy> policies) { this.policies = policies; }
        
        public ApiSpec getSpec() { return spec; }
        public void setSpec(ApiSpec spec) { this.spec = spec; }
    }
    
    public static class ApiPolicy {
        private Long id;
        private String policyTemplateId;
        private String configurationData;
        private Integer order;
        private Boolean disabled;
        
        public Long getId() { return id; }
        public void setId(Long id) { this.id = id; }
        
        public String getPolicyTemplateId() { return policyTemplateId; }
        public void setPolicyTemplateId(String policyTemplateId) { this.policyTemplateId = policyTemplateId; }
        
        public String getConfigurationData() { return configurationData; }
        public void setConfigurationData(String configurationData) { this.configurationData = configurationData; }
        
        public Integer getOrder() { return order; }
        public void setOrder(Integer order) { this.order = order; }
        
        public Boolean getDisabled() { return disabled; }
        public void setDisabled(Boolean disabled) { this.disabled = disabled; }
    }
    
    public static class ApiSpec {
        private String assetId;
        private String version;
        
        public String getAssetId() { return assetId; }
        public void setAssetId(String assetId) { this.assetId = assetId; }
        
        public String getVersion() { return version; }
        public void setVersion(String version) { this.version = version; }
    }
}
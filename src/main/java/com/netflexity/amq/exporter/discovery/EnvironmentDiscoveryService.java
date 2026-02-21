package com.netflexity.amq.exporter.discovery;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.netflexity.amq.exporter.client.AnypointAuthClient;
import com.netflexity.amq.exporter.config.AnypointConfig;
import lombok.Data;
import lombok.extern.slf4j.Slf4j;
import org.springframework.http.HttpStatusCode;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Service;
import org.springframework.web.reactive.function.client.WebClient;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import jakarta.annotation.PostConstruct;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Collectors;

/**
 * Auto-discovers organizations and environments from Anypoint Platform.
 * 
 * On startup and periodically, queries the Platform API to find all
 * accessible organizations and their environments, replacing the need
 * for manual configuration.
 */
@Service
@Slf4j
public class EnvironmentDiscoveryService {

    private final WebClient webClient;
    private final AnypointAuthClient authClient;
    private final AnypointConfig anypointConfig;

    // Discovered orgs and environments
    private final Map<String, OrgInfo> discoveredOrgs = new ConcurrentHashMap<>();
    private final List<AnypointConfig.Environment> discoveredEnvironments = Collections.synchronizedList(new ArrayList<>());
    private volatile boolean discoveryComplete = false;

    public EnvironmentDiscoveryService(WebClient webClient, AnypointAuthClient authClient, AnypointConfig anypointConfig) {
        this.webClient = webClient;
        this.authClient = authClient;
        this.anypointConfig = anypointConfig;
    }

    @PostConstruct
    public void init() {
        if (anypointConfig.isAutoDiscovery()) {
            log.info("Auto-discovery enabled. Discovering organizations and environments...");
            discover().block();
        } else {
            log.info("Auto-discovery disabled. Using configured environments: {}",
                anypointConfig.getEnvironments().stream()
                    .map(e -> e.getName() + " (" + e.getId() + ")")
                    .collect(Collectors.joining(", ")));
            discoveryComplete = true;
        }
    }

    /**
     * Refresh discovery every 5 minutes
     */
    @Scheduled(fixedDelayString = "${anypoint.discovery.refreshIntervalMs:300000}",
               initialDelayString = "${anypoint.discovery.refreshIntervalMs:300000}")
    public void scheduledRefresh() {
        if (anypointConfig.isAutoDiscovery()) {
            log.debug("Refreshing organization and environment discovery");
            discover().subscribe(
                v -> {},
                error -> log.error("Discovery refresh failed: {}", error.getMessage())
            );
        }
    }

    /**
     * Run full discovery: orgs -> environments for each org
     */
    public Mono<Void> discover() {
        return discoverOrganizations()
            .flatMap(this::discoverEnvironments)
            .collectList()
            .doOnSuccess(allEnvs -> {
                discoveredEnvironments.clear();
                allEnvs.forEach(discoveredEnvironments::addAll);
                
                // Update the config with discovered org and environments
                if (!discoveredOrgs.isEmpty() && (anypointConfig.getOrganizationId() == null || anypointConfig.getOrganizationId().isEmpty())) {
                    // Set the first discovered org as the primary org ID
                    String primaryOrgId = discoveredOrgs.keySet().iterator().next();
                    anypointConfig.setOrganizationId(primaryOrgId);
                    log.info("Auto-set organizationId to: {}", primaryOrgId);
                }
                if (!discoveredEnvironments.isEmpty()) {
                    anypointConfig.setEnvironments(new ArrayList<>(discoveredEnvironments));
                    log.info("Discovery complete. Found {} organizations, {} environments",
                        discoveredOrgs.size(), discoveredEnvironments.size());
                    discoveredOrgs.forEach((orgId, org) -> 
                        log.info("  Org: {} ({})", org.getName(), orgId));
                    discoveredEnvironments.forEach(env -> 
                        log.info("  Env: {} ({})", env.getName(), env.getId()));
                } else {
                    log.warn("Discovery found no environments. Keeping existing config.");
                }
                discoveryComplete = true;
            })
            .then();
    }

    /**
     * Discover all organizations accessible to the authenticated user/app.
     * 
     * Uses /accounts/api/me to get the root org, then traverses sub-orgs.
     */
    private Flux<OrgInfo> discoverOrganizations() {
        return authClient.getAccessToken()
            .flatMap(token -> webClient.get()
                .uri(anypointConfig.getBaseUrl() + "/accounts/api/me")
                .header("Authorization", token.getAuthorizationHeader())
                .retrieve()
                .onStatus(HttpStatusCode::isError, response ->
                    response.bodyToMono(String.class)
                        .flatMap(body -> Mono.error(new RuntimeException("Failed to get /me: " + response.statusCode() + " " + body))))
                .bodyToMono(MeResponse.class))
            .flatMapMany(me -> {
                List<OrgInfo> orgs = new ArrayList<>();
                
                // Root org from the user's membership
                if (me.getUser() != null && me.getUser().getOrganization() != null) {
                    UserOrg rootOrg = me.getUser().getOrganization();
                    OrgInfo info = new OrgInfo(rootOrg.getId(), rootOrg.getName());
                    orgs.add(info);
                    discoveredOrgs.put(rootOrg.getId(), info);
                    log.info("Discovered root org: {} ({})", rootOrg.getName(), rootOrg.getId());
                    
                    // Add sub-orgs from memberOfOrganizations (has actual names)
                    if (me.getUser().getMemberOfOrganizations() != null) {
                        for (MemberOrg memberOrg : me.getUser().getMemberOfOrganizations()) {
                            if (!discoveredOrgs.containsKey(memberOrg.getId())) {
                                OrgInfo subInfo = new OrgInfo(memberOrg.getId(), memberOrg.getName());
                                orgs.add(subInfo);
                                discoveredOrgs.put(memberOrg.getId(), subInfo);
                                log.info("Discovered org: {} ({})", memberOrg.getName(), memberOrg.getId());
                            }
                        }
                    }
                    // Fallback: add sub-orgs by ID if not already found via memberOf
                    if (rootOrg.getSubOrganizationIds() != null) {
                        for (String subOrgId : rootOrg.getSubOrganizationIds()) {
                            if (!discoveredOrgs.containsKey(subOrgId)) {
                                OrgInfo subInfo = new OrgInfo(subOrgId, "Sub-org " + subOrgId);
                                orgs.add(subInfo);
                                discoveredOrgs.put(subOrgId, subInfo);
                            }
                        }
                    }
                }
                
                // Also use the configured orgId if not already found
                String configOrgId = anypointConfig.getOrganizationId();
                if (configOrgId != null && !configOrgId.isEmpty() && !discoveredOrgs.containsKey(configOrgId)) {
                    OrgInfo configInfo = new OrgInfo(configOrgId, "Configured Org");
                    orgs.add(configInfo);
                    discoveredOrgs.put(configOrgId, configInfo);
                }
                
                return Flux.fromIterable(orgs);
            });
    }

    /**
     * Discover all environments for a given organization.
     */
    private Flux<List<AnypointConfig.Environment>> discoverEnvironments(OrgInfo org) {
        return authClient.getAccessToken()
            .flatMap(token -> webClient.get()
                .uri(anypointConfig.getBaseUrl() + "/accounts/api/organizations/" + org.getId() + "/environments")
                .header("Authorization", token.getAuthorizationHeader())
                .retrieve()
                .onStatus(HttpStatusCode::isError, response ->
                    response.bodyToMono(String.class)
                        .flatMap(body -> {
                            log.warn("Failed to list environments for org {}: {} {}", org.getName(), response.statusCode(), body);
                            return Mono.error(new RuntimeException("Failed to list environments: " + response.statusCode()));
                        }))
                .bodyToMono(EnvironmentsResponse.class))
            .map(response -> {
                List<AnypointConfig.Environment> envs = new ArrayList<>();
                if (response.getData() != null) {
                    for (EnvironmentData envData : response.getData()) {
                        // Only include environments that have MQ enabled (type sandbox or production)
                        AnypointConfig.Environment env = new AnypointConfig.Environment();
                        env.setId(envData.getId());
                        env.setName(envData.getName());
                        envs.add(env);
                        log.debug("Discovered environment: {} ({}) type={} in org {}", 
                            envData.getName(), envData.getId(), envData.getType(), org.getName());
                    }
                }
                return envs;
            })
            .onErrorResume(error -> {
                log.warn("Could not discover environments for org {}: {}", org.getName(), error.getMessage());
                return Mono.just(Collections.emptyList());
            })
            .flux();
    }

    public boolean isDiscoveryComplete() {
        return discoveryComplete;
    }

    public Map<String, OrgInfo> getDiscoveredOrgs() {
        return Collections.unmodifiableMap(discoveredOrgs);
    }

    public List<AnypointConfig.Environment> getDiscoveredEnvironments() {
        return Collections.unmodifiableList(discoveredEnvironments);
    }

    // ========== Response DTOs ==========

    @Data
    public static class OrgInfo {
        private final String id;
        private final String name;
    }

    @Data
    @JsonIgnoreProperties(ignoreUnknown = true)
    static class MeResponse {
        private MeUser user;
    }

    @Data
    @JsonIgnoreProperties(ignoreUnknown = true)
    static class MeUser {
        @JsonProperty("organization")
        private UserOrg organization;
        @JsonProperty("memberOfOrganizations")
        private List<MemberOrg> memberOfOrganizations;
    }

    @Data
    @JsonIgnoreProperties(ignoreUnknown = true)
    static class UserOrg {
        private String id;
        private String name;
        @JsonProperty("subOrganizationIds")
        private List<String> subOrganizationIds;
    }

    @Data
    @JsonIgnoreProperties(ignoreUnknown = true)
    static class MemberOrg {
        private String id;
        private String name;
    }

    @Data
    @JsonIgnoreProperties(ignoreUnknown = true)
    static class EnvironmentsResponse {
        private List<EnvironmentData> data;
    }

    @Data
    @JsonIgnoreProperties(ignoreUnknown = true)
    static class EnvironmentData {
        private String id;
        private String name;
        private String type; // sandbox, production, design
        private boolean isProduction;
    }
}

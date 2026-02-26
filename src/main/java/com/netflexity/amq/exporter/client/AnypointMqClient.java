package com.netflexity.amq.exporter.client;

import com.netflexity.anypoint.common.client.AnypointAuthClient;
import com.netflexity.anypoint.common.config.AnypointConfig;
import com.netflexity.amq.exporter.model.Exchange;
import com.netflexity.amq.exporter.model.ExchangeStats;
import com.netflexity.anypoint.common.model.Queue;
import com.netflexity.anypoint.common.model.QueueStats;
import lombok.extern.slf4j.Slf4j;
import org.springframework.core.ParameterizedTypeReference;
import org.springframework.http.HttpStatus;
import org.springframework.http.HttpStatusCode;
import org.springframework.stereotype.Component;
import org.springframework.web.reactive.function.client.WebClient;
import org.springframework.web.reactive.function.client.WebClientResponseException;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.util.retry.Retry;

import java.time.Duration;
import java.time.Instant;
import java.time.format.DateTimeFormatter;
import java.util.List;

/**
 * Client for interacting with Anypoint MQ Admin and Stats APIs.
 * 
 * Provides methods to:
 * - List queues and exchanges
 * - Get queue and exchange statistics
 * - Handle authentication automatically via AnypointAuthClient
 */
@Component
@Slf4j
public class AnypointMqClient {

    private final WebClient webClient;
    private final AnypointConfig anypointConfig;
    private final AnypointAuthClient authClient;

    // Anypoint MQ Stats API requires millisecond precision (e.g., 2025-01-01T00:00:00.000Z)
    private static final DateTimeFormatter ISO_FORMATTER = DateTimeFormatter.ofPattern("yyyy-MM-dd'T'HH:mm:ss.SSS'Z'")
            .withZone(java.time.ZoneOffset.UTC);

    public AnypointMqClient(WebClient webClient, AnypointConfig anypointConfig, AnypointAuthClient authClient) {
        this.webClient = webClient;
        this.anypointConfig = anypointConfig;
        this.authClient = authClient;
        
        log.info("Initialized AnypointMqClient for organization: {}", anypointConfig.getOrganizationId());
    }

    /**
     * List all queues for a given environment and region
     *
     * @param environmentId Environment ID
     * @param region Region name (e.g., us-east-1)
     * @return Flux of Queue objects
     */
    public Flux<Queue> listQueues(String environmentId, String region) {
        log.debug("Listing queues for environment {} in region {}", environmentId, region);
        
        String url = String.format("%s/mq/admin/api/v1/organizations/%s/environments/%s/regions/%s/destinations",
                anypointConfig.getBaseUrl(),
                anypointConfig.getOrganizationId(),
                environmentId,
                region);

        return authClient.getAccessToken()
                .flatMapMany(token -> webClient.get()
                        .uri(url)
                        .header("Authorization", token.getAuthorizationHeader())
                        .retrieve()
                        .onStatus(HttpStatusCode::isError, response -> handleApiError(response, "list queues"))
                        .bodyToMono(new ParameterizedTypeReference<List<Queue>>() {})
                        .flatMapMany(Flux::fromIterable)
                        .filter(queue -> queue.getQueueId() != null && !"exchange".equalsIgnoreCase(queue.getType()))
                        .doOnNext(queue -> {
                            queue.setRegion(region);
                            queue.setEnvironment(environmentId);
                        }))
                .retryWhen(Retry.backoff(anypointConfig.getHttp().getMaxRetries(), Duration.ofSeconds(1))
                        .filter(this::isRetryableError)
                        .doBeforeRetry(retrySignal -> log.warn("Retrying list queues, attempt {}", retrySignal.totalRetries() + 1)))
                .timeout(Duration.ofSeconds(anypointConfig.getHttp().getReadTimeoutSeconds()))
                .doOnComplete(() -> log.debug("Completed listing queues for environment {} in region {}", environmentId, region))
                .doOnError(error -> log.error("Failed to list queues for environment {} in region {}: {}", environmentId, region, error.getMessage()));
    }

    /**
     * List all exchanges for a given environment and region
     *
     * @param environmentId Environment ID
     * @param region Region name (e.g., us-east-1)
     * @return Flux of Exchange objects
     */
    public Flux<Exchange> listExchanges(String environmentId, String region) {
        log.debug("Listing exchanges for environment {} in region {}", environmentId, region);
        
        String url = String.format("%s/mq/admin/api/v1/organizations/%s/environments/%s/regions/%s/destinations",
                anypointConfig.getBaseUrl(),
                anypointConfig.getOrganizationId(),
                environmentId,
                region);

        return authClient.getAccessToken()
                .flatMapMany(token -> webClient.get()
                        .uri(url)
                        .header("Authorization", token.getAuthorizationHeader())
                        .retrieve()
                        .onStatus(HttpStatusCode::isError, response -> handleApiError(response, "list exchanges"))
                        .bodyToMono(new ParameterizedTypeReference<List<Exchange>>() {})
                        .flatMapMany(Flux::fromIterable)
                        .filter(exchange -> exchange.getExchangeId() != null && "exchange".equalsIgnoreCase(exchange.getType()))
                        .doOnNext(exchange -> {
                            exchange.setRegion(region);
                            exchange.setEnvironment(environmentId);
                        }))
                .retryWhen(Retry.backoff(anypointConfig.getHttp().getMaxRetries(), Duration.ofSeconds(1))
                        .filter(this::isRetryableError)
                        .doBeforeRetry(retrySignal -> log.warn("Retrying list exchanges, attempt {}", retrySignal.totalRetries() + 1)))
                .timeout(Duration.ofSeconds(anypointConfig.getHttp().getReadTimeoutSeconds()))
                .doOnComplete(() -> log.debug("Completed listing exchanges for environment {} in region {}", environmentId, region))
                .doOnError(error -> log.error("Failed to list exchanges for environment {} in region {}: {}", environmentId, region, error.getMessage()));
    }

    /**
     * Get real-time queue depth from Admin API (no lag).
     * Returns messagesInQueue and messagesInFlight directly.
     */
    public Mono<QueueStats> getQueueDepth(String environmentId, String region, String queueId) {
        log.debug("Getting real-time depth for queue {} in environment {} region {}", queueId, environmentId, region);
        
        String url = String.format("%s/mq/admin/api/v1/organizations/%s/environments/%s/regions/%s/queues/%s",
                anypointConfig.getBaseUrl(),
                anypointConfig.getOrganizationId(),
                environmentId,
                region,
                queueId);

        return authClient.getAccessToken()
                .flatMap(token -> webClient.get()
                        .uri(url)
                        .header("Authorization", token.getAuthorizationHeader())
                        .retrieve()
                        .onStatus(HttpStatusCode::isError, response -> handleApiError(response, "get queue depth for " + queueId))
                        .bodyToMono(java.util.Map.class)
                        .map(body -> {
                            QueueStats stats = new QueueStats();
                            stats.setQueueId(queueId);
                            Object inQueue = body.get("messagesInQueue");
                            Object inFlight = body.get("messagesInFlight");
                            if (inQueue instanceof Number) stats.setMessagesInQueue(((Number) inQueue).longValue());
                            if (inFlight instanceof Number) stats.setMessagesInFlight(((Number) inFlight).longValue());
                            return stats;
                        }))
                .retryWhen(Retry.backoff(anypointConfig.getHttp().getMaxRetries(), Duration.ofSeconds(1))
                        .filter(this::isRetryableError))
                .timeout(Duration.ofSeconds(anypointConfig.getHttp().getReadTimeoutSeconds()))
                .doOnSuccess(stats -> log.debug("Real-time depth for queue {}: inQueue={}, inFlight={}", queueId, 
                        stats != null ? stats.getMessagesInQueue() : "null",
                        stats != null ? stats.getMessagesInFlight() : "null"))
                .doOnError(error -> log.error("Failed to get depth for queue {}: {}", queueId, error.getMessage()));
    }

    /**
     * Get statistics for a specific queue (time-series from Stats API)
     *
     * @param environmentId Environment ID
     * @param region Region name
     * @param queueId Queue ID
     * @param periodSeconds Stats period in seconds
     * @return Mono containing QueueStats
     */
    public Mono<QueueStats> getQueueStats(String environmentId, String region, String queueId, int periodSeconds) {
        log.debug("Getting stats for queue {} in environment {} region {}", queueId, environmentId, region);
        
        Instant endTime = Instant.now();
        Instant startTime = endTime.minusSeconds(periodSeconds);
        
        String url = String.format("%s/mq/stats/api/v1/organizations/%s/environments/%s/regions/%s/queues/%s?startDate=%s&endDate=%s&period=%d",
                anypointConfig.getBaseUrl(),
                anypointConfig.getOrganizationId(),
                environmentId,
                region,
                queueId,
                ISO_FORMATTER.format(startTime),
                ISO_FORMATTER.format(endTime),
                periodSeconds);

        return authClient.getAccessToken()
                .flatMap(token -> webClient.get()
                        .uri(url)
                        .header("Authorization", token.getAuthorizationHeader())
                        .retrieve()
                        .onStatus(HttpStatusCode::isError, response -> handleApiError(response, "get queue stats for " + queueId))
                        .bodyToMono(QueueStats.class)
                        .doOnNext(stats -> stats.setQueueId(queueId)))
                .retryWhen(Retry.backoff(anypointConfig.getHttp().getMaxRetries(), Duration.ofSeconds(1))
                        .filter(this::isRetryableError)
                        .doBeforeRetry(retrySignal -> log.warn("Retrying get queue stats for {}, attempt {}: {}", queueId, retrySignal.totalRetries() + 1, retrySignal.failure().getMessage())))
                .timeout(Duration.ofSeconds(anypointConfig.getHttp().getReadTimeoutSeconds()))
                .doOnSuccess(stats -> log.debug("Successfully retrieved stats for queue {}: {}", queueId, stats != null ? stats.toSafeString() : "null"))
                .doOnError(error -> log.error("Failed to get stats for queue {}: {}", queueId, error.getMessage()));
    }

    /**
     * Get statistics for a specific exchange
     *
     * @param environmentId Environment ID
     * @param region Region name
     * @param exchangeId Exchange ID
     * @param periodSeconds Stats period in seconds
     * @return Mono containing ExchangeStats
     */
    public Mono<ExchangeStats> getExchangeStats(String environmentId, String region, String exchangeId, int periodSeconds) {
        log.debug("Getting stats for exchange {} in environment {} region {}", exchangeId, environmentId, region);
        
        Instant endTime = Instant.now();
        Instant startTime = endTime.minusSeconds(periodSeconds);
        
        String url = String.format("%s/mq/stats/api/v1/organizations/%s/environments/%s/regions/%s/exchanges/%s?startDate=%s&endDate=%s&period=%d",
                anypointConfig.getBaseUrl(),
                anypointConfig.getOrganizationId(),
                environmentId,
                region,
                exchangeId,
                ISO_FORMATTER.format(startTime),
                ISO_FORMATTER.format(endTime),
                periodSeconds);

        return authClient.getAccessToken()
                .flatMap(token -> webClient.get()
                        .uri(url)
                        .header("Authorization", token.getAuthorizationHeader())
                        .retrieve()
                        .onStatus(HttpStatusCode::isError, response -> handleApiError(response, "get exchange stats for " + exchangeId))
                        .bodyToMono(ExchangeStats.class)
                        .doOnNext(stats -> stats.setExchangeId(exchangeId)))
                .retryWhen(Retry.backoff(anypointConfig.getHttp().getMaxRetries(), Duration.ofSeconds(1))
                        .filter(this::isRetryableError)
                        .doBeforeRetry(retrySignal -> log.warn("Retrying get exchange stats for {}, attempt {}: {}", exchangeId, retrySignal.totalRetries() + 1, retrySignal.failure().getMessage())))
                .timeout(Duration.ofSeconds(anypointConfig.getHttp().getReadTimeoutSeconds()))
                .doOnSuccess(stats -> log.debug("Successfully retrieved stats for exchange {}: {}", exchangeId, stats != null ? stats.toSafeString() : "null"))
                .doOnError(error -> log.error("Failed to get stats for exchange {}: {}", exchangeId, error.getMessage()));
    }

    /**
     * Handle API errors and create appropriate error responses.
     * Wraps errors in ApiException to preserve HTTP status for retry filtering.
     */
    private Mono<? extends Throwable> handleApiError(org.springframework.web.reactive.function.client.ClientResponse response, String operation) {
        return response.bodyToMono(String.class)
                .defaultIfEmpty("")
                .doOnNext(body -> log.error("API error during {}: status={}, body={}", operation, response.statusCode(), body))
                .then(Mono.error(new ApiException(response.statusCode().value(),
                        String.format("API call failed during %s with status: %s", operation, response.statusCode()))));
    }

    /**
     * Determine if an error is retryable
     */
    private boolean isRetryableError(Throwable throwable) {
        if (throwable instanceof ApiException apiEx) {
            // Don't retry on client errors (4xx) except for 429 (Too Many Requests)
            if (apiEx.getStatusCode() >= 400 && apiEx.getStatusCode() < 500 && apiEx.getStatusCode() != 429) {
                return false;
            }
        }
        if (throwable instanceof WebClientResponseException wcEx) {
            HttpStatusCode status = wcEx.getStatusCode();
            if (status.is4xxClientError() && status.value() != 429) {
                return false;
            }
        }
        // Retry on server errors, timeouts, and connection issues
        return true;
    }

    /**
     * Custom exception that preserves HTTP status code for retry filtering
     */
    private static class ApiException extends RuntimeException {
        private final int statusCode;

        public ApiException(int statusCode, String message) {
            super(message);
            this.statusCode = statusCode;
        }

        public int getStatusCode() {
            return statusCode;
        }
    }
}
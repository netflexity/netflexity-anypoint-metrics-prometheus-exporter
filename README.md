<p align="center">
  <h1 align="center">Anypoint Metrics Prometheus Exporter</h1>
  <p align="center">
    Complete metrics toolkit for MuleSoft Anypoint Platform - MTK port to Spring Boot Prometheus exporter.
  </p>
</p>

<p align="center">
  <a href="https://www.oracle.com/java/technologies/javase/jdk17-archive-downloads.html"><img src="https://img.shields.io/badge/Java-17-orange?logo=openjdk&logoColor=white" alt="Java 17"></a>
  <a href="https://spring.io/projects/spring-boot"><img src="https://img.shields.io/badge/Spring%20Boot-3.x-6DB33F?logo=springboot&logoColor=white" alt="Spring Boot 3"></a>
  <a href="https://prometheus.io/"><img src="https://img.shields.io/badge/Prometheus-Exporter-E6522C?logo=prometheus&logoColor=white" alt="Prometheus"></a>
  <a href="https://grafana.com/"><img src="https://img.shields.io/badge/Grafana-Ready-F46800?logo=grafana&logoColor=white" alt="Grafana"></a>
  <a href="LICENSE"><img src="https://img.shields.io/badge/License-Apache%202.0-blue.svg" alt="License"></a>
</p>

---

## Overview

A comprehensive Prometheus exporter for **MuleSoft Anypoint Platform** that ports the functionality of the **Netflexity Metrics Toolkit (MTK)** from MuleSoft/DataWeave to a modern Spring Boot application.

This exporter automatically discovers and monitors all aspects of your Anypoint Platform organization:
- **CloudHub Applications** (CH1 & CH2)
- **Dashboard Statistics** (events, response times, errors)
- **API Manager** (APIs, policies, status)
- **API Analytics** (request counts, violations)
- **Metering/Usage** (24 meter types including flow counts, message counts, data throughput)
- **Optimization Reports** (oversized apps, potential savings)
- **Platform Metrics** (org-level stats, users, environments)
- **Business & SDLC Metrics** (custom KPIs, deployment frequency)
- **Alerts** (CloudHub alerts, triggered status)

## Architecture

```
┌──────────────────────────────────────────────────────┐
│                 Anypoint Platform APIs                │
│  /cloudhub/api/v2/applications          (CH1 apps)   │
│  /amc/application-manager/api/v2/…       (CH2 apps)   │
│  /cloudhub/api/v2/applications/{}/…      (stats)      │
│  /apimanager/xapi/v1/…                  (API mgmt)   │
│  /analytics/1.0/{}/…                    (analytics)   │
│  /metering/usage/api/v1/…               (usage)       │
│  /accounts/api/…                        (platform)    │
└──────────────────────┬───────────────────────────────┘
                       │
                       ▼
┌──────────────────────────────────────────────────────┐
│       Anypoint Metrics Exporter (Spring Boot 3)      │
│                                                      │
│  /actuator/prometheus   Prometheus metrics endpoint  │
│  /actuator/health       Application health status    │
│                                                      │
│  ┌─────────────────┐  ┌─────────────────────────────┐│
│  │ MTK Collectors  │  │ Scheduled Collectors        ││
│  │ • Apps (5min)   │  │ • Dashboard Stats (2min)    ││
│  │ • APIs (10min)  │  │ • Analytics (5min)          ││
│  │ • Usage (30min) │  │ • Platform (15min)          ││
│  │ • Optimize(24h) │  │ • Alerts (15min)            ││
│  └─────────────────┘  └─────────────────────────────┘│
└──────────────────────┬───────────────────────────────┘
                       │
                       ▼
┌──────────────────────────────────────────────────────┐
│                  Prometheus                          │
│                    (scrapes)                         │
└──────────────────────┬───────────────────────────────┘
                       │
                       ▼
┌──────────────────────────────────────────────────────┐
│               Grafana Dashboard                      │
│  • Application Inventory & Health                    │
│  • API Performance & Analytics                       │
│  • Metering & Usage Monitoring                       │
│  • Optimization Recommendations                      │
│  • Platform Overview & Alerts                       │
└──────────────────────────────────────────────────────┘
```

## Metrics Collected

### Application Inventory
- `anypoint_app_status` - Application status (1=STARTED, 0=stopped)
- `anypoint_app_vcores` - vCores allocated per application
- `anypoint_app_workers` - Worker count per application
- `anypoint_app_runtime_version` - Runtime version info metric

### Dashboard Statistics
- `anypoint_app_request_count` - Total requests processed
- `anypoint_app_response_time_avg` - Average response time
- `anypoint_app_error_count` - Error count

### API Manager
- `anypoint_api_count` - Total APIs per environment
- `anypoint_api_status` - API status (1=active, 0=inactive)
- `anypoint_api_policy_count` - Policies per API

### API Analytics
- `anypoint_api_requests_total` - Total API requests (counter)
- `anypoint_api_policy_violations_total` - Policy violations (counter)

### Metering/Usage (24 meter types)
- `anypoint_metering_mule_flow_count` - Mule flow executions
- `anypoint_metering_mule_message_count` - Messages processed
- `anypoint_metering_total_data_throughput` - Data throughput
- `anypoint_metering_api_manager_api_count` - API count
- `anypoint_metering_anypoint_mq_requests` - MQ requests
- ...and 19 more meter types

### Optimization
- `anypoint_optimization_oversized_apps` - Apps with excess resources
- `anypoint_optimization_potential_savings_vcores` - Potential vCore savings

### Platform
- `anypoint_platform_environments_count` - Environment count
- `anypoint_platform_users_count` - Organization users
- `anypoint_alerts_total` - Total alerts configured
- `anypoint_alerts_triggered` - Triggered alerts (counter)

## Quick Start

### Docker Compose (Recommended)

1. **Clone and configure:**
   ```bash
   git clone https://bitbucket.org/netflexity/anypoint-metrics-prometheus-exporter.git
   cd anypoint-metrics-prometheus-exporter
   cp .env.example .env
   # Edit .env with your Anypoint credentials
   ```

2. **Start the stack:**
   ```bash
   docker-compose up -d
   ```

3. **Access dashboards:**
   - Prometheus: http://localhost:9090
   - Grafana: http://localhost:3000 (admin/admin)
   - Metrics: http://localhost:9101/actuator/prometheus

### Manual Setup

1. **Prerequisites:**
   - Java 17+
   - Maven 3.6+
   - Connected App credentials from Anypoint Platform

2. **Configure credentials:**
   ```bash
   export ANYPOINT_CLIENT_ID="your-connected-app-client-id"
   export ANYPOINT_CLIENT_SECRET="your-connected-app-client-secret"
   export ANYPOINT_ORG_ID="your-organization-id"
   ```

3. **Build and run:**
   ```bash
   mvn clean package -DskipTests
   java -jar target/anypoint-metrics-prometheus-exporter-1.0.0.jar
   ```

## Configuration

### Environment Variables

| Variable | Description | Default |
|----------|-------------|---------|
| `ANYPOINT_CLIENT_ID` | Connected App client ID | Required |
| `ANYPOINT_CLIENT_SECRET` | Connected App client secret | Required |
| `ANYPOINT_ORG_ID` | Organization ID | Auto-discovered |
| `ANYPOINT_AUTO_DISCOVERY` | Auto-discover environments | `true` |
| `PORT` | HTTP port for metrics endpoint | `9101` |

### Collector Intervals

```yaml
anypoint:
  scrape:
    app-inventory-interval-seconds: 300      # 5 minutes
    dashboard-stats-interval-seconds: 120    # 2 minutes  
    api-manager-interval-seconds: 600        # 10 minutes
    api-analytics-interval-seconds: 300      # 5 minutes
    metering-usage-interval-seconds: 1800    # 30 minutes
    optimization-report-interval-seconds: 86400  # 24 hours
    platform-metrics-interval-seconds: 900  # 15 minutes
```

## Connected App Setup

1. **Create Connected App in Anypoint Platform:**
   - Go to Access Management → Connected Apps
   - Click "Create App"
   - Select "App acts on its own behalf"

2. **Grant required scopes:**
   ```
   Read access to:
   - CloudHub Applications
   - API Manager
   - Analytics
   - Metering
   - Organization data
   ```

3. **Get credentials:**
   - Copy Client ID and Client Secret
   - Use in ANYPOINT_CLIENT_ID and ANYPOINT_CLIENT_SECRET

## Grafana Dashboard

The included Grafana dashboard provides:

- **Application Overview**: Status, vCores usage, worker distribution
- **Performance Metrics**: Response times, throughput, error rates
- **API Analytics**: Request volumes, policy violations, top APIs
- **Usage & Metering**: Flow counts, message volumes, data throughput
- **Optimization**: Resource recommendations, cost savings potential
- **Platform Health**: Environment status, alerts, user activity

Import the dashboard from `grafana/dashboards/anypoint-metrics.json`.

## API Endpoints

- `/actuator/prometheus` - Prometheus metrics
- `/actuator/health` - Health status
- `/actuator/info` - Application info

## Monitoring Best Practices

1. **Scraping Frequency**: Default 60s interval is optimal for most metrics
2. **Retention**: Consider 30-90 day retention based on usage
3. **Alerting**: Set up alerts on application status, error rates, and resource usage
4. **Capacity Planning**: Use optimization metrics to right-size applications

## Development

### Building
```bash
mvn clean package
```

### Running Tests
```bash
mvn test
```

### Adding New Collectors
1. Create collector class extending base pattern
2. Add `@Component` and `@Scheduled` methods
3. Register metrics with MeterRegistry
4. Update configuration with intervals

## MTK Migration Notes

This exporter is a direct port of the MuleSoft Metrics Toolkit (MTK) modules:

1. **CloudHub App Inventory** → `AppInventoryCollector`
2. **Dashboard Statistics** → `DashboardStatsCollector`
3. **API Manager** → `ApiManagerCollector`
4. **API Analytics** → `ApiAnalyticsCollector`
5. **Metering/Usage** → `MeteringUsageCollector`
6. **Optimization Report** → `OptimizationReportCollector`
7. **Platform Metrics** → `PlatformMetricsCollector`
8. **Business Metrics** → `PlatformMetricsCollector`
9. **SDLC Metrics** → `PlatformMetricsCollector`
10. **Alerts** → `PlatformMetricsCollector`

All DataWeave transformations have been converted to Java with equivalent logic.

## Shared Library

This exporter shares its core infrastructure with the [Anypoint MQ Prometheus Exporter](https://bitbucket.org/netflexity/anypoint-mq-prometheus-exporter) via the [`anypoint-common`](https://bitbucket.org/netflexity/netflexity-anypoint-common) library.

The common library provides:
- **OAuth2 authentication** (Connected App + username/password) with token caching
- **Environment auto-discovery** from Anypoint Platform
- **Monitor evaluation engine** (queue depth, DLQ, throughput anomalies, health scores)
- **5 notification channels** (Slack, PagerDuty, Email, Teams, Webhook)
- **REST API controllers** (`/api/status`, `/api/monitors`, `/api/health-scores`)
- **Spring Boot Actuator health indicator** for Anypoint connectivity
- **License gating** (FREE/PRO tier feature control)

## Contributing

1. Fork the repository
2. Create a feature branch
3. Add tests for new functionality
4. Submit a pull request

## License

Apache License 2.0 - see [LICENSE](LICENSE) for details.

## Support

- **Issues**: [Bitbucket Issues](https://bitbucket.org/netflexity/anypoint-metrics-prometheus-exporter/issues)
- **Documentation**: See inline code documentation
- **Community**: Netflexity team

---

**Built with ❤️ by Netflexity** - Porting MuleSoft's best practices to modern observability stacks.
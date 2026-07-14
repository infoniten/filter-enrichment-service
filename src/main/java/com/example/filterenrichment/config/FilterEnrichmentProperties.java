package com.example.filterenrichment.config;

import org.springframework.boot.context.properties.ConfigurationProperties;

/**
 * All service configuration, externalized via env / ConfigMap. Grouped by concern: input/output
 * Kafka topics and DLQs (§24/§25/§29), Redis runtime-subscription keys (§5/§6), the Object Enrich
 * Service HTTP client (§7/§12/§16/§33), retry (§27), backpressure (§31) and the Subscription
 * Service internal API used to fail subscriptions with uncompilable filters (§8).
 */
@ConfigurationProperties(prefix = "filter-enrichment")
public class FilterEnrichmentProperties {

    private final Kafka kafka = new Kafka();
    private final Redis redis = new Redis();
    private final Enrich enrich = new Enrich();
    private final Retry retry = new Retry();
    private final Backpressure backpressure = new Backpressure();
    private final SubscriptionService subscriptionService = new SubscriptionService();

    /** Only subscriptions with this engine (and status ACTIVE) are served. */
    private String servedEngine = "OBJECT_BATCH";

    public Kafka getKafka() {
        return kafka;
    }

    public Redis getRedis() {
        return redis;
    }

    public Enrich getEnrich() {
        return enrich;
    }

    public Retry getRetry() {
        return retry;
    }

    public Backpressure getBackpressure() {
        return backpressure;
    }

    public SubscriptionService getSubscriptionService() {
        return subscriptionService;
    }

    public String getServedEngine() {
        return servedEngine;
    }

    public void setServedEngine(String servedEngine) {
        this.servedEngine = servedEngine;
    }

    public static class Kafka {
        /** Source objects topic (§24). */
        private String inputTopic = "objects.flat";
        /** Shared enriched objects topic (§25). */
        private String outputTopic = "objects.enriched";
        private String consumerGroup = "filter-enrichment-service";
        /** Listener container concurrency; effective parallelism is capped by input partitions (§32). */
        private int concurrency = 3;
        private final Dlq dlq = new Dlq();

        public String getInputTopic() {
            return inputTopic;
        }

        public void setInputTopic(String v) {
            this.inputTopic = v;
        }

        public String getOutputTopic() {
            return outputTopic;
        }

        public void setOutputTopic(String v) {
            this.outputTopic = v;
        }

        public String getConsumerGroup() {
            return consumerGroup;
        }

        public void setConsumerGroup(String v) {
            this.consumerGroup = v;
        }

        public int getConcurrency() {
            return concurrency;
        }

        public void setConcurrency(int v) {
            this.concurrency = v;
        }

        public Dlq getDlq() {
            return dlq;
        }

        /** The three dead-letter queues (§29). */
        public static class Dlq {
            private String input = "filter-enrichment.input.dlq";
            private String enrichment = "filter-enrichment.enrichment.dlq";
            private String output = "filter-enrichment.output.dlq";

            public String getInput() {
                return input;
            }

            public void setInput(String v) {
                this.input = v;
            }

            public String getEnrichment() {
                return enrichment;
            }

            public void setEnrichment(String v) {
                this.enrichment = v;
            }

            public String getOutput() {
                return output;
            }

            public void setOutput(String v) {
                this.output = v;
            }
        }
    }

    public static class Redis {
        private String channel = "subscriptions:changes";
        private String runtimeSetKey = "subs:runtime";
        private String configKeyPrefix = "sub:";

        public String getChannel() {
            return channel;
        }

        public void setChannel(String v) {
            this.channel = v;
        }

        public String getRuntimeSetKey() {
            return runtimeSetKey;
        }

        public void setRuntimeSetKey(String v) {
            this.runtimeSetKey = v;
        }

        public String getConfigKeyPrefix() {
            return configKeyPrefix;
        }

        public void setConfigKeyPrefix(String v) {
            this.configKeyPrefix = v;
        }
    }

    /** Object Enrich Service: metadata (§7) + enrichment endpoints (§12/§16), pooled client (§33). */
    public static class Enrich {
        private String baseUrl = "http://object-enrich-service:8080";
        private String domainConfigPath = "/api/config/domain";
        private int connectTimeoutMs = 2000;
        private int readTimeoutMs = 5000;
        private int maxConnections = 100;
        private int maxConnectionsPerRoute = 100;
        private final CircuitBreaker circuitBreaker = new CircuitBreaker();

        public String getBaseUrl() {
            return baseUrl;
        }

        public void setBaseUrl(String v) {
            this.baseUrl = v;
        }

        public String getDomainConfigPath() {
            return domainConfigPath;
        }

        public void setDomainConfigPath(String v) {
            this.domainConfigPath = v;
        }

        public int getConnectTimeoutMs() {
            return connectTimeoutMs;
        }

        public void setConnectTimeoutMs(int v) {
            this.connectTimeoutMs = v;
        }

        public int getReadTimeoutMs() {
            return readTimeoutMs;
        }

        public void setReadTimeoutMs(int v) {
            this.readTimeoutMs = v;
        }

        public int getMaxConnections() {
            return maxConnections;
        }

        public void setMaxConnections(int v) {
            this.maxConnections = v;
        }

        public int getMaxConnectionsPerRoute() {
            return maxConnectionsPerRoute;
        }

        public void setMaxConnectionsPerRoute(int v) {
            this.maxConnectionsPerRoute = v;
        }

        public CircuitBreaker getCircuitBreaker() {
            return circuitBreaker;
        }

        public static class CircuitBreaker {
            private float failureRateThreshold = 50f;
            private long waitDurationInOpenStateMs = 10_000;
            private int slidingWindowSize = 50;
            private int minimumNumberOfCalls = 20;

            public float getFailureRateThreshold() {
                return failureRateThreshold;
            }

            public void setFailureRateThreshold(float v) {
                this.failureRateThreshold = v;
            }

            public long getWaitDurationInOpenStateMs() {
                return waitDurationInOpenStateMs;
            }

            public void setWaitDurationInOpenStateMs(long v) {
                this.waitDurationInOpenStateMs = v;
            }

            public int getSlidingWindowSize() {
                return slidingWindowSize;
            }

            public void setSlidingWindowSize(int v) {
                this.slidingWindowSize = v;
            }

            public int getMinimumNumberOfCalls() {
                return minimumNumberOfCalls;
            }

            public void setMinimumNumberOfCalls(int v) {
                this.minimumNumberOfCalls = v;
            }
        }
    }

    public static class Retry {
        private int maxAttempts = 4;
        private long initialBackoffMs = 200;
        private long maxBackoffMs = 5_000;
        private double multiplier = 2.0;
        /** Random jitter added to each backoff, as a fraction of the interval (§33). */
        private double jitter = 0.3;

        public int getMaxAttempts() {
            return maxAttempts;
        }

        public void setMaxAttempts(int v) {
            this.maxAttempts = v;
        }

        public long getInitialBackoffMs() {
            return initialBackoffMs;
        }

        public void setInitialBackoffMs(long v) {
            this.initialBackoffMs = v;
        }

        public long getMaxBackoffMs() {
            return maxBackoffMs;
        }

        public void setMaxBackoffMs(long v) {
            this.maxBackoffMs = v;
        }

        public double getMultiplier() {
            return multiplier;
        }

        public void setMultiplier(double v) {
            this.multiplier = v;
        }

        public double getJitter() {
            return jitter;
        }

        public void setJitter(double v) {
            this.jitter = v;
        }
    }

    /** Bounded concurrency so the pod never accumulates unbounded work (§31). */
    public static class Backpressure {
        private int maxConcurrentHttpRequests = 32;
        /** How long a worker waits for an HTTP permit before partitions are paused. */
        private long acquireTimeoutMs = 1_000;
        /** How long partitions stay paused before the backpressure manager resumes them. */
        private long pauseMs = 500;

        public int getMaxConcurrentHttpRequests() {
            return maxConcurrentHttpRequests;
        }

        public void setMaxConcurrentHttpRequests(int v) {
            this.maxConcurrentHttpRequests = v;
        }

        public long getAcquireTimeoutMs() {
            return acquireTimeoutMs;
        }

        public void setAcquireTimeoutMs(long v) {
            this.acquireTimeoutMs = v;
        }

        public long getPauseMs() {
            return pauseMs;
        }

        public void setPauseMs(long v) {
            this.pauseMs = v;
        }
    }

    public static class SubscriptionService {
        private String baseUrl = "http://subscription-service:8080";

        public String getBaseUrl() {
            return baseUrl;
        }

        public void setBaseUrl(String v) {
            this.baseUrl = v;
        }
    }
}

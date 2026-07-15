package com.example.filterenrichment.registry;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.context.annotation.Lazy;
import org.springframework.data.redis.connection.Message;
import org.springframework.data.redis.connection.MessageListener;
import org.springframework.stereotype.Component;

import java.nio.charset.StandardCharsets;

/**
 * Handles {@code CONFIG_CHANGED} messages on the {@code subscriptions:changes} channel. The
 * message is only a signal: with a {@code subscriptionId} the service re-reads that one subscription;
 * without one it performs a full reload of the runtime configuration.
 */
@Component
public class ConfigChangeListener implements MessageListener {

    private static final Logger log = LoggerFactory.getLogger(ConfigChangeListener.class);

    private final ObjectMapper objectMapper;
    private final RuntimeConfigService runtimeConfigService;

    public ConfigChangeListener(ObjectMapper objectMapper,
                                @Lazy RuntimeConfigService runtimeConfigService) {
        this.objectMapper = objectMapper;
        this.runtimeConfigService = runtimeConfigService;
    }

    @Override
    public void onMessage(Message message, byte[] pattern) {
        String body = new String(message.getBody(), StandardCharsets.UTF_8);
        try {
            JsonNode node = objectMapper.readTree(body);
            String type = node.path("type").asText();
            if (!"CONFIG_CHANGED".equals(type)) {
                log.debug("Ignoring pub/sub message: {}", body);
                return;
            }
            String subscriptionId = node.path("subscriptionId").asText(null);
            if (subscriptionId == null || subscriptionId.isBlank()) {
                log.info("CONFIG_CHANGED without subscriptionId -> full reload");
                runtimeConfigService.loadAll();
            } else {
                runtimeConfigService.refreshOne(subscriptionId);
                log.debug("Processed CONFIG_CHANGED for {}", subscriptionId);
            }
        } catch (Exception e) {
            log.warn("Failed to handle pub/sub message '{}': {}", body, e.getMessage());
        }
    }
}

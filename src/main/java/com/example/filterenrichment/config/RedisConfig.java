package com.example.filterenrichment.config;

import com.example.filterenrichment.registry.ConfigChangeListener;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.data.redis.connection.RedisConnectionFactory;
import org.springframework.data.redis.listener.ChannelTopic;
import org.springframework.data.redis.listener.RedisMessageListenerContainer;
import org.springframework.data.redis.listener.Topic;

/**
 * Wires the Redis Pub/Sub listener on the {@code subscriptions:changes} channel.
 */
@Configuration
public class RedisConfig {

    @Bean
    public RedisMessageListenerContainer redisMessageListenerContainer(
            RedisConnectionFactory connectionFactory,
            ConfigChangeListener listener,
            FilterEnrichmentProperties props) {
        RedisMessageListenerContainer container = new RedisMessageListenerContainer();
        container.setConnectionFactory(connectionFactory);
        Topic channel = new ChannelTopic(props.getRedis().getChannel());
        container.addMessageListener(listener, channel);
        return container;
    }
}
